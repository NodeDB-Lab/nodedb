// SPDX-License-Identifier: BUSL-1.1

//! A predicate write or a TRUNCATE after another write of its materialized-sum
//! source, in one request.
//!
//! A request plans as one task set, and every write settles its balance delta
//! at plan time, from the rows that stand before the request. A predicate
//! write after another write of its source matches the rows that write staged,
//! which its settlement did not see, so a cross-shard balance drifts from the
//! source sum. The request is refused whole, before any route runs.
//!
//! The task order is the statement order. A predicate write or a TRUNCATE that
//! comes first is the first reader of its source and settles correctly.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, UpdateValue};
use nodedb_physical::physical_task::PhysicalTask;

use super::settling_writes::SAME_SOURCE_WRITES_REFUSAL;
use super::settling_writes_columns::{
    BindingColumns, MatchShape, binding_columns, expr_columns, filter_columns, is_plain_column,
    match_shape, update_assignments,
};
use crate::Error;
use crate::control::planner::materialized_sum::{drives_cross_shard_sum, source_drives_bindings};
use crate::control::state::SharedState;

/// What the catalog says about one written collection.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SourceSum {
    /// The collection drives no sum whose target lives on another vShard.
    NoCrossShardTarget,
    /// The collection drives a cross-shard sum. `None` when a binding or
    /// generated column makes the settled columns unknown.
    CrossShard(Option<Vec<BindingColumns>>),
}

/// Refuse a request in which a predicate write or a TRUNCATE of a cross-shard
/// sum source follows another write of that source, when the earlier write can
/// move what the later one matches or settles from.
///
/// A catalog read error counts as a cross-shard sum with unknown columns, so
/// the request is refused rather than left to drift.
pub fn refuse_predicate_after_source_write(
    state: &SharedState,
    tasks: &[PhysicalTask],
) -> crate::Result<()> {
    match refused_collection(tasks, |task, collection| {
        source_sum(state, task, collection)
    }) {
        Some(collection) => Err(Error::FeatureNotSupported {
            detail: format!("{SAME_SOURCE_WRITES_REFUSAL} Collection: {collection}."),
        }),
        None => Ok(()),
    }
}

/// The collection of the first refused predicate write or TRUNCATE. The
/// catalog is read only for a write that follows another write of its source.
fn refused_collection(
    tasks: &[PhysicalTask],
    catalog: impl Fn(&PhysicalTask, &str) -> SourceSum,
) -> Option<&str> {
    for (index, task) in tasks.iter().enumerate() {
        let (Some(shape), Some(collection)) =
            (match_shape(&task.plan), written_collection(&task.plan))
        else {
            continue;
        };
        let mut earlier = tasks[..index]
            .iter()
            .map(|task| &task.plan)
            .filter(|plan| written_collection(plan) == Some(collection))
            .peekable();
        if earlier.peek().is_none() {
            continue;
        }
        let SourceSum::CrossShard(bindings) = catalog(task, collection) else {
            continue;
        };
        if earlier.any(|plan| !leaves_settlement_unchanged(plan, &shape, bindings.as_deref())) {
            return Some(collection);
        }
    }
    None
}

fn source_sum(state: &SharedState, task: &PhysicalTask, collection: &str) -> SourceSum {
    if !drives_cross_shard_sum(state, collection, task.tenant_id, task.database_id).unwrap_or(true)
    {
        return SourceSum::NoCrossShardTarget;
    }
    SourceSum::CrossShard(settled_columns(state, task, collection))
}

/// The columns each binding of `collection` settles from. `None` when the
/// catalog cannot answer or the source holds a generated column, which a write
/// of another column can move.
fn settled_columns(
    state: &SharedState,
    task: &PhysicalTask,
    collection: &str,
) -> Option<Vec<BindingColumns>> {
    let key = nodedb_types::CollectionKey::from_qualified_str(task.database_id, collection).ok()?;
    let stored = state
        .credentials
        .catalog()
        .get_collection(task.database_id, task.tenant_id.as_u64(), key.name())
        .ok()??;
    if !stored.generated_columns().is_empty() {
        return None;
    }
    let bindings =
        source_drives_bindings(state, collection, task.tenant_id, task.database_id).ok()??;
    bindings.iter().map(binding_columns).collect()
}

/// The collection a document write names, or `None` for a read or any other
/// plan. A balance delta writes its target, which the rule does not read, so
/// it counts as no write. The match is exhaustive so a new `DocumentOp`
/// variant must state which side it is on.
fn written_collection(plan: &PhysicalPlan) -> Option<&str> {
    let PhysicalPlan::Document(op) = plan else {
        return None;
    };
    match op {
        DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::PointDelete { collection, .. }
        | DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::BatchInsert { collection, .. }
        | DocumentOp::Truncate { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::BatchUpsert { collection, .. }
        | DocumentOp::BulkUpdate { collection, .. }
        | DocumentOp::BulkDelete { collection, .. } => Some(collection.as_str()),
        DocumentOp::InsertSelect {
            target_collection, ..
        }
        | DocumentOp::UpdateFromJoin {
            target_collection, ..
        }
        | DocumentOp::Merge {
            target_collection, ..
        } => Some(target_collection.as_str()),
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => None,
    }
}

/// Whether the earlier write cannot move what `later` matches or settles
/// from. Every condition must hold, and a column that is not one plain
/// top-level name fails it:
///
/// 1. The earlier write is a point or predicate UPDATE. Any other write can
///    add or remove a row.
/// 2. It assigns no primary key, no column the later write's filters read, and
///    no join column of any binding.
/// 3. It assigns no source column of a binding the later write settles. A
///    delete or a TRUNCATE settles every binding. An UPDATE settles a binding
///    when it assigns the binding's join column or a value column.
/// 4. When the later write assigns a settled column, the earlier write assigns
///    none of its columns, and no settled assignment reads one.
///
/// `bindings` are the source's binding columns, or `None` when unknown.
fn leaves_settlement_unchanged(
    earlier: &PhysicalPlan,
    later: &MatchShape<'_>,
    bindings: Option<&[BindingColumns]>,
) -> bool {
    let (Some((assigned, earlier_key)), Some(bindings)) = (update_assignments(earlier), bindings)
    else {
        return false;
    };
    let written = column_names(assigned);
    let assigned_later = column_names(later.updates);
    if !written
        .iter()
        .chain(&assigned_later)
        .all(|column| is_plain_column(column))
    {
        return false;
    }
    let (Some(filters), Some(rls)) = (
        filter_columns(later.filters),
        filter_columns(later.rls_filters),
    ) else {
        return false;
    };
    let mut guarded = filters;
    guarded.extend(rls);
    guarded.extend(earlier_key.map(str::to_owned));
    guarded.extend(later.declared_primary_key.map(str::to_owned));
    let mut settled = BTreeSet::new();
    for binding in bindings {
        guarded.insert(binding.join_column.clone());
        if later.removes_rows
            || assigned_later.contains(&binding.join_column)
            || !binding.value_columns.is_disjoint(&assigned_later)
        {
            settled.insert(binding.join_column.clone());
            settled.extend(binding.value_columns.iter().cloned());
        }
    }
    if !written.is_disjoint(&guarded) || !written.is_disjoint(&settled) {
        return false;
    }
    if assigned_later.is_disjoint(&settled) {
        return true;
    }
    written.is_disjoint(&assigned_later)
        && later
            .updates
            .iter()
            .filter(|(column, _)| settled.contains(column))
            .all(|(_, value)| reads_none_of(value, &written))
}

fn column_names(updates: &[(String, UpdateValue)]) -> BTreeSet<String> {
    updates.iter().map(|(column, _)| column.clone()).collect()
}

/// Whether an assignment reads none of `columns`. An unnamed column reads as
/// one of them.
fn reads_none_of(value: &UpdateValue, columns: &BTreeSet<String>) -> bool {
    match value {
        UpdateValue::Literal(_) => true,
        UpdateValue::Expr(expr) => {
            let mut read = BTreeSet::new();
            expr_columns(expr, &mut read) && read.is_disjoint(columns)
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_task::PostSetOp;
    use nodedb_query::expr::SqlExpr;

    use super::*;
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    use crate::types::{DatabaseId, TenantId, VShardId};

    const SOURCE: &str = "ptv_post";

    fn task(op: DocumentOp) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(op),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn coll(name: &str) -> nodedb_types::QualifiedCollection {
        nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, name)
    }

    fn assign(columns: &[&str]) -> Vec<(String, UpdateValue)> {
        columns
            .iter()
            .map(|column| ((*column).to_owned(), UpdateValue::Literal(Vec::new())))
            .collect()
    }

    fn filters(columns: &[&str]) -> Vec<u8> {
        let filters: Vec<ScanFilter> = columns
            .iter()
            .map(|column| ScanFilter {
                field: (*column).to_owned(),
                op: FilterOp::Eq,
                value: "x".into(),
                clauses: Vec::new(),
                expr: None,
            })
            .collect();
        zerompk::to_msgpack_vec(&filters).expect("encode test filters")
    }

    fn insert(collection: &str) -> PhysicalTask {
        task(DocumentOp::PointInsert {
            collection: coll(collection),
            document_id: "e2".to_owned(),
            value: Vec::new(),
            if_absent: false,
            surrogate: nodedb_types::Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    fn update(updates: Vec<(String, UpdateValue)>) -> PhysicalTask {
        task(DocumentOp::PointUpdate {
            collection: coll(SOURCE),
            document_id: "e0".to_owned(),
            surrogate: None,
            pk_bytes: Vec::new(),
            updates,
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    fn bulk_update(updates: Vec<(String, UpdateValue)>, filtered: &[&str]) -> PhysicalTask {
        task(DocumentOp::BulkUpdate {
            collection: coll(SOURCE),
            filters: filters(filtered),
            updates,
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        })
    }

    fn bulk_delete(filtered: &[&str]) -> PhysicalTask {
        task(DocumentOp::BulkDelete {
            collection: coll(SOURCE),
            filters: filters(filtered),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        })
    }

    fn truncate() -> PhysicalTask {
        task(DocumentOp::Truncate {
            collection: coll(SOURCE),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    fn binding(value: &str) -> BindingColumns {
        BindingColumns {
            join_column: "account_id".to_owned(),
            value_columns: BTreeSet::from([value.to_owned()]),
        }
    }

    /// `balance` sums `amount`, `fees` sums `fee`, both joined on `account_id`.
    fn known() -> SourceSum {
        SourceSum::CrossShard(Some(vec![binding("amount"), binding("fee")]))
    }

    fn refused(tasks: &[PhysicalTask]) -> bool {
        refused_collection(tasks, |_, _| known()).is_some()
    }

    #[test]
    fn an_insert_then_a_predicate_delete_is_refused() {
        assert!(refused(&[insert(SOURCE), bulk_delete(&["amount"])]));
    }

    #[test]
    fn a_point_move_then_a_predicate_delete_is_refused() {
        let tasks = [
            update(assign(&["account_id"])),
            bulk_delete(&["account_id"]),
        ];
        assert!(refused(&tasks));
    }

    #[test]
    fn a_predicate_move_then_a_predicate_delete_is_refused() {
        let tasks = [
            bulk_update(assign(&["account_id"]), &["amount"]),
            bulk_delete(&["account_id"]),
        ];
        assert!(refused(&tasks));
    }

    #[test]
    fn an_insert_then_a_truncate_is_refused() {
        assert!(refused(&[insert(SOURCE), truncate()]));
    }

    #[test]
    fn a_value_column_update_then_a_truncate_is_refused() {
        assert!(refused(&[update(assign(&["amount"])), truncate()]));
    }

    #[test]
    fn an_update_of_a_filtered_column_then_a_predicate_write_is_refused() {
        let tasks = [
            update(assign(&["note"])),
            bulk_update(assign(&["fee"]), &["note"]),
        ];
        assert!(refused(&tasks));
    }

    #[test]
    fn a_settled_assignment_that_reads_an_assigned_column_is_refused() {
        let reads_note = UpdateValue::Expr(SqlExpr::Column("note".to_owned()));
        let tasks = [
            update(assign(&["note"])),
            bulk_update(vec![("fee".to_owned(), reads_note)], &["account_id"]),
        ];
        assert!(refused(&tasks));
    }

    #[test]
    fn a_disjoint_update_with_unknown_columns_is_refused() {
        let tasks = [update(assign(&["note"])), bulk_delete(&["account_id"])];
        assert!(refused_collection(&tasks, |_, _| SourceSum::CrossShard(None)).is_some());
    }

    #[test]
    fn two_predicate_updates_of_disjoint_sum_columns_are_not_refused() {
        let tasks = [
            bulk_update(assign(&["amount"]), &["account_id"]),
            bulk_update(assign(&["fee"]), &["account_id"]),
        ];
        assert!(!refused(&tasks));
    }

    #[test]
    fn an_update_of_an_unsettled_column_then_a_predicate_delete_is_not_refused() {
        let tasks = [update(assign(&["note"])), bulk_delete(&["account_id"])];
        assert!(!refused(&tasks));
    }

    #[test]
    fn an_update_of_an_unsettled_column_then_a_truncate_is_not_refused() {
        assert!(!refused(&[update(assign(&["note"])), truncate()]));
    }

    #[test]
    fn a_predicate_write_then_a_point_write_is_not_refused() {
        let tasks = [bulk_delete(&["account_id"]), insert(SOURCE)];
        assert!(!refused(&tasks));
    }

    #[test]
    fn a_write_of_another_collection_then_a_predicate_write_is_not_refused() {
        assert!(!refused(&[insert("other"), bulk_delete(&["amount"])]));
    }

    #[test]
    fn a_source_with_no_cross_shard_target_is_not_refused() {
        let tasks = [insert(SOURCE), bulk_delete(&["amount"])];
        assert!(refused_collection(&tasks, |_, _| SourceSum::NoCrossShardTarget).is_none());
    }

    #[test]
    fn the_catalog_is_not_read_without_an_earlier_write() {
        let tasks = [bulk_delete(&["amount"])];
        let refused = refused_collection(&tasks, |_, _| panic!("no catalog read expected"));
        assert!(refused.is_none());
    }
}
