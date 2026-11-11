// SPDX-License-Identifier: BUSL-1.1

//! Whether an autocommit statement commits through Calvin because its
//! `RETURNING` list can fail.
//!
//! The direct path commits a write first and evaluates its list after. An
//! error in the list then arrives after the write is stored. A Calvin
//! participant evaluates the list before it votes, so an error refuses the
//! write. A statement whose list can fail commits as one Calvin transaction.
//! A list that cannot fail keeps the direct path.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::{
    CollectionType, DocumentMode, QualifiedCollection, ReturningColumns, ReturningItem,
};

use crate::control::planner::calvin::tx_class::write_keys::task_write_keys;
use crate::control::server::shared::returning::{
    RowFields, tasks_carry_returning_contract, tasks_returning_can_fail,
};
use crate::control::server::shared::session::announced_returning::announced_returning;
use crate::control::state::SharedState;
use crate::types::TenantId;

/// Whether `tasks`, one autocommit statement of `tenant_id`, commit through
/// Calvin because a `RETURNING` list among them can fail.
///
/// A task set Calvin cannot sequence keeps the direct path, as does one the
/// direct path orchestrates on the Control Plane.
pub(crate) fn returning_commits_through_calvin(
    state: &SharedState,
    tenant_id: TenantId,
    tasks: &[PhysicalTask],
) -> bool {
    if !tasks_carry_returning_contract(tasks) {
        return false;
    }
    let announced = announced_returning();
    let can_fail =
        tasks_returning_can_fail(tasks, &state.tuning.network, announced.as_deref(), |plan| {
            star_fields(state, tenant_id, plan)
        });
    can_fail && calvin_sequences(tasks)
}

/// What a `*` in `plan`'s list expands to. A list with no `*` never reads
/// it, so only a list with one reads the catalog. A collection the catalog
/// cannot name counts as unbounded, which only sends more writes through
/// Calvin.
fn star_fields(state: &SharedState, tenant_id: TenantId, plan: &PhysicalPlan) -> RowFields {
    let Some((collection, spec)) = plan.returning_target() else {
        return RowFields::Unbounded;
    };
    if !spec.precommit.as_ref().is_some_and(lists_a_star) {
        return RowFields::Unbounded;
    }
    let Ok(database_id) = QualifiedCollection::database_of_stored(collection.as_str()) else {
        return RowFields::Unbounded;
    };
    match state.credentials.catalog().get_collection(
        database_id,
        tenant_id.as_u64(),
        QualifiedCollection::bare_of_stored(collection.as_str()),
    ) {
        Ok(Some(stored)) if stored.is_active => row_fields_of(&stored.collection_type),
        Ok(_) | Err(_) => RowFields::Unbounded,
    }
}

/// The fields a row of a `collection_type` collection holds: a strict
/// collection's declared columns, else no cap.
fn row_fields_of(collection_type: &CollectionType) -> RowFields {
    match collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            RowFields::declared(schema.columns.iter().map(|column| column.name.as_str()))
        }
        _ => RowFields::Unbounded,
    }
}

/// Whether `list` holds a `*`.
fn lists_a_star(list: &ReturningColumns) -> bool {
    match list {
        ReturningColumns::Star => true,
        ReturningColumns::Named(items) => {
            items.iter().any(|item| matches!(item, ReturningItem::Star))
        }
    }
}

/// Whether Calvin sequences `tasks` as one static transaction. An
/// `INSERT ... SELECT` runs its own orchestrator on the direct path.
fn calvin_sequences(tasks: &[PhysicalTask]) -> bool {
    !tasks.iter().any(|task| {
        matches!(
            task.plan,
            PhysicalPlan::Document(DocumentOp::InsertSelect { .. })
        )
    }) && task_write_keys(tasks).is_ok()
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_task::PostSetOp;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_types::{DatabaseId, RlsWriteCheck, Surrogate};

    use super::*;
    use crate::types::VShardId;

    fn column(name: &str) -> ReturningItem {
        ReturningItem::Column {
            name: name.into(),
            alias: None,
        }
    }

    #[test]
    fn a_star_is_found_alone_or_among_items() {
        assert!(lists_a_star(&ReturningColumns::Star));
        assert!(lists_a_star(&ReturningColumns::Named(vec![
            column("id"),
            ReturningItem::Star,
        ])));
        assert!(!lists_a_star(&ReturningColumns::Named(vec![column("id")])));
    }

    #[test]
    fn a_strict_collection_declares_its_fields() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("n", ColumnType::Int64),
        ])
        .expect("a valid schema");
        assert_eq!(
            row_fields_of(&CollectionType::Document(DocumentMode::Strict(
                schema.clone()
            ))),
            RowFields::declared(["id", "n"])
        );
        assert_eq!(
            row_fields_of(&CollectionType::Document(DocumentMode::Schemaless)),
            RowFields::Unbounded
        );
        assert_eq!(
            row_fields_of(&CollectionType::kv(schema)),
            RowFields::Unbounded
        );
    }

    fn task(plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn a_point_insert_is_sequenced() {
        let insert = PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            document_id: "a".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        });
        assert!(calvin_sequences(&[task(insert)]));
    }
}
