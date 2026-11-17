// SPDX-License-Identifier: BUSL-1.1

//! Whether an autocommit statement commits through Calvin because its
//! `RETURNING` list can fail.
//!
//! The direct path commits a write first and evaluates its list after. An
//! error in the list then arrives after the write is stored. A Calvin
//! participant evaluates the list before it votes, so an error refuses the
//! write. A statement whose list can fail commits as one Calvin transaction.
//! A list that cannot fail keeps the direct path.
//!
//! Calvin takes no write key for a write the direct path orchestrates on the
//! Control Plane. Such a write whose list can fail runs in an implicit
//! transaction instead ([`orchestrated_returning_can_fail`]): each row it
//! writes stages and answers from its staged image, and an error in the list
//! rolls the whole statement back.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::{
    CollectionType, DocumentMode, QualifiedCollection, ReturningColumns, ReturningItem,
};

use crate::control::planner::calvin::is_write_plan;
use crate::control::planner::calvin::tx_class::write_keys::task_write_keys;
use crate::control::server::shared::returning::{
    RowFields, plan_carries_returning_contract, returning_can_fail, tasks_carry_returning_contract,
    tasks_returning_can_fail,
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

/// Whether a task of `tasks` is an unexpanded `INSERT ... SELECT`,
/// `UPDATE ... FROM` or `MERGE` whose `RETURNING` list can fail. The
/// statement then runs in an implicit transaction. A list that cannot fail
/// keeps the direct or paged path.
pub(crate) fn orchestrated_returning_can_fail(state: &SharedState, tasks: &[PhysicalTask]) -> bool {
    tasks.iter().any(|task| {
        expands_in_a_transaction(&task.plan)
            && plan_returning_can_fail(state, task.tenant_id, &task.plan)
    })
}

/// Whether `plan`, a write of `tenant_id`, carries a `RETURNING` contract
/// that can fail under this node's response limits. The columns the running
/// statement announced count when one is in scope. Outside one, the list's
/// own names stand in for them.
pub(crate) fn plan_returning_can_fail(
    state: &SharedState,
    tenant_id: TenantId,
    plan: &PhysicalPlan,
) -> bool {
    let announced = announced_returning();
    returning_can_fail(
        plan,
        &state.tuning.network,
        star_fields(state, tenant_id, plan),
        announced.as_deref(),
    )
}

/// Whether `plan` is a write the direct path orchestrates and a transaction
/// expands into the point writes it stages.
fn expands_in_a_transaction(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Document(
            DocumentOp::InsertSelect { .. }
                | DocumentOp::UpdateFromJoin {
                    source_rows: None,
                    ..
                }
                | DocumentOp::Merge {
                    resolved_inserts: None,
                    ..
                }
        )
    )
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

/// Whether Calvin sequences `tasks` as one transaction. A write the direct
/// path orchestrates on the Control Plane keeps it: Calvin takes no write
/// key for one, so its transaction will write nothing. A task carrying a
/// contract must also be a write Calvin keys, and the set must key.
pub(crate) fn calvin_sequences(tasks: &[PhysicalTask]) -> bool {
    tasks.iter().all(|task| {
        !orchestrated_on_control_plane(&task.plan)
            && (!plan_carries_returning_contract(&task.plan) || is_write_plan(&task.plan))
    }) && task_write_keys(tasks).is_ok()
}

/// Whether the direct path runs `plan` through a Control-Plane
/// orchestrator, which resolves it into the writes it applies.
fn orchestrated_on_control_plane(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Document(
            DocumentOp::InsertSelect { .. }
                | DocumentOp::Merge { .. }
                | DocumentOp::UpdateFromJoin { .. }
                | DocumentOp::ResolveWrite(_)
                | DocumentOp::ResolvedWrite { .. }
        )
    )
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
        assert!(calvin_sequences(&[task(insert.clone())]));
        let PhysicalPlan::Document(op) = insert else {
            panic!("a document insert");
        };
        let resolving = PhysicalPlan::Document(DocumentOp::ResolveWrite(Box::new(op)));
        assert!(
            !calvin_sequences(&[task(resolving)]),
            "a write the direct path resolves stays there"
        );
    }

    #[test]
    fn only_an_unexpanded_orchestrated_write_expands_in_a_transaction() {
        let copy = PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "dst"),
            source: nodedb_physical::physical_plan::RowSource::whole_collection(
                QualifiedCollection::new(DatabaseId::DEFAULT, "src"),
            ),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(expands_in_a_transaction(&copy));
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
        assert!(!expands_in_a_transaction(&insert));
    }
}
