// SPDX-License-Identifier: BUSL-1.1

//! Whether a statement's writes fire a body that joins its transaction.

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::control::trigger::dml_hook::classify_dml_write;
use crate::control::trigger::statement_txn::fires_joined_body;
use crate::control::trigger::{DmlEvent, TriggerScope};

/// The trigger events a write task can fire. An `UPSERT` is an INSERT or an
/// UPDATE, decided by its probe. A `MERGE` inserts, updates and deletes.
pub(super) fn task_events(task: &PhysicalTask) -> Option<(String, Vec<DmlEvent>)> {
    let info = classify_dml_write(&task.plan)?;
    let events = match &task.plan {
        PhysicalPlan::Document(DocumentOp::Merge { .. }) => {
            vec![DmlEvent::Insert, DmlEvent::Update, DmlEvent::Delete]
        }
        _ if info.needs_existence_probe => vec![DmlEvent::Insert, DmlEvent::Update],
        _ => vec![info.event],
    };
    Some((info.collection, events))
}

/// Whether any of `tasks` fires a body that joins its statement's
/// transaction: a BEFORE, INSTEAD OF or SYNC AFTER trigger. A statement
/// outside a transaction block that does runs in an implicit transaction.
pub fn statement_fires_joined_body(state: &SharedState, tasks: &[PhysicalTask]) -> bool {
    tasks.iter().any(|task| {
        let Some((collection, events)) = task_events(task) else {
            return false;
        };
        let scope = TriggerScope {
            database_id: task.database_id,
            tenant_id: task.tenant_id,
        };
        events
            .into_iter()
            .any(|event| fires_joined_body(state, scope, &collection, event))
    })
}

/// Whether a statement outside a transaction block runs in an implicit
/// transaction. It does in three cases:
///
/// - It fires a joined body.
/// - It is a `MERGE` into a collection an edge was ever written into. The
///   MERGE then stages each removed row with its node's edge tasks, and
///   COMMIT applies them together.
/// - It is an `INSERT ... SELECT`, `UPDATE ... FROM` or `MERGE` that writes
///   the source of a materialized sum whose target lives on another vShard.
///   The expansion ships that balance on an `ApplyBalanceDelta` task homed
///   on the target, and COMMIT applies it with the source rows.
pub fn statement_needs_implicit_txn(state: &SharedState, tasks: &[PhysicalTask]) -> bool {
    statement_fires_joined_body(state, tasks)
        || tasks
            .iter()
            .any(|task| merges_into_edge_bearing(state, task) || moves_cross_shard_sum(state, task))
}

/// Whether `task` is an unexpanded `INSERT ... SELECT`, `UPDATE ... FROM` or
/// `MERGE` whose written collection drives a materialized sum with a
/// cross-shard target. Its orchestrator applies on the written collection's
/// vShard alone, so it cannot move that target. A catalog read error reads
/// as a cross-shard sum, so the statement takes the transaction.
fn moves_cross_shard_sum(state: &SharedState, task: &PhysicalTask) -> bool {
    let PhysicalPlan::Document(op) = &task.plan else {
        return false;
    };
    let written = match op {
        DocumentOp::InsertSelect {
            target_collection, ..
        }
        | DocumentOp::UpdateFromJoin {
            target_collection,
            source_rows: None,
            ..
        }
        | DocumentOp::Merge {
            target_collection,
            resolved_inserts: None,
            ..
        } => target_collection.as_str(),
        DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::PointGet { .. }
        | DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Truncate { .. }
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
        | DocumentOp::ApplyBalanceDelta { .. } => return false,
    };
    crate::control::planner::materialized_sum::drives_cross_shard_sum(
        state,
        written,
        task.tenant_id,
        task.database_id,
    )
    .unwrap_or(true)
}

/// Whether `task` is a `MERGE` into an edge-bearing collection. A catalog
/// read error reads as edge-bearing, so the MERGE takes the transaction
/// that keeps its edges consistent.
fn merges_into_edge_bearing(state: &SharedState, task: &PhysicalTask) -> bool {
    let PhysicalPlan::Document(DocumentOp::Merge {
        target_collection, ..
    }) = &task.plan
    else {
        return false;
    };
    let bare = crate::control::target_identity::naming::bare_collection_name(
        task.database_id,
        target_collection.as_str(),
    );
    state
        .credentials
        .catalog()
        .get_collection(task.database_id, task.tenant_id.as_u64(), &bare)
        .map_or(true, |coll| {
            coll.is_some_and(|coll| coll.has_implicit_edges)
        })
}

/// The error for a statement that fires a joined body but plans a write its
/// implicit transaction cannot hold: that write will apply at once and
/// survive the transaction's rollback.
pub fn unbufferable_joined_statement() -> crate::Error {
    crate::Error::BadRequest {
        detail: "this statement fires a BEFORE, INSTEAD OF or SYNC AFTER trigger, so it runs \
                 in one transaction with the trigger bodies, and it plans a write a \
                 transaction cannot hold. Write the rows with statements a transaction can \
                 stage, such as point writes by primary key."
            .into(),
    }
}
