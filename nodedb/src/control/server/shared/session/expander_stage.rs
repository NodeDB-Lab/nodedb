// SPDX-License-Identifier: BUSL-1.1

//! Statement-time expansion + staging of an in-transaction `MERGE`,
//! `UPDATE ... FROM <source>`, or `INSERT ... SELECT`.
//!
//! Resolved against base ∪ overlay and staged through [`stage_write`], so
//! read-your-own-writes holds. Everything else is [`ExpanderOutcome::Passthrough`].

use std::future::Future;

use super::connection::SessionId;
use super::staging_gate::{
    InTxnRoute, StagedTagKind, StagedWriteOutcome, StagingGateError, stage_write,
};
use super::state::TransactionState;
use super::store::SessionStore;
use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::insert_select::resolve_and_emit_insert_select_ops;
use crate::control::merge_orchestrator::resolve_and_emit_merge_ops;
use crate::control::planner::calvin::write_class::{
    plan_counts_toward_statement_tag, plans_have_user_write,
};
use crate::control::server::shared::plan_admission::append_sum_and_period_targets;
use crate::control::state::SharedState;
use crate::control::update_from_join_orchestrator::resolve_and_emit_update_from_join_ops;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Outcome of [`route_in_tx_expander`].
pub(crate) enum ExpanderOutcome {
    /// A not-yet-resolved in-transaction `MERGE`/`UPDATE ... FROM`/`INSERT ... SELECT`:
    /// resolved, staged, and buffered. Carries the aggregate command tag.
    Handled(InTxnRoute),
    /// Autocommit or any other plan. Hands the original task back unmodified
    /// for [`route_in_tx_write`](super::staging_gate::route_in_tx_write).
    /// Boxed so this common variant doesn't bloat the enum.
    Passthrough(Box<PhysicalTask>),
}

/// Intercept an in-transaction `MERGE`/`UPDATE ... FROM`/`INSERT ... SELECT` for
/// statement-time resolution + staging. Hands `task` back via
/// [`ExpanderOutcome::Passthrough`] otherwise, no clone needed.
pub(crate) async fn route_in_tx_expander<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    mut task: PhysicalTask,
    dispatch: F,
) -> Result<ExpanderOutcome, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    match expand_in_tx(state, sessions, session_id, &mut task).await? {
        Some((ops, kind)) => Ok(ExpanderOutcome::Handled(
            stage_and_aggregate(state, sessions, session_id, ops, kind, dispatch).await?,
        )),
        None => Ok(ExpanderOutcome::Passthrough(Box::new(task))),
    }
}

/// Resolve an in-transaction `MERGE`/`UPDATE ... FROM`/`INSERT ... SELECT`, or
/// reshape a `BatchInsert` page, into the concrete point ops it expands to,
/// with the command tag the statement renders. Stages nothing. `None` for
/// every other plan, and outside a transaction block.
///
/// The ops are `PointInsert` for an inserted row, `PointPut` for an updated
/// row's post-image, `PointDelete` for a removed row, and an
/// `ApplyBalanceDelta` for each cross-shard materialized-sum balance they move.
pub(crate) async fn expand_in_tx(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    task: &mut PhysicalTask,
) -> Result<Option<(Vec<PhysicalTask>, StagedTagKind)>, StagingGateError> {
    // Only in-transaction join-expanding DML is handled here; everything else falls through.
    if sessions.transaction_state(session_id) != TransactionState::InBlock {
        return Ok(None);
    }
    let (mut ops, kind) = match &task.plan {
        PhysicalPlan::Document(DocumentOp::Merge {
            resolved_inserts: None,
            ..
        }) => {
            // Stamp txn_id so the resolve pass folds this transaction's staging overlay.
            task.txn_id = sessions.tx_id(session_id);
            // A resolve/surrogate-assignment failure maps to the gate's `Dispatch` variant.
            let ops = resolve_and_emit_merge_ops(state, task.tenant_id, task)
                .await
                .map_err(StagingGateError::Dispatch)?;
            (ops, StagedTagKind::Merge)
        }
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin { .. }) => {
            // Stamp txn_id so the resolve pass folds this transaction's staging overlay.
            task.txn_id = sessions.tx_id(session_id);
            // A resolve failure maps to the gate's `Dispatch` variant.
            let ops = resolve_and_emit_update_from_join_ops(state, task.tenant_id, task)
                .await
                .map_err(StagingGateError::Dispatch)?;
            (ops, StagedTagKind::UpdateFromJoin)
        }
        PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) => {
            // Stamp txn_id so the source scan folds this transaction's staging overlay.
            task.txn_id = sessions.tx_id(session_id);
            // Reuses `StagedTagKind::Insert` — `INSERT ... SELECT` renders the `INSERT n` tag.
            let ops = resolve_and_emit_insert_select_ops(state, task.tenant_id, task)
                .await
                .map_err(StagingGateError::Dispatch)?;
            (ops, StagedTagKind::Insert)
        }
        // A `BatchInsert` page is autocommit-shaped; only point ops give an overlay
        // post-image, per-row undo, and row-level redo, so expand back to points.
        // The page carries the statement's resolution and deferrals already.
        PhysicalPlan::Document(DocumentOp::BatchInsert { .. }) => {
            return Ok(Some((
                expand_batch_insert(task).map_err(StagingGateError::Dispatch)?,
                StagedTagKind::Insert,
            )));
        }
        _ => return Ok(None),
    };
    // The expanded point writes never passed statement admission. They take
    // its sum and period-lock pass here. A cross-shard balance ships on an
    // `ApplyBalanceDelta` task homed on the target's vShard, and the source
    // write does not fold it. The pre-image each balance settles from is read
    // through the transaction's staging overlay, the same view the expansion
    // resolved its rows against.
    let reads = append_sum_and_period_targets(
        state,
        &mut ops,
        task.tenant_id,
        task.database_id,
        sessions.tx_id(session_id),
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(StagingGateError::Dispatch)?;
    // The images each shipped balance was settled from join the read set, so
    // COMMIT aborts when a concurrent write moved them.
    sessions.record_read_entries(session_id, reads);
    Ok(Some((ops, kind)))
}

/// Expand a `BatchInsert` page into one `PointInsert` op per row. Nothing is
/// resolved — pure reshaping. A non-page plan comes back empty. A page whose
/// rows and surrogates differ in count is refused: every row carries its own
/// bound surrogate.
fn expand_batch_insert(task: &PhysicalTask) -> crate::Result<Vec<PhysicalTask>> {
    let PhysicalPlan::Document(DocumentOp::BatchInsert {
        collection,
        documents,
        surrogates,
        returning,
        rls_filters,
        resolved_sum_targets,
        deferred_sum_targets,
        ..
    }) = &task.plan
    else {
        return Ok(Vec::new());
    };
    if documents.len() != surrogates.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "batch insert into '{}' carries {} documents but {} surrogates; every row \
                 must carry its own surrogate",
                collection.as_str(),
                documents.len(),
                surrogates.len(),
            ),
        });
    }
    Ok(documents
        .iter()
        .zip(surrogates.iter().copied())
        .map(|((document_id, value), surrogate)| PhysicalTask {
            tenant_id: task.tenant_id,
            // Rows home to the same vShard the page was routed to.
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: collection.clone(),
                document_id: document_id.clone(),
                value: value.clone(),
                // A page carries no per-row conflict behaviour.
                if_absent: false,
                surrogate,
                returning: returning.clone(),
                rls_filters: rls_filters.clone(),
                resolved_sum_targets: resolved_sum_targets.clone(),
                deferred_sum_targets: deferred_sum_targets.clone(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: task.txn_id,
        })
        .collect())
}

/// Stage + buffer each concrete point op a resolved DML expands to,
/// aggregating affected counts into one outcome. Shared tail of
/// [`route_in_tx_expander`]'s resolve arms.
async fn stage_and_aggregate<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    ops: Vec<PhysicalTask>,
    kind: StagedTagKind,
    dispatch: F,
) -> Result<InTxnRoute, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    // Each `stage_write` dispatches into the overlay AND buffers the concrete op
    // for COMMIT replay — the raw Merge/UpdateFromJoin/InsertSelect is never buffered.
    // A balance task the expansion appended stages too, but its count describes
    // a row the statement never named.
    let has_user_write = plans_have_user_write(ops.iter().map(|op| &op.plan));
    let mut affected = 0usize;
    for op in ops {
        let counts = plan_counts_toward_statement_tag(&op.plan, has_user_write);
        // A removed row of an edge-bearing collection carries its node's edge
        // tasks, read before the removal stages.
        let node_delete_tasks =
            crate::control::planner::calvin::node_delete_txn::txn_node_delete_tasks(
                state,
                &op,
                sessions.tx_id(session_id),
            )
            .await
            .map_err(StagingGateError::Dispatch)?;
        let outcome = stage_write(state, sessions, session_id, op, &dispatch).await?;
        if counts {
            affected += outcome.affected;
        }
        super::node_delete_stage::stage_node_delete_tasks(
            state,
            sessions,
            session_id,
            node_delete_tasks,
            &dispatch,
        )
        .await?;
    }

    Ok(InTxnRoute::Staged(StagedWriteOutcome {
        kind,
        affected,
        payload: Vec::new(),
        returning_rows: Vec::new(),
    }))
}
