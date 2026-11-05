// SPDX-License-Identifier: BUSL-1.1

//! Staging of one in-transaction write with its prediction and the edge
//! tasks it derives.
//!
//! A predicate write reads its rows first (`predict_txn_bulk_write`). A
//! delete on an edge-bearing collection derives its node-delete edge tasks
//! from that same read. An update on one derives its implicit-edge
//! reconciliation from it. The stage refuses with `OllpRetryRequired` when
//! its rows no longer match the read, staging nothing, and the read runs
//! again. Past the retry budget the statement fails with a serialization
//! conflict the client retries.

use std::future::Future;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::control::planner::calvin::node_delete_txn::txn_node_delete_tasks;
use crate::control::planner::calvin::retry_budget::OllpRetryBudget;
use crate::control::planner::calvin::txn_predict::predict_txn_bulk_write;
use crate::control::server::shared::sql::staging_predicates::is_stageable_write;
use crate::control::state::SharedState;
use crate::types::ReadShards;
use nodedb_physical::physical_task::PhysicalTask;

use super::connection::SessionId;
use super::node_delete_stage::stage_node_delete_tasks;
use super::read_set::{ReadCapture, record_read_set};
use super::staging_gate::{InTxnRoute, StagingGateError, stage_write};
use super::store::SessionStore;
use super::txn_expand::expand_for_buffering;

/// Stage or buffer `task` in the session's transaction, with its prediction
/// and the edge tasks it derives.
pub(super) async fn stage_predicted_write<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    task: PhysicalTask,
    dispatch: &F,
) -> Result<InTxnRoute, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    let txn_id = sessions.tx_id(session_id);
    // The shared OLLP retry policy, opened on the first refusal: the
    // orchestrator's budget and backoff. A node with no orchestrator retries
    // nothing.
    let mut budget: Option<OllpRetryBudget<'_>> = None;
    loop {
        let mut task = task.clone();
        let read = predict_txn_bulk_write(state, &mut task, txn_id)
            .await
            .map_err(StagingGateError::Dispatch)?;
        // A delete on an edge-bearing collection carries the edge tasks of
        // each row it deletes, read before the delete stages.
        let mut derived = txn_node_delete_tasks(
            state,
            &task,
            txn_id,
            read.as_ref().map(|read| &read.scan),
            sessions.tx_edge_truncate(session_id),
        )
        .await
        .map_err(StagingGateError::Dispatch)?;
        let predicate_class = read.as_ref().map(|read| read.predicate_class);
        if let Some(read) = read {
            derived.extend(read.edge_tasks);
        }
        // A predicate write read its rows at the block's snapshot. The read
        // joins the transaction's read set, so COMMIT refuses the block when
        // a row of the collection changed after the snapshot: a row that
        // joined or left the predicate moves the set the write matched.
        // A block with no snapshot matched the current rows, and COMMIT
        // checks the prediction against them.
        let predicate_read = predicate_class
            .filter(|_| sessions.tx_snapshot(session_id).is_some())
            .map(|_| (task.plan.clone(), task.tenant_id));
        let route = if is_stageable_write(&task.plan) {
            let staged = stage_write(state, sessions, session_id, task, dispatch).await;
            match (staged, predicate_class) {
                (Ok(outcome), _) => InTxnRoute::Staged(outcome),
                (
                    Err(StagingGateError::Rejected {
                        code: Some(ErrorCode::OllpRetryRequired),
                    }),
                    Some(predicate_class),
                ) => {
                    if budget.is_none() {
                        budget = state.ollp_orchestrator.get().map(|orchestrator| {
                            OllpRetryBudget::configured(orchestrator, predicate_class)
                        });
                    }
                    match budget.as_mut() {
                        Some(budget) if !budget.exhausted() => {
                            budget.spend().await;
                            continue;
                        }
                        // The statement's earlier reads stand, so the
                        // transaction fails with a conflict the client
                        // retries.
                        Some(_) | None => {
                            return Err(StagingGateError::Dispatch(
                                crate::Error::CalvinSerializationConflict,
                            ));
                        }
                    }
                }
                (Err(error), _) => return Err(error),
            }
        } else {
            for shard_task in expand_for_buffering(task).map_err(StagingGateError::Dispatch)? {
                sessions.buffer_write(session_id, shard_task);
            }
            InTxnRoute::Buffered
        };
        if let Some((plan, tenant_id)) = predicate_read {
            record_read_set(
                state,
                sessions,
                session_id,
                tenant_id,
                ReadCapture {
                    plan: &plan,
                    read_vshards: &ReadShards::new(),
                    found: false,
                },
            )
            .await;
        }
        stage_node_delete_tasks(state, sessions, session_id, derived, dispatch).await?;
        return Ok(route);
    }
}
