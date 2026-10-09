// SPDX-License-Identifier: BUSL-1.1

//! Single-shard COMMIT: resolve the transaction's staged post-images into one
//! redo record, then commit that record through the vShard's apply log.
//!
//! The vShard has one apply log, its data-group log: the record is proposed
//! there and every replica, this node included, appends it to its own WAL and
//! installs it when the entry commits. COMMIT returns only once the record is
//! durable and installed here.

use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::gateway::RouteDecision;
use crate::control::state::SharedState;
use crate::control::wal_replication::encode::transaction_redo_entry;
use crate::control::wal_replication::propose_replicated_entry;
use crate::control::wal_replication::transaction_redo::{RedoTarget, TransactionRedoPayload};
use crate::wal::RedoPublish;

use super::super::outcome::{AbortReason, TxnDataPlane};

/// Single-shard commit: resolve the transaction's staged post-images into one
/// `RedoRecord` that also carries `publishes`, then commit it through the
/// vShard's apply log. Returns `Some(reason)` on failure.
pub(super) async fn dispatch_single_shard(
    state: &SharedState,
    dp: &impl TxnDataPlane,
    buffered: &[PhysicalTask],
    tenant_id: crate::types::TenantId,
    vshard_id: crate::types::VShardId,
    publishes: Vec<RedoPublish>,
    ts_preview: &super::ts_rejections::RejectedByCollection,
) -> Option<AbortReason> {
    let plans: Vec<PhysicalPlan> = buffered.iter().map(|t| t.plan.clone()).collect();
    let database_id = buffered
        .first()
        .map_or(crate::types::DatabaseId::DEFAULT, |task| task.database_id);
    if buffered.iter().any(|task| task.database_id != database_id) {
        return Some(AbortReason::Dispatch(crate::Error::BadRequest {
            detail: "transaction spans multiple databases".to_owned(),
        }));
    }

    // txn_id is present for any staged commit (buffer_write stamps it).
    let Some(txn_id) = buffered.first().and_then(|t| t.txn_id) else {
        return Some(AbortReason::Dispatch(crate::Error::Internal {
            detail: "single-shard commit: buffered task carries no txn_id".into(),
        }));
    };

    // 1. Resolve the transaction's staged post-images into ONE replayable
    //    RedoRecord. Read-only: reads `txn_overlays[txn_id]` on the owning
    //    core, writes nothing.
    let resolve_task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id,
        plan: PhysicalPlan::Meta(MetaOp::ResolveTxn {
            txn_id,
            plans: plans.clone(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let resolve_resp = match dp.dispatch_no_wal(resolve_task).await {
        Ok(r) if r.status == Status::Ok => r,
        Ok(r) => {
            return Some(AbortReason::BatchRejected {
                code: r.error_code.as_deref().cloned(),
            });
        }
        Err(e) => return Some(AbortReason::Dispatch(e)),
    };
    let mut redo = match crate::wal::RedoRecord::from_bytes(resolve_resp.payload.as_bytes()) {
        Ok(r) => r,
        Err(e) => {
            return Some(AbortReason::Dispatch(crate::Error::Internal {
                detail: format!("single-shard commit: resolve redo decode failed: {e}"),
            }));
        }
    };

    // Re-verify local vShard leadership before anything durable happens.
    // `run_commit` resolved this vShard as `Local` and validated the read set
    // against this node's write versions, but a leadership handoff can land
    // during the `ResolveTxn` await above. The new leader can already have
    // applied writes this node's validation never saw, so the commit aborts
    // side-effect-free and retryable: the retry sees the vShard is non-local
    // and routes through Calvin's replicated barrier.
    if !matches!(
        crate::control::gateway::live_leaders::resolve_live_decision(state, vshard_id.as_u32()),
        RouteDecision::Local
    ) {
        return Some(AbortReason::Serialization);
    }

    // A transaction with no durable write and no message installs nothing.
    redo.publishes = publishes;
    if redo.ops.is_empty() && redo.publishes.is_empty() {
        return None;
    }
    redo.cross_shard_applied = dp.applied_key();
    // The lines this authoritative resolve rejected, for the notice owed
    // once the record commits.
    let ts_committed = super::ts_rejections::redo_rejected_by_collection(&redo);

    // 2. Commit the record through the vShard's apply log. The payload carries
    //    the resolve-time bitemporal stamps inside the redo sub-records, so
    //    every replica installs each row on the same version key.
    let payload = match TransactionRedoPayload::from_commit(
        state,
        database_id,
        tenant_id,
        redo,
        &plans,
        dp.event_source(),
    ) {
        Ok(payload) => payload,
        Err(e) => return Some(AbortReason::Dispatch(e)),
    };
    let outcome = commit_redo(
        state,
        RedoTarget {
            tenant_id,
            database_id,
            vshard_id,
        },
        &payload,
    )
    .await;
    match outcome {
        Ok(applied) => {
            // The install's counts cover the resolve's and add the rows it
            // rejected at the record's log position.
            let committed = super::ts_rejections::with_applied(ts_committed, &applied);
            super::ts_rejections::raise_commit_rejections(ts_preview, &committed);
            None
        }
        Err(reason) => Some(reason),
    }
}

/// Commit the messages of a transaction with no buffered write in a redo
/// record of their own, on the home vShard of the first message's topic.
/// Returns `Some(reason)` on failure, `None` when there is nothing to commit.
pub(super) async fn commit_publishes(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    publishes: Vec<RedoPublish>,
) -> Option<AbortReason> {
    let first = publishes.first()?;
    let database_id = crate::types::DatabaseId::new(first.database_id);
    let vshard_id = crate::types::VShardId::new(crate::event::topic::publish::topic_vshard(
        database_id,
        &first.topic,
    ));
    let redo = crate::wal::RedoRecord {
        version: 1,
        ops: Vec::new(),
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes,
        row_changes: Vec::new(),
    };
    let payload = match TransactionRedoPayload::from_commit(
        state,
        database_id,
        tenant_id,
        redo,
        &[],
        crate::event::EventSource::User,
    ) {
        Ok(payload) => payload,
        Err(e) => return Some(AbortReason::Dispatch(e)),
    };
    commit_redo(
        state,
        RedoTarget {
            tenant_id,
            database_id,
            vshard_id,
        },
        &payload,
    )
    .await
    .err()
}

/// Commit `payload` through the vShard's apply log and wait until it is
/// durable and installed on this node. Returns the lines and rows the
/// install's timeseries batches rejected, by collection.
///
/// A fail point ahead of the real commit lets a test force this exact
/// synchronous-failure branch (an `AbortReason` back to `run_commit`,
/// which compensates a finalized DDL) without touching disk.
async fn commit_redo(
    state: &SharedState,
    target: RedoTarget,
    payload: &TransactionRedoPayload,
) -> Result<super::ts_rejections::RejectedByCollection, AbortReason> {
    if let Err(e) = inject_commit_failure() {
        return Err(AbortReason::Dispatch(e));
    }
    let proposer = state.async_raft_proposer().map_err(AbortReason::Dispatch)?;
    // The proposer forwards to the group leader and returns once the entry is
    // committed and applied on this node by the apply loop.
    let entry = transaction_redo_entry(
        target.tenant_id,
        target.database_id,
        target.vshard_id,
        payload,
    );
    let deadline = crate::control::wal_replication::statement_propose_deadline(state);
    match propose_replicated_entry(state, proposer, entry, deadline).await {
        Ok((applied, _)) => Ok(super::ts_rejections::applied_rejected_by_collection(
            &applied,
        )),
        Err(crate::Error::DataPlane(code)) => Err(AbortReason::BatchRejected { code: Some(code) }),
        Err(e) => {
            tracing::warn!(error = %e, "transaction redo commit failed");
            Err(AbortReason::Dispatch(e))
        }
    }
}

/// The `commit::single_shard_redo_commit` fail point. Compiles to `Ok(())`
/// outside the `failpoints` feature.
fn inject_commit_failure() -> crate::Result<()> {
    crate::fail_point_err!("commit::single_shard_redo_commit", |detail| {
        crate::Error::Internal { detail }
    });
    Ok(())
}
