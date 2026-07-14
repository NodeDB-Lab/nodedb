// SPDX-License-Identifier: BUSL-1.1

//! Calvin multi-shard arm of the neutral COMMIT orchestrator.
//!
//! Routes a transaction whose buffered writes span two or more vShards through
//! the Calvin sequencer. Strict mode routes the whole buffered batch to the
//! sequencer-group leader via `dispatch_tasks_to_calvin` — one atomic
//! cross-shard transaction bound by the durable Vote/Verdict barrier.
//! Best-effort mode groups the buffered writes by vShard and submits each group
//! as an INDEPENDENT single-vShard Calvin transaction (via
//! `build_single_vshard_tx_class` then `submit_calvin_routed`) — the SAME
//! deterministic sequencer funnel, so each vShard gets an epoch-anchored
//! bitemporal stamp and a `TransactionRedo` WAL record, while remaining
//! non-atomic ACROSS vShards (no global vote binds them; a failure on one vShard
//! does not roll back another).

use std::net::SocketAddr;

use crate::control::planner::calvin::{
    CrossShardTxnMode, TxnDispatchPosition, build_single_vshard_tx_class, dispatch_tasks_to_calvin,
    submit_calvin_routed,
};
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::state::SharedState;
use nodedb_physical::physical_task::PhysicalTask;

use super::outcome::AbortReason;
use super::store::SessionStore;

/// Dispatch a multi-shard transaction batch through Calvin. Strict commits the
/// whole batch atomically through the leader-routed Vote/Verdict barrier;
/// best-effort submits one independent single-vShard Calvin transaction per
/// vShard. Returns `Some(reason)` on failure, `None` on success.
pub(super) async fn run_commit_calvin(
    sessions: &SessionStore,
    addr: &SocketAddr,
    state: &SharedState,
    buffered: &[PhysicalTask],
    tenant_id: crate::types::TenantId,
    reads: &[ReadSetEntry],
) -> Option<AbortReason> {
    let cross_shard_mode = sessions.cross_shard_txn_mode(addr);

    match cross_shard_mode {
        CrossShardTxnMode::Strict => {
            // The sequencer inbox must be wired for the strict cross-shard path;
            // surface a deployment-neutral `SequencerUnavailable` if it is not.
            if state.sequencer_inbox.get().is_none() {
                return Some(AbortReason::Dispatch(crate::Error::SequencerUnavailable));
            }
            // Route the buffered cross-shard batch through the sequencer-group
            // leader via the routed submit-and-await — the whole batch commits
            // atomically. This is the COMMIT flush of a buffered explicit block,
            // NOT a mid-block single statement, so the mid-block cross-shard guard
            // must not fire (hence `CommitFlush`).
            match dispatch_tasks_to_calvin(
                state,
                buffered,
                tenant_id,
                cross_shard_mode,
                TxnDispatchPosition::CommitFlush,
                reads,
            )
            .await
            {
                Ok(_) => None,
                Err(crate::Error::CalvinSerializationConflict) => Some(AbortReason::Serialization),
                Err(e) => Some(AbortReason::Dispatch(e)),
            }
        }
        CrossShardTxnMode::BestEffortNonAtomic => {
            // The sequencer funnel each per-vShard submit uses requires the inbox
            // (or, in cluster mode, a routable sequencer leader); fail fast and
            // deployment-neutral if it is not wired, mirroring the strict arm.
            if state.sequencer_inbox.get().is_none() {
                return Some(AbortReason::Dispatch(crate::Error::SequencerUnavailable));
            }
            // Group the buffered writes by vShard. Each group becomes ONE
            // independent single-vShard Calvin transaction, sequenced through the
            // SAME deterministic funnel the contended point-write path uses
            // (`build_single_vshard_tx_class` + `submit_calvin_routed`): the
            // scheduler resolves it into a `TransactionRedo`, WAL-appends it, and
            // the Calvin flush sets `epoch_system_ms` so every engine's bitemporal
            // stamp is epoch-anchored and byte-identical on replay.
            //
            // Non-atomic ACROSS vShards is preserved by construction: each group
            // is a separate submit-and-await with its own single-participant
            // verdict (no cross-shard vote barrier). On the FIRST failure we
            // surface the reason and stop — we do NOT roll back vShards that have
            // already committed, exactly as the mode's contract requires.
            let mut by_vshard: std::collections::BTreeMap<u32, Vec<PhysicalTask>> =
                std::collections::BTreeMap::new();
            for task in buffered {
                by_vshard
                    .entry(task.vshard_id.as_u32())
                    .or_default()
                    .push(task.clone());
            }
            for (_vshard_u32, tasks) in by_vshard {
                // Empty read-set: best-effort performs no cross-shard OCC (the
                // multi-shard COMMIT path never ran `si_conflict_abort`), so each
                // group carries no versioned reads — matching the single-vShard
                // submit `route_write_to_calvin` uses.
                let tx_class = match build_single_vshard_tx_class(&tasks, tenant_id, &[]) {
                    Ok(tc) => tc,
                    Err(e) => return Some(AbortReason::Dispatch(e)),
                };
                match submit_calvin_routed(state, tx_class).await {
                    Ok(_) => {}
                    Err(crate::Error::CalvinSerializationConflict) => {
                        return Some(AbortReason::Serialization);
                    }
                    Err(e) => return Some(AbortReason::Dispatch(e)),
                }
            }
            None
        }
    }
}
