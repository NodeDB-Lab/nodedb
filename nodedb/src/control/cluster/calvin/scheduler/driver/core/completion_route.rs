// SPDX-License-Identifier: BUSL-1.1

//! Executor-response routing for the Calvin scheduler.
//!
//! [`Scheduler::handle_completion`] is the `completion_rx` arm of the main
//! `select!` loop: it classifies each executor response (disconnect, or the
//! txn's commit-resolution state) and routes it to the matching handler.
//! Every txn threads through the commit-barrier states in
//! [`super::commit_resolve`]; a txn parked in
//! [`CommitState::AwaitingVerdict`] has no outstanding bridge, so a completion
//! for it is a no-op that keeps it parked.

use super::super::types::CommitState;
use super::halt::{HaltReason, HaltStep};
use super::scheduler::Scheduler;
use crate::bridge::envelope::Response;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::types::RequestId;

impl Scheduler {
    /// Process a completed executor response (or disconnected channel).
    ///
    /// Called from the `completion_rx` arm of the main `select!` loop.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) async fn handle_completion(
        &mut self,
        txn_id: TxnId,
        request_id: RequestId,
        resp_opt: Option<Response>,
    ) {
        let Some(commit_state) = self.pending.get(&txn_id).map(|p| p.commit_state) else {
            // A response bridge exists only for a pending txn, and the txn
            // leaves `pending` only after its last response.
            tracing::error!(
                vshard_id = self.vshard_id,
                request_id = request_id.as_u64(),
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: executor completion for a txn with no pending entry; ignoring"
            );
            return;
        };
        let response = match resp_opt {
            Some(r) => r,
            None => {
                // The bridge task saw the channel close before any response,
                // so the request's outcome on this replica is unknown. Hold
                // the txn unapplied and halt.
                self.metrics.record_executor_error();
                self.halt_apply(
                    txn_id,
                    HaltReason::ResponseDisconnected,
                    HaltStep::awaited_by(commit_state),
                    format!(
                        "executor response channel for request {} closed before a response",
                        request_id.as_u64()
                    ),
                );
                return;
            }
        };

        let elapsed_ms = self
            .pending
            .get(&txn_id)
            .map(|p| p.dispatch_time.elapsed().as_millis() as u64)
            .unwrap_or(0);
        self.metrics.record_executor_txn_duration_ms(elapsed_ms);

        // A txn resolves through its commit-barrier state, OLLP answer
        // included. The flush installs a resolved redo record and runs no
        // OLLP check, so an OLLP answer under a later state is a failed step
        // that halts and holds. It never settles as a retry.
        match commit_state {
            CommitState::Staged => {
                // Stage failures remain participants in the barrier:
                // `resolve_staged_commit` turns any `Status::Error` into an
                // explicit false vote, parks locally, and waits for the
                // durable global verdict before issuing a drop.
                self.resolve_staged_commit(txn_id, &response);
            }
            CommitState::AwaitingRedoResolve => {
                self.finish_redo_resolve(txn_id, response);
            }
            CommitState::AwaitingResolve {
                committed,
                redo_lsn,
            } => {
                self.finish_resolved_commit(txn_id, response, committed, redo_lsn)
                    .await;
            }
            CommitState::AwaitingFlushTurn { .. }
            | CommitState::AwaitingResolveTurn
            | CommitState::AwaitingVersionRecord => {
                // A txn waiting for its flush or resolve turn has no request
                // out. A write-version record drains its own response.
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    request_id = request_id.as_u64(),
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    ?commit_state,
                    "calvin: executor completion for a txn with no request out; ignoring"
                );
            }
            CommitState::AwaitingVerdict => {
                // A parked txn dispatched no executor request (it is waiting on
                // the cross-shard verdict, not the Data Plane), so no response
                // bridge is outstanding for it — a completion here indicates a
                // logic error, not a real Data-Plane reply. Do NOT run the commit
                // tail or release locks (that can tear the transaction);
                // remain parked and let the verdict path resume it.
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    request_id = request_id.as_u64(),
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "calvin: executor completion for a txn parked on the commit barrier; \
                     ignoring (no bridge is outstanding while AwaitingVerdict)"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::halt::HaltReason;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        error_response, make_sequenced_txn, scheduler_with_pending, staged_pending,
    };
    use crate::control::cluster::calvin::scheduler::metrics::apply_halt_reason;

    /// A response channel that closes before a response leaves the txn's
    /// outcome unknown: the txn stays pending and unapplied, the scheduler
    /// halts, and the node marker names the txn.
    #[tokio::test]
    async fn disconnected_response_holds_txn_unapplied_and_sets_node_marker() {
        let txn_id = TxnId::new(5, 1);
        let (mut scheduler, _dir) = scheduler_with_pending(
            txn_id,
            CommitState::AwaitingResolve {
                committed: true,
                redo_lsn: None,
            },
        );

        scheduler
            .handle_completion(txn_id, RequestId::new(9), None)
            .await;

        assert!(
            !scheduler.applied.is_applied(5, 1),
            "an unknown outcome must not mark the position applied"
        );
        assert!(scheduler.pending.contains_key(&txn_id));
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::ResponseDisconnected)
        );
        let marker = scheduler.shared.sequencer_halt.apply_halt().report();
        assert_eq!(
            marker.map(|h| (h.vshard_id, h.epoch, h.position, h.step)),
            Some((7, 5, 1, "flush"))
        );
        assert_eq!(scheduler.metrics.apply_halted.load(Ordering::Relaxed), 1);
        assert_eq!(
            scheduler.metrics.apply_halt_reason.load(Ordering::Relaxed),
            apply_halt_reason::RESPONSE_DISCONNECTED as u64
        );
    }

    /// A second disconnect keeps the first cause and holds its txn too.
    #[tokio::test]
    async fn second_halt_keeps_the_first_cause() {
        let first = TxnId::new(5, 1);
        let second = TxnId::new(6, 0);
        let (mut scheduler, _dir) = scheduler_with_pending(first, CommitState::Staged);
        let mut pending = staged_pending(make_sequenced_txn(6, 0), second);
        pending.commit_state = CommitState::AwaitingRedoResolve;
        scheduler.pending.insert(second, pending);

        scheduler
            .handle_completion(first, RequestId::new(9), None)
            .await;
        scheduler
            .handle_completion(second, RequestId::new(10), None)
            .await;

        assert!(!scheduler.applied.is_applied(6, 0));
        assert!(scheduler.pending.contains_key(&second));
        let marker = scheduler.shared.sequencer_halt.apply_halt().report();
        assert_eq!(
            marker.map(|h| (h.epoch, h.position, h.step)),
            Some((5, 1, "stage"))
        );
    }

    /// A committed flush that answers `OllpRetryRequired` wrote nothing on
    /// this replica. The txn stays pending and unapplied, its redo records
    /// stay open, and the scheduler halts on the flush.
    #[tokio::test]
    async fn an_ollp_answer_to_a_committed_flush_halts_and_holds() {
        let txn_id = TxnId::new(5, 1);
        let (mut scheduler, _dir) = scheduler_with_pending(
            txn_id,
            CommitState::AwaitingResolve {
                committed: true,
                redo_lsn: None,
            },
        );

        scheduler
            .handle_completion(
                txn_id,
                RequestId::new(9),
                Some(error_response(
                    crate::bridge::envelope::ErrorCode::OllpRetryRequired,
                )),
            )
            .await;

        assert!(!scheduler.applied.is_applied(5, 1));
        assert!(
            scheduler.pending.contains_key(&txn_id),
            "the txn must stay pending"
        );
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::FlushFailed)
        );
    }
}
