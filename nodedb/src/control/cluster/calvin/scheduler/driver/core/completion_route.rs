// SPDX-License-Identifier: BUSL-1.1

//! Executor-response routing for the Calvin scheduler.
//!
//! [`Scheduler::handle_completion`] is the `completion_rx` arm of the main
//! `select!` loop: it classifies each executor response (disconnect, or the
//! txn's commit-resolution state) and routes it to the matching handler.
//! Only the answer to the request a txn waits for drives it. An answer to a
//! step the txn left, after a demotion or an install from another copy, is
//! ignored.

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
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn handle_completion(
        &mut self,
        txn_id: TxnId,
        request_id: RequestId,
        resp_opt: Option<Response>,
    ) {
        if !self.awaits(txn_id, request_id) {
            tracing::debug!(
                vshard_id = self.vshard_id,
                request_id = request_id.as_u64(),
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: executor answer to a step the txn left; ignoring"
            );
            return;
        }
        let Some(commit_state) = self.pending.get(&txn_id).map(|p| p.commit_state) else {
            return;
        };
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.awaiting = None;
        }
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

        match commit_state {
            CommitState::Staged => {
                // Stage failures remain participants in the barrier:
                // `resolve_staged_commit` proposes the response's abort vote,
                // parks locally, and waits for the durable global verdict
                // before it issues a drop.
                self.resolve_staged_commit(txn_id, &response);
            }
            CommitState::AwaitingRedoResolve => {
                self.finish_redo_resolve(txn_id, response);
            }
            CommitState::AwaitingDrop => {
                self.finish_drop(txn_id, &response);
            }
            CommitState::ReadingPassive => {
                self.finish_passive_read(txn_id, &response);
            }
            CommitState::Following
            | CommitState::AwaitingVerdict
            | CommitState::AwaitingResolveTurn
            | CommitState::AwaitingRedoApply { .. }
            | CommitState::AwaitingRestage => {
                // A txn in these states sends no request it waits for. A
                // recorded request names a step it left.
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    request_id = request_id.as_u64(),
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    ?commit_state,
                    "calvin: executor completion for a txn with no request out; ignoring"
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
    #[test]
    fn disconnected_response_holds_txn_unapplied_and_sets_node_marker() {
        let txn_id = TxnId::new(5, 1);
        let (mut scheduler, _dir) =
            scheduler_with_pending(txn_id, CommitState::AwaitingRedoResolve);

        scheduler.handle_completion(txn_id, RequestId::new(9), None);

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
            Some((7, 5, 1, "resolve"))
        );
        assert_eq!(scheduler.metrics.apply_halted.load(Ordering::Relaxed), 1);
        assert_eq!(
            scheduler.metrics.apply_halt_reason.load(Ordering::Relaxed),
            apply_halt_reason::RESPONSE_DISCONNECTED as u64
        );
    }

    /// A second disconnect keeps the first cause and holds its txn too.
    #[test]
    fn second_halt_keeps_the_first_cause() {
        let first = TxnId::new(5, 1);
        let second = TxnId::new(6, 0);
        let (mut scheduler, _dir) = scheduler_with_pending(first, CommitState::Staged);
        let mut pending = staged_pending(make_sequenced_txn(6, 0), second);
        pending.commit_state = CommitState::AwaitingRedoResolve;
        pending.awaiting = Some(RequestId::new(10));
        scheduler.pending.insert(second, pending);

        scheduler.handle_completion(first, RequestId::new(9), None);
        scheduler.handle_completion(second, RequestId::new(10), None);

        assert!(!scheduler.applied.is_applied(6, 0));
        assert!(scheduler.pending.contains_key(&second));
        let marker = scheduler.shared.sequencer_halt.apply_halt().report();
        assert_eq!(
            marker.map(|h| (h.epoch, h.position, h.step)),
            Some((5, 1, "stage"))
        );
    }

    /// A refused resolve of a committed txn leaves no redo to propose. The
    /// txn stays pending and unapplied, and the scheduler halts on the
    /// resolve.
    #[test]
    fn a_refused_resolve_of_a_committed_txn_halts_and_holds() {
        let txn_id = TxnId::new(5, 1);
        let (mut scheduler, _dir) =
            scheduler_with_pending(txn_id, CommitState::AwaitingRedoResolve);

        scheduler.handle_completion(
            txn_id,
            RequestId::new(9),
            Some(error_response(
                crate::bridge::envelope::ErrorCode::OllpRetryRequired,
            )),
        );

        assert!(!scheduler.applied.is_applied(5, 1));
        assert!(
            scheduler.pending.contains_key(&txn_id),
            "the txn must stay pending"
        );
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::ResolveFailed)
        );
    }

    /// An answer to a request the txn no longer waits for, as after a
    /// demotion, changes nothing.
    #[test]
    fn an_answer_to_a_step_the_txn_left_is_ignored() {
        let txn_id = TxnId::new(5, 1);
        let (mut scheduler, _dir) =
            scheduler_with_pending(txn_id, CommitState::AwaitingRedoResolve);

        scheduler.handle_completion(txn_id, RequestId::new(8), None);

        assert!(scheduler.apply_halt().is_none());
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingRedoResolve)
        );
        assert_eq!(
            scheduler.pending.get(&txn_id).and_then(|p| p.awaiting),
            Some(RequestId::new(9))
        );
    }
}
