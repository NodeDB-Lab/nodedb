// SPDX-License-Identifier: BUSL-1.1

//! Executor-response routing for the Calvin scheduler.
//!
//! [`Scheduler::handle_completion`] is the `completion_rx` arm of the main
//! `select!` loop: it classifies each executor response (disconnect, OLLP
//! mismatch, staged commit-resolution state, or direct apply) and routes it to
//! the matching handler. Staged transactions thread through the commit-barrier
//! states in [`super::commit_resolve`]; a txn parked in
//! [`CommitState::AwaitingVerdict`] has no outstanding bridge, so a completion
//! for it is a no-op that keeps it parked.

use super::super::types::CommitState;
use super::halt::{HaltReason, HaltStep};
use super::owed::SchedulerProposal;
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
        let response = match resp_opt {
            Some(r) => r,
            None => {
                // The bridge task saw the channel close before any response,
                // so the request's outcome on this replica is unknown. Hold
                // the txn unapplied and halt.
                self.metrics.record_executor_error();
                let state = self.pending.get(&txn_id).and_then(|p| p.commit_state);
                self.halt_apply(
                    txn_id,
                    HaltReason::ResponseDisconnected,
                    HaltStep::awaited_by(state),
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

        // A staged transaction resolves through its commit-barrier state, OLLP
        // answer included. The flush installs a resolved redo record and runs
        // no OLLP check, so an OLLP answer under a commit state is a failed
        // step that halts and holds. It never settles as a retry.
        let commit_state = self.pending.get(&txn_id).and_then(|p| p.commit_state);

        // OLLP mismatch: the active executor detected predicate drift and returned
        // OllpRetryRequired without writing. The retry loop is now COORDINATOR-owned
        // (`run_dependent_with_retry`): the scheduler must NOT re-submit a stale
        // prediction. Instead it (1) releases the aborted attempt's locks and
        // (2) signals the coordinator's completion waiter via the registry so it
        // can run a FRESH reconnaissance and resubmit.
        if commit_state.is_none()
            && response.status == crate::bridge::envelope::Status::Error
            && response.error_code.as_deref()
                == Some(&crate::bridge::envelope::ErrorCode::OllpRetryRequired)
        {
            // A mismatch is a normal OLLP retry signal, not a failure: the executor
            // correctly detected predicate drift and declined to write. Count it as
            // a received executor response, but NOT as an executor error or infra
            // abort — those would inflate failure metrics on every routine retry.
            self.metrics.record_completed();
            // (1) Release the aborted attempt's locks and clean up pending state.
            // This fixes the lock-leak: the old `schedule_ollp_retry` re-submitted
            // without ever releasing the aborted attempt's locks.
            self.on_txn_complete(txn_id);
            // OLLP mismatch broadcast is LEADER-ONLY. The optimistic-lock
            // verification runs only on the data-group leader (the data plane
            // skips it on followers — see the `ollp_is_group_leader` gate in the
            // bulk-DML handlers), so by construction only a leader's executor can
            // return `OllpRetryRequired`. This guard is defense-in-depth: a
            // non-leader scheduler must never broadcast a mismatch — a lagging
            // follower could otherwise poison an attempt the leader already
            // completed, exhausting retries on a static dataset. A non-leader
            // simply releases locks (done above) and returns; the leader owns the
            // single mismatch signal that the completion registry observes.
            if !self.is_group_leader() {
                tracing::debug!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "calvin: OllpRetryRequired observed on non-leader; NOT broadcasting mismatch \
                     (leader owns the verification decision)"
                );
                return;
            }
            // (2) Broadcast the mismatch signal via the sequencer-group Raft so that
            // the coordinator's CalvinCompletionRegistry fires on EVERY replica,
            // including remote nodes. The SequencerStateMachine::apply() arm for
            // OllpMismatch calls note_ollp_mismatch, waking the coordinator's
            // retry-loop waiter wherever it is — mirrors how CompletionAck is
            // delivered to remote coordinators.
            self.propose_sequencer_entry(txn_id, SchedulerProposal::OllpMismatch);
            return;
        }

        // Staged static Calvin apply: a STAGED response carries the local commit
        // vote; drive the vote-and-park and let the verdict resume the
        // flush-or-drop. Dependent / active txns carry no `commit_state` and apply
        // directly below.
        match commit_state {
            Some(CommitState::Staged) => {
                // Stage failures remain participants in the barrier:
                // `resolve_staged_commit` turns any `Status::Error` into an
                // explicit false vote, parks locally, and waits for the
                // durable global verdict before issuing a drop.
                self.resolve_staged_commit(txn_id, &response);
                return;
            }
            Some(CommitState::AwaitingRedoResolve) => {
                self.finish_redo_resolve(txn_id, response);
                return;
            }
            Some(CommitState::AwaitingResolve {
                committed,
                redo_lsn,
            }) => {
                self.finish_resolved_commit(txn_id, response, committed, redo_lsn);
                return;
            }
            Some(CommitState::AwaitingVerdict) => {
                // A parked txn dispatched no executor request (it is waiting on
                // the cross-shard verdict, not the Data Plane), so no response
                // bridge is outstanding for it — a completion here indicates a
                // logic error, not a real Data-Plane reply. Do NOT run the commit
                // tail or release locks (that could tear the transaction);
                // remain parked and let the verdict path resume it.
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    request_id = request_id.as_u64(),
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "calvin: executor completion for a txn parked on the commit barrier; \
                     ignoring (no bridge is outstanding while AwaitingVerdict)"
                );
                return;
            }
            None => {}
        }

        if response.status != crate::bridge::envelope::Status::Ok {
            // A failed direct apply must not leave the txn parked with its locks
            // held: that wedges every txn queued behind those keys and freezes
            // this vShard's epoch watermark, and no sweep re-drives the entry.
            // Surface the infra abort and force completion.
            tracing::error!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: executor response was not Ok; forcing infra-abort completion so locks \
                 release and the epoch advances"
            );
            self.metrics.record_executor_error();
            self.metrics.record_infra_abort(
                crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::IO_ERROR,
            );
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
            return;
        }

        // Observe whether the applying participant reported its slice of the
        // transaction's reads as no longer current against the local write
        // versions. Direct-apply (dependent/active) observation only: the
        // staged path folds this into its commit vote instead. `None` means
        // no read-set was checked.
        if response.read_set_valid == Some(false) {
            self.shared
                .calvin_counters
                .read_set_validation_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // `false` means the commit tail halted the scheduler: the txn stays
        // pending and unapplied.
        if self.commit_apply_tail(txn_id, response, None) {
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
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
        pending.commit_state = Some(CommitState::AwaitingRedoResolve);
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
            Some(HaltReason::FlushFailed)
        );
    }
}
