// SPDX-License-Identifier: BUSL-1.1

//! Resume-on-verdict, verdict-signal handling, and the stall re-probe sweep
//! for a staged Calvin transaction parked on the cross-shard commit barrier.

use std::sync::atomic::Ordering;
use std::time::Instant;

use nodedb_cluster::calvin::VerdictSignal;

use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason;

impl Scheduler {
    /// Resume a txn parked in [`CommitState::AwaitingVerdict`] once the durable
    /// GLOBAL verdict is known: dispatch its flush (commit) or drop (abort).
    ///
    /// `committed` is the authoritative cross-shard verdict — NOT this shard's
    /// local vote. On commit, dispatches `MetaOp::CalvinResolve` and moves the
    /// txn to [`CommitState::AwaitingRedoResolve`] (the resolved redo is
    /// WAL-appended and the flush dispatched from [`Self::finish_redo_resolve`]).
    /// On abort, dispatches the drop directly and moves the txn to
    /// [`CommitState::AwaitingResolve`]. Bumps the flushed / dropped counter. The
    /// commit tail runs later in [`Self::finish_resolved_commit`], once the
    /// flush/drop response arrives.
    ///
    /// Double-resume guard: the verdict push and the probe-on-park (and the
    /// stall re-probe sweep) can all fire for one txn, so this first confirms the
    /// txn is still `Some(AwaitingVerdict)` — if it already transitioned out
    /// (resolve/drop dispatched, or completed), this is a no-op. This guarantees
    /// the flush/drop is dispatched exactly once.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resume_on_verdict(
        &mut self,
        txn_id: TxnId,
        committed: bool,
    ) {
        // Guard: only a still-parked txn resumes. Mirrors `handle_completion`'s
        // state-match so a duplicate push/probe/timeout is idempotent.
        if !matches!(
            self.pending.get(&txn_id).and_then(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        ) {
            return;
        }

        let dispatched = if committed {
            // Resolve the staged post-images into a replayable `RedoRecord`
            // first; the redo is WAL-appended (in `finish_redo_resolve`) before
            // the flush is dispatched, restoring restart durability for this
            // vShard's slice of a multi-shard Calvin commit.
            self.dispatch_calvin_resolve(txn_id)
        } else {
            self.dispatch_commit_resolution(txn_id, false, None)
        };
        if !dispatched {
            // Resolve/drop dispatch failed: complete the txn as an infra error so
            // its locks release and the epoch advances rather than stalling. The
            // staged buffer is reclaimed by a later drop or on core teardown.
            self.metrics.record_executor_error();
            self.metrics
                .record_infra_abort(infra_abort_reason::IO_ERROR);
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
            return;
        }

        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.commit_state = Some(if committed {
                CommitState::AwaitingRedoResolve
            } else {
                CommitState::AwaitingResolve {
                    committed: false,
                    redo_lsn: None,
                }
            });
            // No longer parked: clear the stall deadline.
            pending.verdict_deadline = None;
        }

        if committed {
            self.shared
                .calvin_counters
                .commits_flushed
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.shared
                .calvin_counters
                .commits_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Handle a pushed [`VerdictSignal`] from this node's completion registry.
    ///
    /// Matches the signal to the parked txn by `(epoch, position)` and resumes
    /// it. A signal for a txn this scheduler does not host, or one that already
    /// resumed, is a harmless no-op (the double-resume guard covers the latter).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn handle_verdict_signal(
        &mut self,
        signal: VerdictSignal,
    ) {
        let txn_id = TxnId::new(signal.epoch, signal.position);
        self.resume_on_verdict(txn_id, signal.verdict.is_commit());
    }

    /// Sweep parked `AwaitingVerdict` txns whose stall deadline has passed.
    ///
    /// For each stalled txn, RE-PROBE the durable verdict: if it is now known,
    /// resume (a push we dropped on a full channel, or a verdict that landed
    /// after the last probe). If it is STILL unknown, KEEP WAITING — hold locks,
    /// emit a stall metric + warning, and re-arm the deadline so the warning is
    /// rate-limited rather than per-iteration. It NEVER releases locks and NEVER
    /// unilaterally aborts: a participant cannot know whether a peer already
    /// flushed a COMMIT, so aborting one side while a peer committed would tear
    /// the transaction. The verdict is guaranteed to arrive eventually — a
    /// post-failover leader re-aggregates the replicated votes (seeded on every
    /// replica) into the same verdict — so waiting is always the safe action.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn check_awaiting_verdict_stalls(
        &mut self,
    ) {
        // no-determinism: stall-detection clock drives warnings/metrics only; this path holds locks and never aborts, so it cannot affect the replicated outcome.
        let now = Instant::now();
        let stalled: Vec<TxnId> = self
            .pending
            .iter()
            .filter(|(_, p)| matches!(p.commit_state, Some(CommitState::AwaitingVerdict)))
            .filter(|(_, p)| p.verdict_deadline.is_some_and(|d| now >= d))
            .map(|(id, _)| *id)
            .collect();

        for txn_id in stalled {
            if let Some(verdict) = self.registry.verdict(nodedb_cluster::calvin::TxnId::new(
                txn_id.epoch,
                txn_id.position,
            )) {
                self.resume_on_verdict(txn_id, verdict);
                continue;
            }

            // Verdict still unknown: keep waiting, hold locks, never abort.
            self.metrics.record_verdict_stall();
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: staged txn still awaiting the cross-shard verdict past its stall \
                 deadline; HOLDING locks and waiting (never aborting — a peer may have already \
                 flushed a commit). The verdict is guaranteed to arrive."
            );
            if let Some(pending) = self.pending.get_mut(&txn_id) {
                pending.verdict_deadline = Some(now + self.config.verdict_stall_warn());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::calvin::{
        AbortReason, CalvinCompletionRegistry, ParticipantVote, VerdictOutcome,
    };
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_physical::physical_plan::meta::MetaOp;

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_sequenced_txn, staged_pending, staged_response,
    };

    /// A false vote from either participant makes the only global verdict abort;
    /// applying that durable verdict broadcasts the abort to every parked local
    /// participant. The scheduler's `resume_on_verdict(false)` then dispatches a
    /// drop, never a resolve/flush, on each recipient.
    #[tokio::test]
    async fn two_participant_false_vote_broadcasts_global_abort_to_every_scheduler() {
        let registry = CalvinCompletionRegistry::new_detached();
        let txn = nodedb_cluster::calvin::TxnId::new(14, 2);
        let txn_id = TxnId::new(14, 2);
        let (mut first_scheduler, _first_dir, mut first_data) =
            build_test_scheduler_with_data_side(7, Arc::clone(&registry));
        let (mut second_scheduler, _second_dir, mut second_data) =
            build_test_scheduler_with_data_side(9, Arc::clone(&registry));
        first_scheduler
            .pending
            .insert(txn_id, staged_pending(make_sequenced_txn(14, 2), txn_id));
        second_scheduler
            .pending
            .insert(txn_id, staged_pending(make_sequenced_txn(14, 2), txn_id));

        // Local staging votes only park their own staged slices; neither the
        // affirmative nor the failed participant may resolve or drop unilaterally.
        first_scheduler.resolve_staged_commit(txn_id, &staged_response(Status::Ok, Some(true)));
        second_scheduler.resolve_staged_commit(txn_id, &staged_response(Status::Error, None));
        for (scheduler, data_side) in [
            (&first_scheduler, &mut first_data),
            (&second_scheduler, &mut second_data),
        ] {
            assert!(matches!(
                scheduler
                    .pending
                    .get(&txn_id)
                    .and_then(|pending| pending.commit_state),
                Some(CommitState::AwaitingVerdict)
            ));
            assert!(data_side.request_rx.try_pop().is_err());
        }

        // Model the replicated vote entries and their resulting durable verdict.
        // The shared registry sends each scheduler's actual registered channel.
        registry.seed_expected(txn, 2);
        registry.note_vote(txn, 7, ParticipantVote::Commit);
        assert!(registry.drain_unproposed_verdicts().is_empty());
        registry.note_vote(
            txn,
            9,
            ParticipantVote::Abort(Some(AbortReason::SerializationConflict)),
        );
        assert_eq!(
            registry.drain_unproposed_verdicts(),
            vec![(
                txn,
                VerdictOutcome::Abort(Some(AbortReason::SerializationConflict))
            )]
        );
        registry.note_verdict(
            txn,
            VerdictOutcome::Abort(Some(AbortReason::SerializationConflict)),
        );
        assert_eq!(registry.verdict(txn), Some(false));

        let first_signal = first_scheduler
            .verdict_rx
            .try_recv()
            .expect("registry must signal the first registered scheduler");
        let second_signal = second_scheduler
            .verdict_rx
            .try_recv()
            .expect("registry must signal the second registered scheduler");
        first_scheduler.handle_verdict_signal(first_signal);
        second_scheduler.handle_verdict_signal(second_signal);

        for (scheduler, data_side) in [
            (&first_scheduler, &mut first_data),
            (&second_scheduler, &mut second_data),
        ] {
            assert!(matches!(
                scheduler
                    .pending
                    .get(&txn_id)
                    .and_then(|pending| pending.commit_state),
                Some(CommitState::AwaitingResolve {
                    committed: false,
                    redo_lsn: None
                })
            ));
            let request = data_side
                .request_rx
                .try_pop()
                .expect("global abort must dispatch a drop to every participant");
            assert!(matches!(
                request.inner.plan,
                PhysicalPlan::Meta(MetaOp::CalvinDrop {
                    epoch: 14,
                    position: 2
                })
            ));
            assert!(data_side.request_rx.try_pop().is_err());
        }
    }
}
