// SPDX-License-Identifier: BUSL-1.1

//! Resume-on-verdict, verdict-signal handling, and the stall re-probe sweep
//! for a granted Calvin transaction waiting on the cross-shard commit
//! barrier.

use std::sync::atomic::Ordering;
use std::time::Instant;

use nodedb_cluster::calvin::VerdictSignal;

use crate::control::cluster::calvin::scheduler::driver::core::deferred::{
    DispatchOutcome, DispatchStep,
};
use crate::control::cluster::calvin::scheduler::driver::core::owed::SchedulerProposal;
use crate::control::cluster::calvin::scheduler::driver::core::process::LedgerMark;
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Act on the durable GLOBAL verdict of `txn_id`.
    ///
    /// `committed` is the authoritative cross-shard verdict, never this
    /// shard's local vote.
    ///
    /// A leader's txn parked in [`CommitState::AwaitingVerdict`]:
    /// - abort: dispatch the drop of its staged state;
    /// - COMMIT for a slice with no write: drop the staged state too, since
    ///   nothing installs;
    /// - COMMIT for a write slice: resolve its staged post-images, at once,
    ///   or at its turn for a whole-collection resolve. The resolved redo is
    ///   proposed from [`Self::finish_redo_resolve`].
    ///
    /// A follower's [`CommitState::Following`] txn completes at an abort or
    /// at a COMMIT for a slice with no write: no log entry follows either.
    /// A COMMIT for a write slice waits for the slice's redo to apply.
    ///
    /// Any other state already left the barrier, so a duplicate push, probe
    /// or sweep is a no-op.
    ///
    /// A COMMIT verdict for a txn this leader failed to stage restages it
    /// (see `super::super::restage`). The txn keeps its locks and its
    /// position stays unapplied meanwhile.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resume_on_verdict(
        &mut self,
        txn_id: TxnId,
        committed: bool,
    ) {
        let Some((state, writes)) = self
            .pending
            .get(&txn_id)
            .map(|p| (p.commit_state, p.scope.writes))
        else {
            return;
        };
        match state {
            CommitState::Following => {
                if !committed || !writes {
                    self.complete_without_entry(txn_id);
                }
            }
            CommitState::AwaitingVerdict => self.resume_parked(txn_id, committed, writes),
            CommitState::Staged
            | CommitState::AwaitingResolveTurn
            | CommitState::AwaitingRedoResolve
            | CommitState::AwaitingRedoApply { .. }
            | CommitState::AwaitingDrop
            | CommitState::AwaitingRestage => {}
        }
    }

    /// Resume a leader's txn parked on the barrier.
    fn resume_parked(&mut self, txn_id: TxnId, committed: bool, writes: bool) {
        // An earlier leader of the vShard voted COMMIT, and this leader's
        // stage failed: the txn cannot drop while its peers apply it.
        if committed
            && self
                .pending
                .get(&txn_id)
                .is_some_and(|pending| pending.stage_error.is_some())
        {
            self.restage_or_halt(txn_id);
            return;
        }
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            // No longer parked: clear the stall deadline.
            pending.verdict_deadline = None;
        }
        let counter = if committed {
            &self.shared.calvin.counters.commits_flushed
        } else {
            &self.shared.calvin.counters.commits_dropped
        };
        counter.fetch_add(1, Ordering::Relaxed);

        if !committed || !writes {
            self.drop_staged_slice(txn_id);
            return;
        }
        // A slice that truncates rows resolves at its turn, once every lower
        // txn of this vShard finished.
        let at_turn = self
            .pending
            .get(&txn_id)
            .is_some_and(|pending| pending.scope.resolve_at_turn);
        if at_turn {
            if let Some(pending) = self.pending.get_mut(&txn_id) {
                pending.commit_state = CommitState::AwaitingResolveTurn;
            }
            self.pump_resolve_turn();
            return;
        }
        self.start_resolve(txn_id);
    }

    /// Dispatch the resolve of a committed slice, and wait for its answer.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn start_resolve(
        &mut self,
        txn_id: TxnId,
    ) {
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.commit_state = CommitState::AwaitingRedoResolve;
        }
        // Sent or parked for re-send at capacity: either way the txn awaits
        // the resolve's answer.
        if let DispatchOutcome::Failed(error) = self.dispatch_calvin_resolve(txn_id) {
            self.fail_dispatch_step(txn_id, DispatchStep::Resolve, error);
        }
    }

    /// Dispatch the drop of a staged slice that ends with no log entry, and
    /// wait for its answer.
    fn drop_staged_slice(&mut self, txn_id: TxnId) {
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.commit_state = CommitState::AwaitingDrop;
        }
        if let DispatchOutcome::Failed(error) = self.dispatch_drop(txn_id, DispatchStep::Drop) {
            // Terminal refusal: the scheduler halts and holds the txn with
            // its locks and staged buffer.
            self.fail_dispatch_step(txn_id, DispatchStep::Drop, error);
        }
    }

    /// Complete `txn_id`, which ends with no log entry on this vShard. It
    /// owes an empty `CompletionAck`: the coordinator's outcome waits for
    /// every participant's.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn complete_without_entry(
        &mut self,
        txn_id: TxnId,
    ) {
        self.propose_sequencer_entry(
            txn_id,
            SchedulerProposal::CompletionAck { result: Vec::new() },
        );
        self.metrics.record_completed();
        self.on_txn_complete(txn_id, LedgerMark::Terminal);
    }

    /// Handle a pushed [`VerdictSignal`] from this node's completion registry.
    ///
    /// Matches the signal to the txn by `(epoch, position)` and resumes it. A
    /// signal for a txn this scheduler does not host, or one that already
    /// resumed, is a harmless no-op.
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
    /// committed, so aborting one side while a peer committed will tear
    /// the transaction. The verdict is guaranteed to arrive eventually — a
    /// post-failover leader re-aggregates the replicated votes (seeded on every
    /// replica) into the same verdict — so waiting is always the safe action.
    ///
    /// The same stall tick re-proposes this vShard's vote while it is owed
    /// (`retry_owed_sequencer_entries`). The warning names whether the vote is
    /// still owed, so a stall that waits on another participant shows apart.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn check_awaiting_verdict_stalls(
        &mut self,
    ) {
        // no-determinism: stall-detection clock drives warnings/metrics only; this path holds locks and never aborts, so it cannot affect the replicated outcome.
        let now = Instant::now();
        let stalled: Vec<TxnId> = self
            .pending
            .iter()
            .filter(|(_, p)| p.commit_state == CommitState::AwaitingVerdict)
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
            let vote_owed = self.owed.contains_key(&(
                txn_id,
                crate::control::cluster::calvin::scheduler::driver::core::owed::OwedKind::Vote,
            ));
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                vote_owed,
                "calvin: staged txn still awaiting the cross-shard verdict past its stall \
                 deadline; HOLDING locks and waiting (never aborting — a peer may have already \
                 committed). The verdict is guaranteed to arrive."
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
    use nodedb_types::TenantId;

    use super::*;
    use crate::bridge::dispatch::CoreChannelDataSide;
    use crate::bridge::envelope::ErrorCode;
    use crate::bridge::envelope::{Payload, StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::lead_data_group;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        await_data_plane_request, build_test_scheduler, build_test_scheduler_with_data_side,
        error_response, fill_tenant_inflight, following_pending, make_sequenced_txn,
        release_filler, spawn_scheduler_loop, staged_pending, staged_response,
    };
    use crate::control::state::SharedState;
    use crate::types::RequestId;
    use crate::wal::RedoRecord;

    /// A scheduler with one txn parked in a commit state while its tenant sits
    /// at the dispatcher's in-flight cap.
    struct ParkedAtCapacity {
        scheduler: Scheduler,
        _dir: tempfile::TempDir,
        data_side: CoreChannelDataSide,
        shared: Arc<SharedState>,
        fillers: Vec<RequestId>,
    }

    /// Park `txn_id` in `state`, then fill its tenant to the in-flight cap.
    fn parked_at_capacity(txn_id: TxnId, state: CommitState) -> ParkedAtCapacity {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, dir, mut data_side) = build_test_scheduler_with_data_side(7, registry);
        let mut pending = staged_pending(make_sequenced_txn(txn_id.epoch, txn_id.position), txn_id);
        pending.commit_state = state;
        scheduler.pending.insert(txn_id, pending);
        let shared = Arc::clone(&scheduler.shared);
        let fillers = fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));
        ParkedAtCapacity {
            scheduler,
            _dir: dir,
            data_side,
            shared,
            fillers,
        }
    }

    /// An Ok resolve response whose redo record carries no ops.
    fn empty_redo_response() -> crate::bridge::envelope::Response {
        let redo = RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
        };
        let mut response = staged_response(Status::Ok, None);
        let resolved = nodedb_physical::physical_plan::CalvinResolved {
            redo: redo.to_bytes().expect("encode empty redo record"),
            reply: nodedb_physical::physical_plan::CalvinReplySpec::Count(Vec::new()),
        };
        response.payload =
            Payload::from_vec(zerompk::to_msgpack_vec(&resolved).expect("encode resolved answer"));
        response
    }

    /// Under a COMMIT verdict, a refused resolve dispatch does not complete
    /// the txn: its position stays unapplied and its pending entry stays.
    #[tokio::test]
    async fn commit_verdict_resolve_refused_at_capacity_does_not_complete_txn() {
        let txn_id = TxnId::new(14, 2);
        let mut parked = parked_at_capacity(txn_id, CommitState::AwaitingVerdict);
        let scheduler = &mut parked.scheduler;

        scheduler.resume_on_verdict(txn_id, true);

        assert!(
            !scheduler.applied.is_applied(14, 2),
            "a refused resolve must not mark the position applied"
        );
        assert!(
            scheduler.pending.contains_key(&txn_id),
            "a refused resolve must keep the txn's pending entry"
        );
    }

    /// A resolved redo is proposed to the data group, and the txn waits for
    /// its apply: its position stays unapplied and its pending entry stays.
    #[tokio::test]
    async fn a_resolved_redo_is_proposed_and_waits_for_its_apply() {
        let txn_id = TxnId::new(14, 2);
        let (mut scheduler, _dir) = build_test_scheduler(7);
        lead_data_group(&mut scheduler);
        let mut pending = staged_pending(make_sequenced_txn(14, 2), txn_id);
        pending.commit_state = CommitState::AwaitingRedoResolve;
        scheduler.pending.insert(txn_id, pending);

        scheduler.finish_redo_resolve(txn_id, empty_redo_response());

        assert!(matches!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingRedoApply { proposed: Some(_) })
        ));
        assert!(
            !scheduler.applied.is_applied(14, 2),
            "a proposed redo must not mark the position applied"
        );
        assert!(!scheduler.is_apply_halted());
    }

    /// Under an ABORT verdict, a refused drop dispatch does not complete the
    /// txn: its position stays unapplied and its pending entry stays.
    #[tokio::test]
    async fn abort_verdict_drop_refused_at_capacity_does_not_complete_txn() {
        let txn_id = TxnId::new(14, 2);
        let mut parked = parked_at_capacity(txn_id, CommitState::AwaitingVerdict);
        let scheduler = &mut parked.scheduler;

        scheduler.resume_on_verdict(txn_id, false);

        assert!(
            !scheduler.applied.is_applied(14, 2),
            "a refused drop must not mark the position applied"
        );
        assert!(
            scheduler.pending.contains_key(&txn_id),
            "a refused drop must keep the txn's pending entry"
        );
    }

    /// Once a Data Plane response frees tenant capacity, a refused resolve
    /// reaches the Data Plane.
    #[tokio::test]
    async fn refused_resolve_reaches_data_plane_after_capacity_frees() {
        let txn_id = TxnId::new(14, 2);
        let ParkedAtCapacity {
            mut scheduler,
            _dir,
            mut data_side,
            shared,
            fillers,
        } = parked_at_capacity(txn_id, CommitState::AwaitingVerdict);

        scheduler.resume_on_verdict(txn_id, true);
        let running = spawn_scheduler_loop(scheduler);
        release_filler(&shared, &mut data_side, fillers[0]);

        let arrived = await_data_plane_request(&mut data_side, |plan| {
            matches!(
                plan,
                PhysicalPlan::Meta(MetaOp::CalvinResolve {
                    epoch: 14,
                    position: 2
                })
            )
        })
        .await;
        running.stop().await;

        assert!(
            arrived,
            "the refused resolve must reach the Data Plane once capacity frees"
        );
    }

    /// Once a Data Plane response frees tenant capacity, a refused drop
    /// reaches the Data Plane.
    #[tokio::test]
    async fn refused_drop_reaches_data_plane_after_capacity_frees() {
        let txn_id = TxnId::new(14, 2);
        let ParkedAtCapacity {
            mut scheduler,
            _dir,
            mut data_side,
            shared,
            fillers,
        } = parked_at_capacity(txn_id, CommitState::AwaitingVerdict);

        scheduler.resume_on_verdict(txn_id, false);
        let running = spawn_scheduler_loop(scheduler);
        release_filler(&shared, &mut data_side, fillers[0]);

        let arrived = await_data_plane_request(&mut data_side, |plan| {
            matches!(
                plan,
                PhysicalPlan::Meta(MetaOp::CalvinDrop {
                    epoch: 14,
                    position: 2,
                })
            )
        })
        .await;
        running.stop().await;

        assert!(
            arrived,
            "the refused drop must reach the Data Plane once capacity frees"
        );
    }

    /// A false vote from either participant makes the only global verdict abort;
    /// applying that durable verdict broadcasts the abort to every parked local
    /// participant. The scheduler's `resume_on_verdict(false)` then dispatches a
    /// drop, never a resolve, on each recipient.
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
        // affirmative nor the failed participant can resolve or drop unilaterally.
        first_scheduler.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );
        second_scheduler.resolve_staged_commit(txn_id, &staged_response(Status::Error, None));
        for (scheduler, data_side) in [
            (&first_scheduler, &mut first_data),
            (&second_scheduler, &mut second_data),
        ] {
            assert!(matches!(
                scheduler
                    .pending
                    .get(&txn_id)
                    .map(|pending| pending.commit_state),
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
            ParticipantVote::Abort(AbortReason::SerializationConflict),
        );
        assert_eq!(
            registry.drain_unproposed_verdicts(),
            vec![(
                txn,
                VerdictOutcome::Abort(AbortReason::SerializationConflict)
            )]
        );
        registry.note_verdict(
            txn,
            VerdictOutcome::Abort(AbortReason::SerializationConflict),
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
                    .map(|pending| pending.commit_state),
                Some(CommitState::AwaitingDrop)
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

    /// A leader scheduler whose stage failed, parked on the verdict barrier.
    fn leader_with_failed_stage(
        txn_id: TxnId,
    ) -> (Scheduler, tempfile::TempDir, CoreChannelDataSide) {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, dir, data_side) = build_test_scheduler_with_data_side(7, registry);
        scheduler.pending.insert(
            txn_id,
            staged_pending(make_sequenced_txn(txn_id.epoch, txn_id.position), txn_id),
        );
        scheduler.resolve_staged_commit(
            txn_id,
            &error_response(ErrorCode::Internal {
                detail: "stage failed".to_string(),
            }),
        );
        (scheduler, dir, data_side)
    }

    /// A COMMIT verdict for a txn this leader failed to stage holds it for a
    /// restage: an earlier leader staged and voted commit, so the txn cannot
    /// drop. The failed stage's state is discarded, no resolve is
    /// dispatched, and the position stays unapplied.
    #[tokio::test]
    async fn commit_verdict_after_local_stage_error_awaits_a_restage() {
        let txn_id = TxnId::new(14, 2);
        let (mut scheduler, _dir, mut data_side) = leader_with_failed_stage(txn_id);

        scheduler.resume_on_verdict(txn_id, true);

        assert!(!scheduler.applied.is_applied(14, 2));
        let pending = scheduler
            .pending
            .get(&txn_id)
            .expect("the txn stays pending");
        assert_eq!(pending.commit_state, CommitState::AwaitingRestage);
        assert_eq!(pending.verdict_deadline, None);
        assert!(!scheduler.is_apply_halted());
        let restage = scheduler
            .restages
            .get(&txn_id)
            .expect("the txn waits for its first restage");
        assert_eq!(restage.attempts, 0);
        assert!(restage.due.is_some());
        let request = data_side
            .request_rx
            .try_pop()
            .expect("the failed stage's state is discarded");
        assert!(matches!(
            request.inner.plan,
            PhysicalPlan::Meta(MetaOp::CalvinDrop {
                epoch: 14,
                position: 2
            })
        ));
        assert!(
            data_side.request_rx.try_pop().is_err(),
            "no resolve reaches the Data Plane"
        );
    }

    /// An abort verdict for a txn this leader failed to stage drops it as
    /// usual: every replica reaches the same abort.
    #[tokio::test]
    async fn abort_verdict_after_local_stage_error_drops_without_halting() {
        let txn_id = TxnId::new(14, 2);
        let (mut scheduler, _dir, mut data_side) = leader_with_failed_stage(txn_id);

        scheduler.resume_on_verdict(txn_id, false);

        assert!(!scheduler.is_apply_halted());
        assert!(scheduler.restages.is_empty(), "an abort never restages");
        let request = data_side
            .request_rx
            .try_pop()
            .expect("the abort dispatches a drop");
        assert!(matches!(
            request.inner.plan,
            PhysicalPlan::Meta(MetaOp::CalvinDrop {
                epoch: 14,
                position: 2
            })
        ));
    }

    /// A follower's txn completes at an abort verdict with no log entry: it
    /// owes its ack and marks its position applied. A COMMIT verdict for a
    /// write slice leaves it waiting for its redo.
    #[tokio::test]
    async fn a_following_txn_completes_at_an_abort_and_waits_at_a_commit() {
        use crate::control::cluster::calvin::scheduler::driver::core::owed::OwedKind;
        let (mut scheduler, _dir) = build_test_scheduler(7);
        let aborted = TxnId::new(14, 2);
        let committed = TxnId::new(15, 0);
        for txn_id in [aborted, committed] {
            scheduler.pending.insert(
                txn_id,
                following_pending(make_sequenced_txn(txn_id.epoch, txn_id.position), txn_id),
            );
        }

        scheduler.resume_on_verdict(aborted, false);
        scheduler.resume_on_verdict(committed, true);

        assert!(!scheduler.pending.contains_key(&aborted));
        assert!(scheduler.applied.is_applied(14, 2));
        assert!(
            scheduler
                .owed
                .contains_key(&(aborted, OwedKind::CompletionAck))
        );
        assert_eq!(
            scheduler.pending.get(&committed).map(|p| p.commit_state),
            Some(CommitState::Following),
            "a committed write slice waits for its redo"
        );
        assert!(!scheduler.applied.is_applied(15, 0));
    }
}
