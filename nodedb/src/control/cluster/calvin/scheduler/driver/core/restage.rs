// SPDX-License-Identifier: BUSL-1.1

//! Restage of a committed txn whose stage failed on this leader.
//!
//! A COMMIT verdict reaches a leader whose own stage failed only when an
//! earlier leader of the vShard voted COMMIT first. The txn cannot drop:
//! its peers apply it. So the leader discards what the failed stage left,
//! holds the txn with its locks, and stages it again once a backoff passes.
//!
//! - A restage takes the path a promotion takes,
//!   [`Scheduler::route_granted`]: a static stage, a dependent barrier, or a
//!   plan rejection. A rejected plan or a timed-out barrier parks the txn
//!   unstaged again. That counts as one more failed stage.
//! - The verdict is known, so the restage's vote changes nothing, and its
//!   park probe resumes the txn at once. A restage that stages resolves and
//!   proposes its redo like any committed slice.
//! - Due restages run in sequencer order. A txn waits for every lower txn
//!   that also waits for its restage.
//! - The leader halts with `LocalStageFailed` once the restages run out. A
//!   txn whose collection is superseded cannot commit here, and halts at
//!   once.
//!
//! The backoff runs on this node's clock. It only delays a retry of a
//! decided verdict, so no replica applies anything different.

use std::sync::atomic::Ordering;
use std::time::Instant;

use tracing::{info, warn};

use super::super::types::{CommitState, PendingTxn};
use super::halt::{HaltReason, HaltStep};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

/// How far a committed txn is through its restages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct Restage {
    /// Restages dispatched so far.
    pub attempts: u32,
    /// When the next restage dispatches. `None` while one is in flight.
    pub due: Option<Instant>,
}

impl Scheduler {
    /// Act on a COMMIT verdict for `txn_id`, whose stage failed on this
    /// leader: hold it for a restage, or halt once it cannot commit here.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn restage_or_halt(
        &mut self,
        txn_id: TxnId,
    ) {
        let Some((superseded, stage_error)) = self.pending.get_mut(&txn_id).map(|pending| {
            pending.verdict_deadline = None;
            (
                pending.superseded,
                pending.stage_error.clone().unwrap_or_default(),
            )
        }) else {
            return;
        };
        if superseded {
            self.halt_apply(
                txn_id,
                HaltReason::LocalStageFailed,
                HaltStep::Stage,
                format!(
                    "COMMIT verdict for a txn whose collection this leader found superseded: \
                     {stage_error}"
                ),
            );
            return;
        }
        let done = self
            .restages
            .get(&txn_id)
            .map_or(0, |restage| restage.attempts);
        if done >= self.config.restage_attempts {
            self.restages.remove(&txn_id);
            self.halt_apply(
                txn_id,
                HaltReason::LocalStageFailed,
                HaltStep::Stage,
                format!(
                    "COMMIT verdict for a txn this leader failed to stage in {done} restages: \
                     {stage_error}"
                ),
            );
            return;
        }
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.commit_state = CommitState::AwaitingRestage;
            pending.awaiting = None;
            pending.stage_error = None;
        }
        let backoff = self.config.restage_backoff(done);
        // no-determinism: the restage backoff only delays a retry of a decided COMMIT verdict; it never changes what a replica applies.
        let due = Instant::now() + backoff;
        self.restages.insert(
            txn_id,
            Restage {
                attempts: done,
                due: Some(due),
            },
        );
        warn!(
            vshard_id = self.vshard_id,
            epoch = txn_id.epoch,
            position = txn_id.position,
            restages_done = done,
            backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
            error = %stage_error,
            "calvin: COMMIT verdict for a txn this leader failed to stage; staging it again \
             after the backoff"
        );
        // Clear what the failed stage left on the core. The restage reaches
        // the same core after it, in dispatch order.
        self.discard_staged(txn_id);
    }

    /// Stage again, in sequencer order, every txn whose restage backoff
    /// passed. Runs on the leader only, and not once the scheduler halted.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn restage_due(&mut self) {
        if self.restages.is_empty() || !self.role.is_leader() || self.is_apply_halted() {
            return;
        }
        // no-determinism: the restage backoff only delays a retry of a decided COMMIT verdict.
        let now = Instant::now();
        let mut due = Vec::new();
        for (txn_id, restage) in &self.restages {
            let Some(at) = restage.due else {
                continue;
            };
            // A lower txn's restage goes first. A discard that waits for
            // capacity holds its restage back, so the stage never overtakes it.
            if at > now || self.has_deferred_for(*txn_id) {
                break;
            }
            due.push(*txn_id);
        }
        for txn_id in due {
            self.restage(txn_id);
            if self.is_apply_halted() || !self.role.is_leader() {
                return;
            }
        }
    }

    /// When the run loop next wakes for a restage: the backoff end of the
    /// lowest txn that waits for its restage. `None` when no restage can
    /// run, or when one waits for capacity, which wakes the loop itself.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn next_restage_at(
        &self,
    ) -> Option<Instant> {
        if !self.role.is_leader() || self.is_apply_halted() {
            return None;
        }
        let (txn_id, at) = self
            .restages
            .iter()
            .find_map(|(txn_id, restage)| restage.due.map(|at| (*txn_id, at)))?;
        (!self.has_deferred_for(txn_id)).then_some(at)
    }

    /// Forget the restage and the barrier log of `txn_id`, which finished.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn forget_held_state(
        &mut self,
        txn_id: TxnId,
    ) {
        self.restages.remove(&txn_id);
        self.barrier_logs.remove(&txn_id);
    }

    /// Stage `txn_id` again through the grant path.
    fn restage(&mut self, txn_id: TxnId) {
        let awaits = self
            .pending
            .get(&txn_id)
            .is_some_and(|pending| pending.commit_state == CommitState::AwaitingRestage);
        let Some(restage) = self.restages.get_mut(&txn_id) else {
            return;
        };
        if !awaits {
            restage.due = None;
            return;
        }
        restage.attempts = restage.attempts.saturating_add(1);
        restage.due = None;
        let attempt = restage.attempts;
        let Some(pending) = self.pending.remove(&txn_id) else {
            return;
        };
        self.metrics.record_restage();
        self.shared
            .calvin
            .counters
            .stage_restages
            .fetch_add(1, Ordering::Relaxed);
        info!(
            vshard_id = self.vshard_id,
            epoch = txn_id.epoch,
            position = txn_id.position,
            attempt,
            "calvin: staging a committed txn again"
        );
        let PendingTxn {
            txn,
            lock_owner,
            gates,
            ..
        } = pending;
        // The old gates stay held until the restage holds its own, so a
        // purge of a named collection never runs between the two.
        self.route_granted(txn, txn_id, lock_owner);
        drop(gates);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use nodedb_cluster::calvin::types::SchedulerInput;
    use nodedb_cluster::calvin::{CalvinCompletionRegistry, VerdictOutcome};
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_physical::physical_plan::meta::MetaOp;

    use super::*;
    use crate::bridge::dispatch::CoreChannelDataSide;
    use crate::bridge::envelope::{ErrorCode, StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        lead_data_group, step_down_from_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, error_response, make_local_write_txn, staged_response,
        test_coll_vshard,
    };

    /// A data-group leader on the `test_coll` vShard, sharing `registry`.
    fn leader(
        registry: &Arc<CalvinCompletionRegistry>,
    ) -> (Scheduler, tempfile::TempDir, CoreChannelDataSide) {
        let (mut scheduler, dir, data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), Arc::clone(registry));
        lead_data_group(&mut scheduler);
        (scheduler, dir, data_side)
    }

    /// The Calvin plans on the Data Plane side, in arrival order. Drains
    /// the request ring.
    fn requests(data_side: &mut CoreChannelDataSide) -> Vec<PhysicalPlan> {
        let mut plans = Vec::new();
        while let Ok(request) = data_side.request_rx.try_pop() {
            plans.push(request.inner.plan);
        }
        plans
    }

    fn is_stage(plan: &PhysicalPlan, txn_id: TxnId) -> bool {
        matches!(
            plan,
            PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic { epoch, position, .. })
                if *epoch == txn_id.epoch && *position == txn_id.position
        )
    }

    fn is_drop(plan: &PhysicalPlan, txn_id: TxnId) -> bool {
        *plan
            == PhysicalPlan::Meta(MetaOp::CalvinDrop {
                epoch: txn_id.epoch,
                position: txn_id.position,
            })
    }

    fn stage_failed() -> crate::bridge::envelope::Response {
        error_response(ErrorCode::Internal {
            detail: "stage failed on this core".to_string(),
        })
    }

    /// Store a COMMIT verdict for `txn_id`, as the sequencer log applies it.
    fn commit_verdict(registry: &CalvinCompletionRegistry, txn_id: TxnId) {
        registry.note_verdict(
            nodedb_cluster::calvin::TxnId::new(txn_id.epoch, txn_id.position),
            VerdictOutcome::Commit,
        );
    }

    /// End the backoff of `txn_id`'s next restage now.
    fn end_backoff(scheduler: &mut Scheduler, txn_id: TxnId) {
        let restage = scheduler
            .restages
            .get_mut(&txn_id)
            .expect("the txn waits for a restage");
        // no-determinism: test-only backoff end in the past.
        restage.due = Some(Instant::now() - Duration::from_millis(1));
    }

    fn restages(scheduler: &Scheduler) -> u64 {
        scheduler
            .shared
            .calvin
            .counters
            .stage_restages
            .load(Ordering::Relaxed)
    }

    /// A leader whose stage of a committed write slice failed: it stages the
    /// slice again after the backoff, fails once more, then stages it and
    /// commits. The resolve follows the normal commit path.
    #[tokio::test]
    async fn a_restage_that_succeeds_on_the_second_attempt_commits() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) = leader(&registry);
        scheduler.config.restage_backoff_ms = 60_000;
        let txn_id = TxnId::new(20, 0);
        scheduler
            .process_scheduler_input(SchedulerInput::Txn(Box::new(make_local_write_txn(20, 0))));
        assert!(requests(&mut data_side).iter().any(|p| is_stage(p, txn_id)));

        scheduler.resolve_staged_commit(txn_id, &stage_failed());
        commit_verdict(&registry, txn_id);
        scheduler.resume_on_verdict(txn_id, true);

        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingRestage)
        );
        assert!(
            requests(&mut data_side).iter().any(|p| is_drop(p, txn_id)),
            "the failed stage's state is discarded"
        );
        assert!(!scheduler.is_apply_halted());

        scheduler.restage_due();
        assert!(
            requests(&mut data_side).is_empty(),
            "nothing stages before the backoff ends"
        );
        assert!(scheduler.next_restage_at().is_some());

        end_backoff(&mut scheduler, txn_id);
        scheduler.restage_due();
        assert!(requests(&mut data_side).iter().any(|p| is_stage(p, txn_id)));
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Staged)
        );
        assert_eq!(restages(&scheduler), 1);

        // The second stage fails too. The known verdict holds it for the
        // next restage at once.
        scheduler.resolve_staged_commit(txn_id, &stage_failed());
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingRestage)
        );
        assert_eq!(scheduler.restages.get(&txn_id).map(|r| r.attempts), Some(1));
        requests(&mut data_side);

        end_backoff(&mut scheduler, txn_id);
        scheduler.restage_due();
        assert!(requests(&mut data_side).iter().any(|p| is_stage(p, txn_id)));
        assert_eq!(restages(&scheduler), 2);

        scheduler.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingRedoResolve)
        );
        assert!(
            requests(&mut data_side).iter().any(|p| matches!(
                p,
                PhysicalPlan::Meta(MetaOp::CalvinResolve {
                    epoch: 20,
                    position: 0
                })
            )),
            "the committed slice resolves its redo"
        );
        assert!(!scheduler.is_apply_halted());
        assert!(!scheduler.applied.is_applied(20, 0));
    }

    /// Once its restages run out, a leader that still cannot stage a
    /// committed txn halts. The txn stays parked with its locks, unapplied.
    #[tokio::test]
    async fn a_spent_restage_bound_halts_unapplied() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) = leader(&registry);
        scheduler.config.restage_attempts = 1;
        let txn_id = TxnId::new(21, 0);
        scheduler
            .process_scheduler_input(SchedulerInput::Txn(Box::new(make_local_write_txn(21, 0))));
        commit_verdict(&registry, txn_id);
        scheduler.resolve_staged_commit(txn_id, &stage_failed());
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingRestage)
        );

        end_backoff(&mut scheduler, txn_id);
        scheduler.restage_due();
        requests(&mut data_side);
        scheduler.resolve_staged_commit(txn_id, &stage_failed());

        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::LocalStageFailed)
        );
        let pending = scheduler
            .pending
            .get(&txn_id)
            .expect("the txn stays pending");
        assert_eq!(pending.commit_state, CommitState::AwaitingVerdict);
        assert_eq!(pending.verdict_deadline, None);
        assert!(scheduler.restages.is_empty());
        assert_eq!(restages(&scheduler), 1);
        assert!(!scheduler.applied.is_applied(21, 0));
        assert!(
            !requests(&mut data_side)
                .iter()
                .any(|p| matches!(p, PhysicalPlan::Meta(MetaOp::CalvinResolve { .. }))),
            "no resolve reaches the Data Plane"
        );
    }

    /// A committed txn whose collection this leader found superseded cannot
    /// commit here. It halts at once, with no restage.
    #[tokio::test]
    async fn a_superseded_collection_halts_at_once() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) = leader(&registry);
        let txn_id = TxnId::new(22, 0);
        scheduler
            .process_scheduler_input(SchedulerInput::Txn(Box::new(make_local_write_txn(22, 0))));
        requests(&mut data_side);
        if let Some(pending) = scheduler.pending.get_mut(&txn_id) {
            pending.superseded = true;
        }
        commit_verdict(&registry, txn_id);

        scheduler.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );

        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::LocalStageFailed)
        );
        assert!(scheduler.restages.is_empty());
        assert_eq!(restages(&scheduler), 0);
        assert!(
            requests(&mut data_side).is_empty(),
            "nothing is discarded or staged"
        );
    }

    /// An abort verdict for a txn this leader failed to stage drops it. No
    /// restage follows.
    #[tokio::test]
    async fn an_abort_verdict_drops_without_restaging() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) = leader(&registry);
        let txn_id = TxnId::new(23, 0);
        scheduler
            .process_scheduler_input(SchedulerInput::Txn(Box::new(make_local_write_txn(23, 0))));
        requests(&mut data_side);
        scheduler.resolve_staged_commit(txn_id, &stage_failed());

        scheduler.resume_on_verdict(txn_id, false);

        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingDrop)
        );
        assert!(requests(&mut data_side).iter().any(|p| is_drop(p, txn_id)));
        assert!(scheduler.restages.is_empty());
        assert_eq!(scheduler.next_restage_at(), None);
        assert_eq!(restages(&scheduler), 0);
        assert!(!scheduler.is_apply_halted());
    }

    /// A txn whose plans this leader rejected restages through the grant
    /// path. The plans reject again, which spends the bound and halts.
    #[tokio::test]
    async fn a_rejected_plan_restages_through_the_grant_path() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) = leader(&registry);
        scheduler.config.restage_attempts = 1;
        let txn_id = TxnId::new(24, 0);
        let mut txn = make_local_write_txn(24, 0);
        txn.tx_class.plans = vec![0xc1];
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(txn)));
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        );

        commit_verdict(&registry, txn_id);
        scheduler.resume_on_verdict(txn_id, true);
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingRestage)
        );

        end_backoff(&mut scheduler, txn_id);
        scheduler.restage_due();

        assert_eq!(restages(&scheduler), 1);
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::LocalStageFailed)
        );
        assert!(
            scheduler.pending.contains_key(&txn_id),
            "the rejected restage parks the txn again"
        );
    }

    /// A demoted leader holds a txn awaiting its restage `Following`, and
    /// forgets the restage.
    #[tokio::test]
    async fn demotion_forgets_a_waiting_restage() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) = leader(&registry);
        let txn_id = TxnId::new(25, 0);
        scheduler
            .process_scheduler_input(SchedulerInput::Txn(Box::new(make_local_write_txn(25, 0))));
        commit_verdict(&registry, txn_id);
        scheduler.resolve_staged_commit(txn_id, &stage_failed());
        assert!(scheduler.restages.contains_key(&txn_id));

        step_down_from_data_group(&scheduler);
        scheduler.refresh_role();

        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Following)
        );
        assert!(scheduler.restages.is_empty());
        assert_eq!(scheduler.next_restage_at(), None);
    }
}
