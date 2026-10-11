// SPDX-License-Identifier: BUSL-1.1

//! What the scheduler does when the data-group apply loop concluded a
//! committed slice's stamped redo. Every replica runs it alike.
//!
//! - `RedoApplied` for a txn this scheduler holds: deposit the reply,
//!   owe the `CompletionAck`, release the locks, mark the gate, complete.
//!   The ack's bytes come from the reply, which every replica renders the
//!   same, so the first ack the sequencer log holds is the true one.
//! - `RedoApplied` for a txn still waiting for its locks: kept until the
//!   locks are granted, then completed the same way.
//! - `RedoApplied` for a txn whose input has not arrived: the log ran ahead
//!   of the sequencer fan-out. Mark the gate and owe the ack. The input is
//!   skipped when it arrives.
//! - `RedoRefused` and `RedoNotApplied` halt the scheduler. The txn holds
//!   its locks.
//!
//! The apply loop marked the ledger before it pushed the event. A halted
//! scheduler still drains its inbox, so the apply loop never waits on it.

use crate::bridge::envelope::Response;
use crate::control::cluster::calvin::scheduler::CalvinApplyEvent;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::state::AckSlice;

use super::super::types::CommitState;
use super::halt::{HaltReason, HaltStep};
use super::owed::SchedulerProposal;
use super::process::LedgerMark;
use super::scheduler::Scheduler;

/// A committed slice's install that is durable on this replica.
#[derive(Debug, Clone)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct AppliedRedo {
    pub reply: Response,
    pub primary_write: bool,
    pub returning: bool,
}

impl Scheduler {
    /// Handle every event waiting in this scheduler's inbox, in
    /// `(epoch, position)` order.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn drain_inbox(&mut self) {
        for ((epoch, position), event) in self.inbox.inbox().take_all() {
            self.handle_calvin_apply_event(TxnId::new(epoch, position), event);
        }
    }

    /// Handle how the apply loop concluded the stamped redo of `txn_id`.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn handle_calvin_apply_event(
        &mut self,
        txn_id: TxnId,
        event: CalvinApplyEvent,
    ) {
        match event {
            CalvinApplyEvent::RedoApplied {
                reply,
                primary_write,
                returning,
            } => self.on_redo_applied(
                txn_id,
                AppliedRedo {
                    reply,
                    primary_write,
                    returning,
                },
            ),
            CalvinApplyEvent::RedoRefused { error } => self.halt_apply(
                txn_id,
                HaltReason::RedoInstallRefused,
                HaltStep::RedoApply,
                format!("the data group refused the committed slice's redo for good: {error}"),
            ),
            CalvinApplyEvent::RedoNotApplied { error } => self.halt_apply(
                txn_id,
                HaltReason::RedoApplyFailed,
                HaltStep::RedoApply,
                format!("the committed slice's redo did not become durable here: {error}"),
            ),
        }
    }

    /// The stamped redo of `txn_id` installed on this replica.
    fn on_redo_applied(&mut self, txn_id: TxnId, applied: AppliedRedo) {
        if let Some(pending) = self.pending.get(&txn_id) {
            let state = pending.commit_state;
            let lock_owner = pending.lock_owner;
            if holds_staged_state(state) {
                // Another leader's copy installed while this node staged the
                // txn again: the staged state is spent.
                self.discard_staged(txn_id);
            }
            self.pending.remove(&txn_id);
            self.complete_applied(txn_id, lock_owner, applied);
            return;
        }
        if let Some(barrier) = self.dependent_barrier.remove(&txn_id) {
            self.complete_applied(txn_id, barrier.lock_owner, applied);
            return;
        }
        if let Some(lock_owner) = self.parts.release_awaiting(txn_id) {
            self.complete_applied(txn_id, lock_owner, applied);
            return;
        }
        let blocked = self
            .blocked
            .values()
            .any(|blocked| TxnId::new(blocked.txn.epoch, blocked.txn.position) == txn_id);
        if blocked {
            self.early_applied.insert(txn_id, applied);
            return;
        }
        // The input has not arrived. Its position is applied: the input is
        // skipped when it does, and takes no lock.
        self.owe_applied_ack(txn_id, &applied);
        if let Some(watermark) = self.applied.mark_applied(txn_id.epoch, txn_id.position) {
            self.publish_watermark(watermark);
        }
    }

    /// Complete `txn_id`, whose redo installed, releasing the locks
    /// `lock_owner` holds.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn complete_applied(
        &mut self,
        txn_id: TxnId,
        lock_owner: TxnId,
        applied: AppliedRedo,
    ) {
        self.owe_applied_ack(txn_id, &applied);
        self.metrics.record_completed();
        self.release_and_mark_applied(txn_id, lock_owner, LedgerMark::ByApply);
    }

    /// Deposit a primary slice's reply, then owe the ack that carries it.
    /// The deposit lands first, so the coordinator finds it when the ack
    /// fires its waiter.
    fn owe_applied_ack(&mut self, txn_id: TxnId, applied: &AppliedRedo) {
        if applied.primary_write {
            self.deposit_reply(txn_id, &applied.reply, applied.returning);
        }
        let result = self.ack_result_of(
            &applied.reply,
            AckSlice {
                primary_write: applied.primary_write,
                returning: applied.returning,
            },
        );
        self.propose_sequencer_entry(txn_id, SchedulerProposal::CompletionAck { result });
    }
}

/// Whether a txn in `state` holds staged state on this node's core.
fn holds_staged_state(state: CommitState) -> bool {
    match state {
        CommitState::Staged
        | CommitState::AwaitingVerdict
        | CommitState::AwaitingResolveTurn
        | CommitState::AwaitingRedoResolve
        | CommitState::AwaitingDrop => true,
        // The install consumed the staged entry of a proposed slice, a
        // follower staged nothing, a txn awaiting its restage discarded what
        // it staged, and a passive read stages nothing.
        CommitState::AwaitingRedoApply { .. }
        | CommitState::Following
        | CommitState::AwaitingRestage
        | CommitState::ReadingPassive => false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nodedb_cluster::calvin::CalvinCompletionRegistry;
    use nodedb_cluster::calvin::types::SchedulerInput;

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::control::cluster::calvin::scheduler::driver::core::halt::HaltReason;
    use crate::control::cluster::calvin::scheduler::driver::core::owed::OwedKind;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_sequenced_txn, redo_applied,
        scheduler_with_pending, spawn_scheduler_loop, staged_response, test_coll_vshard,
    };

    /// The installed slice's reply.
    fn reply() -> Response {
        staged_response(Status::Ok, None)
    }

    /// A follower's held txn whose redo installed completes: it owes its
    /// ack, releases its locks, and marks its position applied. The txn
    /// waiting on its locks runs.
    #[tokio::test]
    async fn redo_applied_releases_locks_and_owes_the_ack() {
        let (mut scheduler, _dir, _data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        );
        let txn_id = TxnId::new(3, 0);
        let next = TxnId::new(4, 0);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(make_sequenced_txn(3, 0))));
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(make_sequenced_txn(4, 0))));
        assert!(scheduler.blocked.contains_key(&next), "the same key waits");

        scheduler.handle_calvin_apply_event(txn_id, redo_applied(reply()));

        assert!(!scheduler.pending.contains_key(&txn_id));
        assert!(scheduler.applied.is_applied(3, 0));
        assert!(
            scheduler
                .owed
                .contains_key(&(txn_id, OwedKind::CompletionAck))
        );
        assert!(!scheduler.blocked.contains_key(&next));
        assert!(
            scheduler.pending.contains_key(&next),
            "the freed locks grant the next txn"
        );
        assert!(!scheduler.is_apply_halted());
    }

    /// The log ran ahead of the sequencer fan-out: the redo installed before
    /// this scheduler saw the txn. Its position is applied and its ack owed,
    /// and the input is skipped when it arrives.
    #[tokio::test]
    async fn redo_applied_before_the_input_skips_the_input() {
        let (mut scheduler, _dir, _data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        );
        let txn_id = TxnId::new(3, 0);

        scheduler.handle_calvin_apply_event(txn_id, redo_applied(reply()));
        assert!(scheduler.applied.is_applied(3, 0));
        assert!(
            scheduler
                .owed
                .contains_key(&(txn_id, OwedKind::CompletionAck))
        );

        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(make_sequenced_txn(3, 0))));
        assert!(
            !scheduler.pending.contains_key(&txn_id),
            "the input is skipped"
        );
        assert!(!scheduler.blocked.contains_key(&txn_id));
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(make_sequenced_txn(4, 0))));
        assert!(
            scheduler.pending.contains_key(&TxnId::new(4, 0)),
            "the skipped input took no lock"
        );
    }

    /// A redo every replica refused for good halts the scheduler. The txn
    /// keeps its locks and its position stays unapplied.
    #[tokio::test]
    async fn redo_refused_halts() {
        let txn_id = TxnId::new(5, 1);
        let (mut scheduler, _dir) =
            scheduler_with_pending(txn_id, CommitState::AwaitingRedoApply { proposed: None });

        scheduler.handle_calvin_apply_event(
            txn_id,
            CalvinApplyEvent::RedoRefused {
                error: "constraint".to_string(),
            },
        );

        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::RedoInstallRefused)
        );
        assert!(scheduler.pending.contains_key(&txn_id));
        assert!(!scheduler.applied.is_applied(5, 1));
    }

    /// A halted scheduler still drains its inbox, so the apply loop never
    /// waits on it.
    #[tokio::test]
    async fn a_halted_scheduler_still_drains_the_inbox() {
        let txn_id = TxnId::new(5, 1);
        let (mut scheduler, _dir) =
            scheduler_with_pending(txn_id, CommitState::AwaitingRedoApply { proposed: None });
        scheduler.handle_calvin_apply_event(
            txn_id,
            CalvinApplyEvent::RedoNotApplied {
                error: "fsync".to_string(),
            },
        );
        assert!(scheduler.is_apply_halted());
        let inbox = std::sync::Arc::clone(scheduler.inbox.inbox());

        let running = spawn_scheduler_loop(scheduler);
        inbox
            .push(
                6,
                0,
                CalvinApplyEvent::RedoNotApplied {
                    error: "fsync".to_string(),
                },
            )
            .await;
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while inbox.waiting() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        running.stop().await;

        assert!(drained.is_ok(), "the halted scheduler drains its inbox");
    }
}
