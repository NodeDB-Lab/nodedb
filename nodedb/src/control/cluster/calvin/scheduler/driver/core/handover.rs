// SPDX-License-Identifier: BUSL-1.1

//! Promotion and demotion of this scheduler in its vShard's data group.
//!
//! - Promotion stages every `Following` txn as a new grant, in sequencer
//!   order. A txn whose verdict is already COMMIT proposes its vote again,
//!   which changes nothing: the first vote of the vShard counts.
//! - Demotion discards the staged state of every txn the leader drove,
//!   drops its owed votes and its restages, and holds the txn `Following`.
//!   A redo this node proposed can still commit in the new term: the
//!   applied ledger installs one copy per position.

use super::super::types::CommitState;
use super::deferred::{DispatchOutcome, DispatchStep};
use super::owed::OwedKind;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Open the stage gate: stage every `Following` txn, in sequencer order.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn promote(&mut self) {
        self.role.open();
        tracing::info!(
            node_id = self.shared.node_id,
            vshard_id = self.vshard_id,
            term = self.role.term(),
            "calvin scheduler: this node leads the vShard; staging its held txns"
        );
        let following: Vec<TxnId> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.commit_state == CommitState::Following)
            .map(|(txn_id, _)| *txn_id)
            .collect();
        for txn_id in following {
            let Some(pending) = self.pending.remove(&txn_id) else {
                continue;
            };
            self.route_granted(pending.txn, txn_id, pending.lock_owner);
        }
    }

    /// Close the stage gate: discard the staged state of every txn this
    /// node drove, drop its owed votes, and hold each txn `Following`.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn demote(
        &mut self,
        why: &str,
    ) {
        self.role.close();
        tracing::info!(
            node_id = self.shared.node_id,
            vshard_id = self.vshard_id,
            reason = why,
            "calvin scheduler: this node stops leading the vShard; holding its txns"
        );
        let driven: Vec<TxnId> = self
            .pending
            .iter()
            .filter(|(_, pending)| leader_drives(pending.commit_state))
            .map(|(txn_id, _)| *txn_id)
            .collect();
        for txn_id in driven {
            self.release_to_following(txn_id);
        }
        // Only a leader restages. A later term counts its own restages.
        self.restages.clear();
        // A barrier waits only on the leader. Its txn holds its locks and
        // follows the log like any other. Its barrier log stays, so a later
        // barrier of it decides from the same entries.
        let barriers: Vec<TxnId> = self.dependent_barrier.keys().copied().collect();
        for txn_id in barriers {
            if let Some(barrier) = self.dependent_barrier.remove(&txn_id) {
                self.follow(barrier.txn, txn_id, barrier.lock_owner);
            }
        }
    }

    /// Hold `txn_id` `Following`, and discard what this node staged for it.
    ///
    /// The txn's scope becomes its slice's own, from its plans. A leader's
    /// step can hold a narrower one: a passive read or a parked abort vote
    /// holds a scope that writes nothing. A follower that kept it would end a
    /// COMMIT with no entry, mark the position applied, and then skip the
    /// slice's redo when it applies.
    fn release_to_following(&mut self, txn_id: TxnId) {
        let Some(scope) = self
            .pending
            .get(&txn_id)
            .map(|pending| self.local_slice_scope(&pending.txn, txn_id))
        else {
            return;
        };
        let Some(pending) = self.pending.get_mut(&txn_id) else {
            return;
        };
        pending.commit_state = CommitState::Following;
        pending.scope = scope;
        pending.awaiting = None;
        pending.verdict_deadline = None;
        pending.stage_error = None;
        pending.superseded = false;
        pending.redo = None;
        pending.gates.clear();
        pending.ungated = false;
        // The images, the evaluation in flight and the frozen replies are
        // this term's. A finished evaluation finds no seq to match, and a
        // later stage evaluates or reads the stored reply again.
        pending.returning.reset_stage();
        self.owed.remove(&(txn_id, OwedKind::Vote));
        self.discard_staged(txn_id);
    }

    /// Dispatch a `CalvinDrop` that clears what this node staged for
    /// `txn_id`. An absent staged entry makes it a no-op on the core.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn discard_staged(
        &mut self,
        txn_id: TxnId,
    ) {
        if let DispatchOutcome::Failed(error) = self.dispatch_drop(txn_id, DispatchStep::Discard) {
            self.fail_dispatch_step(txn_id, DispatchStep::Discard, error);
        }
    }
}

/// Whether a txn in `state` is one the leader drives: it holds staged state
/// or waits on a step only the leader takes.
fn leader_drives(state: CommitState) -> bool {
    match state {
        CommitState::Staged
        | CommitState::AwaitingVerdict
        | CommitState::AwaitingResolveTurn
        | CommitState::AwaitingRedoResolve
        | CommitState::AwaitingRedoApply { .. }
        | CommitState::AwaitingRestage
        | CommitState::ReadingPassive => true,
        // A drop ends the txn with no log entry whatever the role.
        CommitState::AwaitingDrop | CommitState::Following => false,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::SchedulerInput;

    use super::super::role_fixtures::{dropped, scheduler, staged_requests, validate_only_on};
    use super::*;
    use crate::bridge::envelope::{StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        lead_data_group, step_down_from_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::staged_response;

    /// Promotion stages every `Following` txn, in sequencer order.
    #[tokio::test]
    async fn promotion_stages_following_txns_in_sequencer_order() {
        let (mut scheduler, _dir, mut data_side) = scheduler();
        for (epoch, surrogate) in [(3, 1), (4, 2), (5, 3)] {
            scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(validate_only_on(
                epoch, surrogate,
            ))));
        }
        assert!(staged_requests(&mut data_side).is_empty());

        lead_data_group(&mut scheduler);

        assert_eq!(
            staged_requests(&mut data_side),
            vec![(3, 0), (4, 0), (5, 0)]
        );
        for epoch in [3, 4, 5] {
            assert_eq!(
                scheduler
                    .pending
                    .get(&TxnId::new(epoch, 0))
                    .map(|p| p.commit_state),
                Some(CommitState::Staged)
            );
        }
    }

    /// Demotion discards what the leader staged, drops its owed vote, and
    /// holds the txn `Following` with its locks.
    #[tokio::test]
    async fn demotion_drops_staged_state_and_owed_votes() {
        let (mut scheduler, _dir, mut data_side) = scheduler();
        lead_data_group(&mut scheduler);
        let txn_id = TxnId::new(3, 0);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(validate_only_on(3, 1))));
        assert_eq!(staged_requests(&mut data_side), vec![(3, 0)]);
        scheduler.resolve_staged_commit(
            txn_id,
            &staged_response(Status::Ok, Some(StageVote::Commit)),
        );
        assert!(scheduler.owed.contains_key(&(txn_id, OwedKind::Vote)));

        step_down_from_data_group(&scheduler);
        scheduler.refresh_role();

        assert!(!scheduler.role.is_leader());
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Following)
        );
        assert!(
            !scheduler.owed.contains_key(&(txn_id, OwedKind::Vote)),
            "a demoted node keeps no vote owed"
        );
        assert!(
            dropped(&mut data_side, txn_id),
            "the staged state is dropped"
        );
        assert!(!scheduler.applied.is_applied(3, 0));
        let blocked = validate_only_on(4, 1);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(blocked)));
        assert!(
            scheduler.blocked.contains_key(&TxnId::new(4, 0)),
            "the held txn keeps its locks"
        );
    }
}
