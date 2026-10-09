// SPDX-License-Identifier: BUSL-1.1

//! This scheduler's role in its vShard's data group.
//!
//! Only the data-group leader stages, votes, resolves, and proposes a
//! committed slice's redo. Every other replica holds each granted txn as
//! [`CommitState::Following`] and installs its slice from the log.
//!
//! A node that wins a term leads only once the term's election no-op
//! applied here: the stage gate. Every entry of an earlier term in its log
//! sits below the no-op and commits with it. So a redo an earlier leader
//! proposed installs before this leader stages anything, and this leader
//! finds that position applied.
//!
//! - Promotion stages every `Following` txn as a new grant, in sequencer
//!   order. A txn whose verdict is already COMMIT proposes its vote again,
//!   which changes nothing: the first vote of the vShard counts.
//! - Demotion discards the staged state of every txn the leader drove,
//!   drops its owed votes, and holds the txn `Following`. A redo this node
//!   proposed can still commit in the new term: the applied ledger installs
//!   one copy per position.
//!
//! [`CommitState::Following`]: super::super::types::CommitState::Following

use super::super::types::CommitState;
use super::deferred::{DispatchOutcome, DispatchStep};
use super::owed::OwedKind;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

/// What this node does for its vShard's Calvin txns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum Role {
    /// Holds granted txns and installs committed slices from the log.
    Follower,
    /// Stages, votes, resolves, and proposes.
    Leader,
}

/// The stage gate: the role, and the leadership term it opened in.
#[derive(Debug, Clone, Copy)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct StageGate {
    role: Role,
    /// The data-group term this node leads, as last seen.
    term: Option<u64>,
    /// The index of the term's election no-op. The gate opens once this
    /// node applied it.
    term_start: Option<u64>,
}

impl Default for StageGate {
    fn default() -> Self {
        Self {
            role: Role::Follower,
            term: None,
            term_start: None,
        }
    }
}

impl StageGate {
    /// Whether this node stages the vShard's txns.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// Whether this node leads the data group and waits for its term's
    /// no-op to apply.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn awaits_term_start(
        &self,
    ) -> bool {
        self.role == Role::Follower && self.term.is_some()
    }
}

/// What the data group says about this node now.
struct Leadership {
    group_id: u64,
    term: u64,
    term_start: Option<u64>,
}

impl Scheduler {
    /// Read this node's place in the vShard's data group, and promote or
    /// demote on a change.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn refresh_role(&mut self) {
        let Some(leadership) = self.leadership() else {
            self.role.term = None;
            self.role.term_start = None;
            if self.role.is_leader() {
                self.demote("this node no longer leads the vShard's data group");
            }
            return;
        };
        if self.role.term != Some(leadership.term) {
            if self.role.is_leader() {
                self.demote("this node leads a later term of the vShard's data group");
            }
            self.role.term = Some(leadership.term);
            self.role.term_start = leadership.term_start;
        }
        if self.role.is_leader() {
            return;
        }
        let applied = self
            .shared
            .applied_index_watcher(leadership.group_id)
            .current();
        if self.role.term_start.is_some_and(|start| applied >= start) {
            self.promote();
        }
    }

    /// This node's term and term start in the vShard's data group, while it
    /// leads the group.
    fn leadership(&self) -> Option<Leadership> {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let group_id = mr
            .routing()
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_for_vshard(self.vshard_id)
            .ok()?;
        let term = mr.leader_term(group_id)?;
        Some(Leadership {
            group_id,
            term,
            term_start: mr.term_start_index(group_id),
        })
    }

    /// Whether this node leads the vShard's data group in Raft right now,
    /// whatever its stage gate says. Owed sequencer entries are proposed
    /// only then.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn is_group_leader(
        &self,
    ) -> bool {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.vshard_role_is_leader(self.vshard_id)
    }

    /// Open the stage gate: stage every `Following` txn, in sequencer order.
    fn promote(&mut self) {
        self.role.role = Role::Leader;
        tracing::info!(
            vshard_id = self.vshard_id,
            term = self.role.term,
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
        self.role.role = Role::Follower;
        tracing::info!(
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
        // A barrier waits only on the leader. Its txn holds its locks and
        // follows the log like any other.
        let barriers: Vec<TxnId> = self.dependent_barrier.keys().copied().collect();
        for txn_id in barriers {
            if let Some(barrier) = self.dependent_barrier.remove(&txn_id) {
                self.follow(barrier.txn, txn_id, barrier.lock_owner);
            }
        }
    }

    /// Hold `txn_id` `Following`, and discard what this node staged for it.
    fn release_to_following(&mut self, txn_id: TxnId) {
        let Some(pending) = self.pending.get_mut(&txn_id) else {
            return;
        };
        pending.commit_state = CommitState::Following;
        pending.awaiting = None;
        pending.verdict_deadline = None;
        pending.stage_error = None;
        pending.superseded = false;
        pending.redo = None;
        pending.gates.clear();
        pending.ungated = false;
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

    /// Note that a redo proposal found this node no longer leads.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn on_not_leader(&mut self) {
        if self.role.is_leader() {
            self.demote("a redo proposal found this node no longer leads");
        }
        self.role.term = None;
        self.role.term_start = None;
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
        | CommitState::AwaitingRedoApply { .. } => true,
        // A drop ends the txn with no log entry whatever the role.
        CommitState::AwaitingDrop | CommitState::Following => false,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::CalvinCompletionRegistry;
    use nodedb_cluster::calvin::types::{
        EngineKeySet, ReadWriteSet, SchedulerInput, SequencedTxn, SortedVec,
    };
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_physical::physical_plan::meta::MetaOp;

    use super::*;
    use crate::bridge::dispatch::CoreChannelDataSide;
    use crate::bridge::envelope::{StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        elect_data_group_leader, lead_data_group, step_down_from_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_validate_only_txn, staged_response,
        test_coll_vshard,
    };

    /// A scheduler on the `test_coll` vShard with its Data Plane side.
    fn scheduler() -> (Scheduler, tempfile::TempDir, CoreChannelDataSide) {
        build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        )
    }

    /// A validate-only txn at `(epoch, 0)` that locks `test_coll` row
    /// `surrogate`, so txns on distinct rows hold their locks together.
    fn validate_only_on(epoch: u64, surrogate: u32) -> SequencedTxn {
        let mut txn = make_validate_only_txn(epoch, 0);
        txn.tx_class.write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: "test_coll".to_string(),
            surrogates: SortedVec::new(vec![surrogate]),
        }]);
        txn
    }

    /// The `(epoch, position)` of every Calvin stage request on the Data
    /// Plane side, in arrival order. Drains the request ring.
    fn staged_requests(data_side: &mut CoreChannelDataSide) -> Vec<(u64, u32)> {
        let mut staged = Vec::new();
        while let Ok(request) = data_side.request_rx.try_pop() {
            if let PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
                epoch, position, ..
            }) = request.inner.plan
            {
                staged.push((epoch, position));
            }
        }
        staged
    }

    /// Whether a `CalvinDrop` of `txn_id` reached the Data Plane side.
    /// Drains the request ring.
    fn dropped(data_side: &mut CoreChannelDataSide, txn_id: TxnId) -> bool {
        let mut dropped = false;
        while let Ok(request) = data_side.request_rx.try_pop() {
            dropped |= request.inner.plan
                == PhysicalPlan::Meta(MetaOp::CalvinDrop {
                    epoch: txn_id.epoch,
                    position: txn_id.position,
                });
        }
        dropped
    }

    /// A replica that does not lead the data group holds a granted txn
    /// `Following`. It stages nothing and owes no vote.
    #[tokio::test]
    async fn a_follower_grant_dispatches_nothing() {
        let (mut scheduler, _dir, mut data_side) = scheduler();
        let txn_id = TxnId::new(3, 0);

        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(validate_only_on(3, 1))));

        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Following)
        );
        assert!(data_side.request_rx.try_pop().is_err(), "nothing staged");
        assert!(scheduler.owed.is_empty(), "a follower owes no vote");
        assert!(!scheduler.applied.is_applied(3, 0));
    }

    /// A node that won a term stages nothing until the term's no-op applied
    /// here. Once it applied, the held txn stages.
    #[tokio::test]
    async fn a_new_leader_stages_nothing_before_the_commit_index_applies() {
        let (mut scheduler, _dir, mut data_side) = scheduler();
        let group_id = elect_data_group_leader(&scheduler);
        scheduler.refresh_role();
        assert!(!scheduler.role.is_leader(), "the gate stays closed");
        assert!(scheduler.role.awaits_term_start());
        let txn_id = TxnId::new(3, 0);

        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(validate_only_on(3, 1))));
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Following)
        );
        assert!(staged_requests(&mut data_side).is_empty());

        let term_start = scheduler
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .term_start_index(group_id)
            .expect("the elected node names its term start");
        scheduler
            .shared
            .applied_index_watcher(group_id)
            .bump(term_start);
        scheduler.refresh_role();

        assert!(scheduler.role.is_leader(), "the gate opens");
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Staged)
        );
        assert_eq!(staged_requests(&mut data_side), vec![(3, 0)]);
    }

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
