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
//! A scheduler that starts while this node already leads also waits for its
//! start fence. A write the gate admitted earlier holds no key on this
//! scheduler's lock table: none existed, or its keys were on a retired one.
//! The gate stays closed until each such write applied (see
//! `write_admission::tickets`).
//!
//! The gate opens with a promotion and closes with a demotion (see
//! [`super::handover`]).
//!
//! [`CommitState::Following`]: super::super::types::CommitState::Following

use super::scheduler::Scheduler;
use crate::control::server::shared::write_admission::{FenceState, StartFence};

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
    /// The writes the gate admitted for this vShard before this scheduler's
    /// lock table was registered. The gate opens once they all applied.
    /// `None` once they did.
    start_fence: Option<StartFence>,
}

impl Default for StageGate {
    fn default() -> Self {
        Self {
            role: Role::Follower,
            term: None,
            term_start: None,
            start_fence: None,
        }
    }
}

impl StageGate {
    /// The gate of a scheduler that starts behind `start_fence`.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn fenced(
        start_fence: StartFence,
    ) -> Self {
        Self {
            start_fence: Some(start_fence),
            ..Self::default()
        }
    }

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

    /// Open the gate: this node stages the vShard's txns.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn open(&mut self) {
        self.role = Role::Leader;
    }

    /// Close the gate: this node holds the vShard's txns.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn close(&mut self) {
        self.role = Role::Follower;
    }

    /// The data-group term this node leads, as last seen.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn term(&self) -> Option<u64> {
        self.term
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
            tracing::debug!(
                node_id = self.shared.node_id,
                vshard_id = self.vshard_id,
                group_id = leadership.group_id,
                term = leadership.term,
                term_start = ?leadership.term_start,
                applied = self
                    .shared
                    .applied_index_watcher(leadership.group_id)
                    .current(),
                "calvin scheduler: this node won the data group; staging waits for the term's \
                 first entry to apply"
            );
        }
        if self.role.is_leader() {
            return;
        }
        let applied = self
            .shared
            .applied_index_watcher(leadership.group_id)
            .current();
        if self.role.term_start.is_some_and(|start| applied >= start)
            && self.start_fence_passed(leadership.group_id, applied)
        {
            // The apply loop pushes a redo's install report before the
            // applied index passes the redo. Each report at or below
            // `applied` already waits in the inbox. Handling them first
            // completes their txns, so promotion stages none of them again.
            self.drain_inbox();
            self.promote();
        }
    }

    /// Whether every write the gate admitted for this vShard before its lock
    /// table was registered applied, with `applied` the group's applied index.
    ///
    /// Such a write holds no key on this table. A txn that stages before it
    /// reached its core reads the state before it, and its redo overwrites it.
    fn start_fence_passed(&mut self, group_id: u64, applied: u64) -> bool {
        let Some(fence) = self.role.start_fence else {
            return true;
        };
        let passed = match self
            .shared
            .calvin
            .admission_tickets
            .fence_state(fence, group_id)
        {
            FenceState::Open => false,
            FenceState::Through(through) => through.is_none_or(|index| applied >= index),
        };
        if passed {
            self.role.start_fence = None;
        } else {
            tracing::debug!(
                vshard_id = self.vshard_id,
                group_id,
                applied,
                "calvin scheduler: staging waits for writes admitted before its lock table"
            );
        }
        passed
    }

    /// This node's term and term start in the vShard's data group, while it
    /// leads the group.
    fn leadership(&self) -> Option<Leadership> {
        let group_id = self
            .routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_for_vshard(self.vshard_id)
            .ok()?;
        let held = self.leader_board.leadership(group_id)?;
        Some(Leadership {
            group_id,
            term: held.term,
            term_start: held.term_start,
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

    /// Note that a redo proposal found this node no longer leads.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn on_not_leader(&mut self) {
        if self.role.is_leader() {
            self.demote("a redo proposal found this node no longer leads");
        }
        self.role.term = None;
        self.role.term_start = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::calvin::types::SchedulerInput;

    use super::super::role_fixtures::{scheduler, staged_requests, validate_only_on};
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::elect_data_group_leader;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        redo_applied, staged_response,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
    use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

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

    /// A redo of an earlier term reports its install before the applied
    /// index passes the term start. The new leader handles that report
    /// before it stages, so the txn's position is applied and nothing stages.
    #[tokio::test]
    async fn a_new_leader_stages_no_txn_whose_install_report_waits() {
        let (mut scheduler, _dir, mut data_side) = scheduler();
        let group_id = elect_data_group_leader(&scheduler);
        let txn_id = TxnId::new(3, 0);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(validate_only_on(3, 1))));
        scheduler.refresh_role();
        assert!(staged_requests(&mut data_side).is_empty());

        let inbox = Arc::clone(scheduler.inbox.inbox());
        inbox
            .push(
                txn_id.epoch,
                txn_id.position,
                redo_applied(staged_response(Status::Ok, None)),
            )
            .await;
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
        assert!(
            staged_requests(&mut data_side).is_empty(),
            "the applied txn stages nothing"
        );
        assert!(!scheduler.pending.contains_key(&txn_id));
        assert!(scheduler.applied.is_applied(3, 0));
    }

    /// A write the gate admitted with no lock table holds the stage gate
    /// closed until it landed and its index applied.
    #[tokio::test]
    async fn a_new_scheduler_stages_nothing_before_earlier_unheld_writes_applied() {
        let (mut scheduler, _dir, mut data_side) = scheduler();
        let tickets = Arc::clone(&scheduler.shared.calvin.admission_tickets);
        let write = tickets.open(scheduler.vshard_id);
        scheduler.role = StageGate::fenced(tickets.start_fence(scheduler.vshard_id));

        let group_id = elect_data_group_leader(&scheduler);
        let term_start = scheduler
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .term_start_index(group_id)
            .expect("the elected node names its term start");
        let watcher = scheduler.shared.applied_index_watcher(group_id);
        watcher.bump(term_start);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(validate_only_on(3, 1))));

        scheduler.refresh_role();
        assert!(!scheduler.role.is_leader(), "the write has not landed");
        assert!(staged_requests(&mut data_side).is_empty());

        let landed_at = term_start + 5;
        write.landed(group_id, landed_at);
        scheduler.refresh_role();
        assert!(!scheduler.role.is_leader(), "the write has not applied");
        assert!(staged_requests(&mut data_side).is_empty());

        watcher.bump(landed_at);
        scheduler.refresh_role();
        assert!(scheduler.role.is_leader(), "the gate opens");
        assert_eq!(staged_requests(&mut data_side), vec![(3, 0)]);
    }
}
