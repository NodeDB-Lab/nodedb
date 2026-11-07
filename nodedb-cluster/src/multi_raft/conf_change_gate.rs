// SPDX-License-Identifier: BUSL-1.1

//! The leader's check on a configuration change before it enters the log.
//!
//! A replica awaiting a snapshot holds no state to lead from, and only a
//! leader sends it one. So a group must keep a whole voter:
//! - A learner is promoted only once it caught up and awaits no snapshot.
//! - A voter is removed only when a voter that may lead stays. The leader
//!   may lead: a replica whose data diverged steps down and never
//!   campaigns. A peer counts by the state its latest reply this term
//!   reported: one that only wants a snapshot may lead, one whose data
//!   diverged may not. A peer not heard this term does not count: its
//!   state is unknown.
//!
//! Every proposal path runs the check: a change proposed as a conf change,
//! and conf-change bytes proposed as a plain entry.

use nodedb_raft::{NodeRole, RaftError, ReplicaState};

use crate::conf_change::{ConfChange, ConfChangeType};
use crate::error::{ClusterError, Result};

use super::core::MultiRaft;

impl MultiRaft {
    /// Refuse `change` to `group_id` when it would promote a learner that is
    /// not caught up, or remove the last whole voter. A node that does not
    /// lead the group admits it: its propose refuses with `NotLeader`.
    pub(super) fn admit_conf_change(&self, group_id: u64, change: &ConfChange) -> Result<()> {
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        if node.role() != NodeRole::Leader {
            return Ok(());
        }
        match change.change_type {
            // At apply time the commit index has already passed the promote
            // entry, so the check runs here, before the entry enters the log.
            ConfChangeType::PromoteLearner => {
                if self.peer_caught_up(group_id, change.node_id) {
                    return Ok(());
                }
                Err(ClusterError::LearnerNotCaughtUp {
                    group_id,
                    node_id: change.node_id,
                    match_index: node.match_index_for(change.node_id).unwrap_or(0),
                    commit_index: node.commit_index(),
                })
            }
            ConfChangeType::RemoveNode => {
                let whole_voter_stays = std::iter::once(self.node_id)
                    .chain(node.voters().iter().copied())
                    .filter(|&voter| voter != change.node_id)
                    .any(|voter| {
                        if voter == self.node_id {
                            node.replica_state().may_lead()
                        } else {
                            node.peer_replica_state(voter)
                                .is_some_and(ReplicaState::may_lead)
                        }
                    });
                if whole_voter_stays {
                    return Ok(());
                }
                Err(ClusterError::Raft(RaftError::ProposalRejected {
                    reason: format!(
                        "removing voter {} from group {group_id} leaves no voter that may \
                         lead; every other voter is unheard this term or its data diverged; \
                         retry once another voter answers that it may lead",
                        change.node_id
                    ),
                }))
            }
            ConfChangeType::AddNode
            | ConfChangeType::AddLearner
            | ConfChangeType::RemoveLearner => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_raft::message::AppendEntriesResponse;
    use nodedb_raft::node::leader_lease::UNTRACKED_ROUND;

    use super::*;
    use crate::routing::RoutingTable;

    /// Node 1 leads group 0 alone, with its election no-op committed.
    fn leader_mr() -> (MultiRaft, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).expect("add group");
        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        mr.tick().expect("tick");
        mr.wait_all_durable_blocking().expect("durable");
        mr.tick().expect("tick");
        assert!(mr.group_role_is_leader(0));
        (mr, dir)
    }

    fn reply(mr: &MultiRaft, last_log_index: u64, replica: ReplicaState) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term: mr.groups.get(&0).map_or(0, |node| node.current_term()),
            success: !replica.wants_snapshot(),
            last_log_index,
            round: UNTRACKED_ROUND,
            replica,
        }
    }

    fn change(change_type: ConfChangeType, node_id: u64) -> ConfChange {
        ConfChange {
            change_type,
            node_id,
        }
    }

    /// The leader cannot remove itself while every other voter awaits a
    /// snapshot: no replica would be left to lead and send it one.
    #[test]
    fn removing_the_last_whole_voter_is_refused() {
        let (mut mr, _dir) = leader_mr();
        mr.apply_conf_change(0, &change(ConfChangeType::AddNode, 2))
            .expect("add voter 2");
        let awaiting = reply(&mr, 0, ReplicaState::AwaitingSnapshot);
        mr.handle_append_entries_response(0, 2, &awaiting)
            .expect("reply");

        let refused = mr.propose_conf_change(0, &change(ConfChangeType::RemoveNode, 1));
        assert!(
            matches!(
                refused,
                Err(ClusterError::Raft(RaftError::ProposalRejected { .. }))
            ),
            "got {refused:?}"
        );
        let bytes = change(ConfChangeType::RemoveNode, 1)
            .to_entry_data()
            .expect("encode");
        assert!(
            mr.propose_to_group(0, bytes).is_err(),
            "the plain proposal path runs the same check"
        );

        assert!(
            mr.admit_conf_change(0, &change(ConfChangeType::RemoveNode, 2))
                .is_ok(),
            "removing the awaiting voter keeps the whole leader"
        );
    }

    /// A fresh leader has heard no other voter this term: their state is
    /// unknown, so it cannot remove itself.
    #[test]
    fn a_leader_that_heard_no_voter_cannot_remove_itself() {
        let (mut mr, _dir) = leader_mr();
        for voter in [2, 3] {
            mr.apply_conf_change(0, &change(ConfChangeType::AddNode, voter))
                .expect("add voter");
        }
        let refused = mr.admit_conf_change(0, &change(ConfChangeType::RemoveNode, 1));
        assert!(
            matches!(
                refused,
                Err(ClusterError::Raft(RaftError::ProposalRejected { .. }))
            ),
            "got {refused:?}"
        );

        let answer = reply(&mr, 0, ReplicaState::WantsSnapshot);
        mr.handle_append_entries_response(0, 3, &answer)
            .expect("reply");
        assert!(
            mr.admit_conf_change(0, &change(ConfChangeType::RemoveNode, 1))
                .is_ok(),
            "voter 3 answered and may lead"
        );
    }

    /// Node 1 leads with voters 2 and 3, which both answered with `state`.
    fn leader_with_two_voters_in(state: ReplicaState) -> (MultiRaft, tempfile::TempDir) {
        let (mut mr, dir) = leader_mr();
        for voter in [2, 3] {
            mr.apply_conf_change(0, &change(ConfChangeType::AddNode, voter))
                .expect("add voter");
        }
        let answer = reply(&mr, 0, state);
        for voter in [2, 3] {
            mr.handle_append_entries_response(0, voter, &answer)
                .expect("reply");
        }
        (mr, dir)
    }

    /// Voters that only want a snapshot for state beside their log may
    /// lead: the leader can remove itself. Voters whose data diverged may
    /// not: the removal is refused.
    #[test]
    fn a_voter_that_wants_a_snapshot_may_lead_and_a_diverged_one_may_not() {
        let (mr, _dir) = leader_with_two_voters_in(ReplicaState::WantsSnapshot);
        assert!(
            mr.admit_conf_change(0, &change(ConfChangeType::RemoveNode, 1))
                .is_ok()
        );
        assert!(
            !mr.peer_caught_up(0, 2),
            "a peer that wants a snapshot is behind"
        );

        let (mr, _dir) = leader_with_two_voters_in(ReplicaState::AwaitingSnapshot);
        assert!(matches!(
            mr.admit_conf_change(0, &change(ConfChangeType::RemoveNode, 1)),
            Err(ClusterError::Raft(RaftError::ProposalRejected { .. }))
        ));
        assert!(!mr.peer_caught_up(0, 2));
    }

    #[test]
    fn removing_a_voter_while_a_whole_voter_stays_is_admitted() {
        let (mut mr, _dir) = leader_mr();
        mr.apply_conf_change(0, &change(ConfChangeType::AddNode, 2))
            .expect("add voter 2");
        let last = mr.groups.get(&0).map_or(0, |node| node.last_log_index());
        let whole = reply(&mr, last, ReplicaState::Whole);
        mr.handle_append_entries_response(0, 2, &whole)
            .expect("reply");
        assert!(
            mr.admit_conf_change(0, &change(ConfChangeType::RemoveNode, 1))
                .is_ok()
        );
    }

    /// A remounted learner whose log matches still awaits its snapshot: it
    /// is promoted only once the snapshot installs.
    #[test]
    fn a_learner_awaiting_a_snapshot_is_not_promoted() {
        let (mut mr, _dir) = leader_mr();
        mr.apply_conf_change(0, &change(ConfChangeType::AddLearner, 2))
            .expect("add learner 2");
        let last = mr.groups.get(&0).map_or(0, |node| node.last_log_index());
        let matched = reply(&mr, last, ReplicaState::Whole);
        mr.handle_append_entries_response(0, 2, &matched)
            .expect("reply");
        let awaiting = reply(&mr, last, ReplicaState::AwaitingSnapshot);
        mr.handle_append_entries_response(0, 2, &awaiting)
            .expect("reply");

        assert!(mr.ready_learners(0).is_empty());
        assert!(matches!(
            mr.admit_conf_change(0, &change(ConfChangeType::PromoteLearner, 2)),
            Err(ClusterError::LearnerNotCaughtUp { .. })
        ));

        mr.handle_append_entries_response(0, 2, &matched)
            .expect("reply");
        assert_eq!(mr.ready_learners(0), vec![2]);
    }
}
