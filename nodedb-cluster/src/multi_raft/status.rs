// SPDX-License-Identifier: BUSL-1.1

//! Observability snapshots of the Raft groups hosted on this node.

use crate::multi_raft::core::MultiRaft;

/// Snapshot of a single Raft group's state for observability.
#[derive(Debug, Clone, serde::Serialize)]
pub struct GroupStatus {
    pub group_id: u64,
    /// Role as a human-readable string ("Leader", "Follower", "Candidate", "Learner").
    pub role: String,
    pub leader_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
    /// Highest log index covered by the latest compacted snapshot.
    /// Advances when the group's log is compacted past the start (gated
    /// by `RaftConfig::log_compaction_threshold`). A non-zero value
    /// means entries at or below it are no longer in the log and a
    /// lagging peer below this index can only be caught up via
    /// `InstallSnapshot`, never `AppendEntries`.
    pub snapshot_index: u64,
    pub member_count: usize,
    pub learner_count: usize,
    pub vshard_count: usize,
}

/// Membership snapshot for a hosted Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMembership {
    pub group_id: u64,
    pub leader_id: u64,
    /// Voting members, including this node when it is a voter.
    pub voters: Vec<u64>,
    /// Non-voting learners, including this node when it is a learner.
    pub learners: Vec<u64>,
}

impl MultiRaft {
    /// Snapshot the actual Raft membership rather than the vShard routing view.
    pub fn group_membership(&self, group_id: u64) -> Option<GroupMembership> {
        let node = self.groups.get(&group_id)?;
        let mut voters = node.voters().to_vec();
        let mut learners = node.learners().to_vec();
        match node.role() {
            nodedb_raft::NodeRole::Learner => learners.push(self.node_id),
            nodedb_raft::NodeRole::Observer => {}
            _ => voters.push(self.node_id),
        }
        voters.sort_unstable();
        voters.dedup();
        learners.sort_unstable();
        learners.dedup();
        Some(GroupMembership {
            group_id,
            leader_id: node.leader_id(),
            voters,
            learners,
        })
    }

    /// Snapshot of all Raft group states for observability.
    pub fn group_statuses(&self) -> Vec<GroupStatus> {
        let mut statuses = Vec::with_capacity(self.groups.len());
        for (&group_id, node) in &self.groups {
            let vshard_count = self
                .routing
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .vshards_for_group(group_id)
                .len();
            let self_is_voter = !matches!(
                node.role(),
                nodedb_raft::NodeRole::Learner | nodedb_raft::NodeRole::Observer
            );

            statuses.push(GroupStatus {
                group_id,
                role: format!("{:?}", node.role()),
                leader_id: node.leader_id(),
                term: node.current_term(),
                commit_index: node.commit_index(),
                last_applied: node.last_applied(),
                last_log_index: node.last_log_index(),
                snapshot_index: node.log_snapshot_index(),
                member_count: node.voters().len() + usize::from(self_is_voter),
                learner_count: node.learners().len()
                    + usize::from(node.role() == nodedb_raft::NodeRole::Learner),
                vshard_count,
            });
        }
        statuses.sort_by_key(|s| s.group_id);
        statuses
    }
}
