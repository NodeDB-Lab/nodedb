// SPDX-License-Identifier: BUSL-1.1

//! Leader-side peer response counts for a hosted group.

use std::time::Duration;

use nodedb_raft::{NodeRole, StalenessVerdict};

use crate::multi_raft::core::MultiRaft;

/// One sample of a leader's per-peer `AppendEntries` response counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAckSample {
    /// Term the counts belong to. Counts restart at zero in every term.
    pub term: u64,
    /// `(peer, responses received this term)` for every voter and learner.
    pub acks: Vec<(u64, u64)>,
}

impl MultiRaft {
    /// This node's term in `group_id` while it leads the group, else `None`.
    /// Reads one group only.
    pub fn leader_term(&self, group_id: u64) -> Option<u64> {
        self.groups
            .get(&group_id)
            .filter(|node| node.role() == NodeRole::Leader)
            .map(|node| node.current_term())
    }

    /// Index of the no-op this node appended when it won its current term in
    /// `group_id`, while it leads the group, else `None`. Every entry of an
    /// earlier term in its log sits below it and commits with it.
    pub fn term_start_index(&self, group_id: u64) -> Option<u64> {
        self.groups
            .get(&group_id)
            .and_then(|node| node.term_start_index())
    }

    /// Whether this node heard from the leader of `group_id` within
    /// `window`. The leader itself always has. Unlike a staleness bound, a
    /// replica that is merely behind on apply still counts as in contact.
    /// False for a group not hosted here.
    pub fn leader_contact_within(&self, group_id: u64, window: Duration) -> bool {
        self.groups
            .get(&group_id)
            .is_some_and(|node| node.staleness_verdict(window) != StalenessVerdict::NoRecentContact)
    }

    /// Response counts from every peer of `group_id`. `None` when the group
    /// is not hosted here or this node does not lead it.
    pub fn leader_peer_acks(&self, group_id: u64) -> Option<PeerAckSample> {
        let node = self.groups.get(&group_id)?;
        if node.role() != NodeRole::Leader {
            return None;
        }
        let acks = node
            .voters()
            .iter()
            .chain(node.learners())
            .filter_map(|&peer| node.peer_ack_count(peer).map(|count| (peer, count)))
            .collect();
        Some(PeerAckSample {
            term: node.current_term(),
            acks,
        })
    }
}
