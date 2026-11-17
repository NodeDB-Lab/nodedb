// SPDX-License-Identifier: BUSL-1.1

//! Immediate election of each group this node is the only voter of.

use std::time::{Duration, Instant};

use nodedb_raft::NodeRole;

use super::core::MultiRaft;

impl MultiRaft {
    /// Move the election deadline of each group this node alone votes in
    /// into the past, so the next tick elects this node.
    ///
    /// A sole voter is the whole quorum: no other node can win its election.
    /// Waiting out the election timeout first only delays the group's first
    /// write. A group with another voter keeps its random timeout.
    pub fn elect_sole_voter_groups_now(&mut self) {
        let past = Instant::now() - Duration::from_millis(1);
        for node in self.groups.values_mut() {
            if node.role() == NodeRole::Follower && node.voters().is_empty() {
                node.election_deadline_override(past);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::multi_raft::MultiRaft;
    use crate::routing::RoutingTable;

    /// A sole-voter group elects on the first tick. A group with a peer
    /// waits for its timeout.
    #[test]
    fn only_a_sole_voter_group_elects_on_the_first_tick() {
        let dir = tempfile::tempdir().expect("tempdir");
        let rt = RoutingTable::uniform(2, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(1, vec![]).expect("sole voter group");
        mr.add_group(2, vec![2]).expect("two voter group");

        mr.elect_sole_voter_groups_now();
        mr.tick().expect("tick");

        assert!(mr.is_group_leader(1));
        assert!(!mr.is_group_leader(2));
    }
}
