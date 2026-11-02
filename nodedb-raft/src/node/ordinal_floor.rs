// SPDX-License-Identifier: BUSL-1.1

//! The group's system-time floor: the highest read cut any leader of the
//! group closed.
//!
//! A leader that closes a read cut `S` raises the floor to `S` on a quorum
//! of voters, each durably, before the read proceeds. A new leader gathers
//! the floor from a quorum of voters of its own term before it stamps a
//! write. The two quorums share a voter, and that voter answered the old
//! term's raise before the new term's gather, since its term only grows. So
//! the new leader stamps above every closed cut, whatever its clock reads.
//!
//! The floor lives in the hard state, so a restart keeps it. The host drives
//! the raise and the gather (the cluster's floor exchange); this node holds
//! the value.

use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// The floor this node holds.
    pub fn ordinal_floor(&self) -> i64 {
        self.hard_state.ordinal_floor
    }

    /// Raise the floor to `floor`. A raise stages the hard state, and the
    /// caller makes it durable before it reports the floor to any peer.
    /// Returns whether the floor rose.
    pub fn raise_ordinal_floor(&mut self, floor: i64) -> bool {
        if floor <= self.hard_state.ordinal_floor {
            return false;
        }
        self.hard_state.ordinal_floor = floor;
        self.persist_hard_state();
        true
    }

    /// Whether this node leads and gathered the floor from a quorum of
    /// voters in its current term. Until then it stamps no write.
    pub fn floor_gathered(&self) -> bool {
        self.role == NodeRole::Leader && self.floor_gathered_term == self.hard_state.current_term
    }

    /// Record that a quorum of voters answered a floor exchange of this
    /// node's leadership at `term`. Ignored unless this node still leads at
    /// `term`.
    pub fn mark_floor_gathered(&mut self, term: u64) {
        if self.role == NodeRole::Leader && self.hard_state.current_term == term {
            self.floor_gathered_term = term;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::node::core::RaftNode;
    use crate::storage::{LogStorage, MemStorage};
    use crate::test_support::{force_election, test_config};

    #[test]
    fn the_floor_only_rises_and_survives_a_restart() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        assert_eq!(node.ordinal_floor(), 0);
        assert!(node.raise_ordinal_floor(500));
        assert!(
            !node.raise_ordinal_floor(400),
            "a lower value never lowers it"
        );
        assert!(!node.raise_ordinal_floor(500));
        assert_eq!(node.ordinal_floor(), 500);
        node.persist_hard_state_if_dirty().expect("persist");
        assert_eq!(
            node.storage()
                .load_hard_state()
                .expect("load")
                .ordinal_floor,
            500
        );
    }

    #[test]
    fn a_gather_counts_only_for_the_term_it_ran_in() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        node.mark_floor_gathered(node.current_term());
        assert!(!node.floor_gathered(), "a follower gathers nothing");
        force_election(&mut node);
        let term = node.current_term();
        for peer in [2, 3] {
            node.handle_request_vote_response(
                peer,
                &crate::message::RequestVoteResponse {
                    term,
                    vote_granted: true,
                },
            );
        }
        assert_eq!(node.role(), crate::state::NodeRole::Leader);
        assert!(!node.floor_gathered(), "a new term gathers again");
        node.mark_floor_gathered(term - 1);
        assert!(!node.floor_gathered());
        node.mark_floor_gathered(term);
        assert!(node.floor_gathered());
    }
}
