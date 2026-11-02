// SPDX-License-Identifier: BUSL-1.1

//! The system-time floor of each hosted group, as the floor exchange reads
//! and raises it (`crate::raft_loop::ordinal_floor`).

use nodedb_raft::state::NodeRole;

use crate::multi_raft::core::MultiRaft;

/// What a leader needs to run one floor exchange on a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloorRound {
    /// The term the leader leads at. A peer counts only while it answers at
    /// this term, and the round counts only while the leader still leads at
    /// it.
    pub term: u64,
    /// The floor the leader holds after its own raise.
    pub floor: i64,
    /// Every other voter of the group.
    pub voters: Vec<u64>,
    /// Voters, this node included, that must hold the floor.
    pub quorum: usize,
}

/// A peer's answer to a floor exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloorAnswer {
    /// The peer is at the round's term. Its floor, durable, after it took
    /// the leader's.
    Held(i64),
    /// The peer is at another term, or does not host the group. It counts
    /// toward no quorum.
    Refused { term: u64 },
}

impl MultiRaft {
    /// The floor this node holds for `group_id`, `None` when the group is
    /// not hosted here.
    pub fn group_ordinal_floor(&self, group_id: u64) -> Option<i64> {
        self.groups.get(&group_id).map(|node| node.ordinal_floor())
    }

    /// Raise this leader's floor of `group_id` to `floor` and describe the
    /// exchange that spreads it. `None` when this node does not lead the
    /// group. The caller makes the hard state durable (`persist_group_hard_state`
    /// and `reply_ticket`) before it sends the round.
    pub fn begin_floor_round(&mut self, group_id: u64, floor: i64) -> Option<FloorRound> {
        let node = self.groups.get_mut(&group_id)?;
        if node.role() != NodeRole::Leader {
            return None;
        }
        node.raise_ordinal_floor(floor);
        let voters = node.voters().to_vec();
        let quorum = voters.len().div_ceil(2) + 1;
        Some(FloorRound {
            term: node.current_term(),
            floor: node.ordinal_floor(),
            voters,
            quorum,
        })
    }

    /// The floor a write this leader proposes to `group_id` stamps above:
    /// `Some(floor)` once the leader gathered it from a quorum in its
    /// current term. `None` while it has not, and for a group this node
    /// does not lead.
    pub fn gathered_ordinal_floor(&self, group_id: u64) -> Option<i64> {
        let node = self.groups.get(&group_id)?;
        node.floor_gathered().then(|| node.ordinal_floor())
    }

    /// Record that a quorum answered this leader's floor exchange of
    /// `group_id` at `term`.
    pub fn mark_floor_gathered(&mut self, group_id: u64, term: u64) {
        if let Some(node) = self.groups.get_mut(&group_id) {
            node.mark_floor_gathered(term);
        }
    }

    /// Whether this node still leads `group_id` at `term`.
    pub fn leads_at_term(&self, group_id: u64, term: u64) -> bool {
        self.groups
            .get(&group_id)
            .is_some_and(|node| node.role() == NodeRole::Leader && node.current_term() == term)
    }

    /// A peer's side of a floor exchange: take `floor` from the leader of
    /// `term`, then answer the floor this node holds. Refused unless this
    /// node is at `term`: only the one leader of a term sends at it, and a
    /// node at a later term no longer counts for the earlier one. The caller
    /// makes the hard state durable before the answer leaves.
    pub fn answer_floor_round(&mut self, group_id: u64, term: u64, floor: i64) -> FloorAnswer {
        let Some(node) = self.groups.get_mut(&group_id) else {
            return FloorAnswer::Refused { term: 0 };
        };
        if node.current_term() != term {
            return FloorAnswer::Refused {
                term: node.current_term(),
            };
        }
        node.raise_ordinal_floor(floor);
        FloorAnswer::Held(node.ordinal_floor())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::routing::RoutingTable;

    fn coordinator(dir: &tempfile::TempDir) -> MultiRaft {
        MultiRaft::new(
            1,
            RoutingTable::uniform(1, &[1, 2, 3], 3),
            PathBuf::from(dir.path()),
        )
    }

    /// A single voter that elected itself leads group 7.
    fn sole_leader(mr: &mut MultiRaft) {
        mr.add_group(7, vec![]).expect("add group");
        let node = mr.groups_mut().get_mut(&7).expect("group 7");
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        mr.tick().expect("tick");
        assert!(mr.is_group_leader(7), "a single voter elects itself");
    }

    #[test]
    fn a_follower_answers_only_at_its_own_term() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mr = coordinator(&dir);
        mr.add_group(7, vec![1, 2, 3]).expect("add group");
        let (_, term) = mr.group_leader_at_term(7);
        assert_eq!(
            mr.answer_floor_round(7, term + 1, 900),
            FloorAnswer::Refused { term },
            "a later term's leader is not counted before this node reaches it"
        );
        assert_eq!(mr.group_ordinal_floor(7), Some(0));
        assert_eq!(mr.answer_floor_round(7, term, 900), FloorAnswer::Held(900));
        assert_eq!(
            mr.answer_floor_round(7, term, 100),
            FloorAnswer::Held(900),
            "a lower floor never lowers it"
        );
        assert_eq!(
            mr.answer_floor_round(9, term, 1),
            FloorAnswer::Refused { term: 0 }
        );
        assert!(
            mr.begin_floor_round(7, 1_000).is_none(),
            "a follower runs no round"
        );
    }

    #[test]
    fn a_leader_gathers_once_per_term() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mr = coordinator(&dir);
        sole_leader(&mut mr);
        assert_eq!(mr.gathered_ordinal_floor(7), None, "not gathered yet");
        let round = mr
            .begin_floor_round(7, 500)
            .expect("the leader runs a round");
        assert_eq!(round.floor, 500);
        assert_eq!(round.quorum, 1);
        assert!(round.voters.is_empty());
        assert!(mr.leads_at_term(7, round.term));
        assert!(!mr.leads_at_term(7, round.term + 1));
        mr.mark_floor_gathered(7, round.term);
        assert_eq!(mr.gathered_ordinal_floor(7), Some(500));
    }
}
