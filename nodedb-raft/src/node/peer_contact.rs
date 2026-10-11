// SPDX-License-Identifier: BUSL-1.1

//! Read-only views of contact with peers: the leader's per-peer response
//! counts, and whether the leader this node names is live.

use std::time::Instant;

use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// `AppendEntries` responses received from `peer` in the current term,
    /// rejections included. `None` when this node is not the leader.
    pub fn peer_ack_count(&self, peer: u64) -> Option<u64> {
        self.leader_state
            .as_ref()
            .map(|leader| leader.ack_count_for(peer))
    }

    /// The highest log index every live voter's log is known to hold. See
    /// [`Self::replicated_floor_at`].
    pub fn replicated_floor(&self) -> u64 {
        self.replicated_floor_at(Instant::now())
    }

    /// The highest log index every live voter's log is known to hold at
    /// `now`. A log compacted at or below it never sends a live voter a
    /// snapshot.
    ///
    /// The leader takes the lowest `match_index` over the other voters,
    /// capped at its commit index: a committed entry stays in every log that
    /// holds it. It sends the value with each `AppendEntries`. Every replica
    /// keeps the highest value it saw, so the floor never moves back.
    ///
    /// Only voters that answered within the check-quorum window count. A
    /// voter not yet heard this term counts until the window passes from the
    /// election win. A silent voter never holds compaction on any replica.
    /// This assumes a voter that falls behind the compacted boundary recovers
    /// through `InstallSnapshot` once it answers again.
    ///
    /// Learners and observers never count: a new replica takes a snapshot.
    /// Off the leader path this is the highest floor a leader sent.
    pub fn replicated_floor_at(&self, now: Instant) -> u64 {
        let Some(leader) = self.leader_state.as_ref() else {
            return self.replicated_floor;
        };
        let lowest_live_voter = self
            .config
            .peers
            .iter()
            .filter(|&&peer| self.voter_heard_within_window(peer, now))
            .map(|&peer| leader.match_index_for(peer))
            .min()
            .unwrap_or(u64::MAX);
        self.replicated_floor
            .max(lowest_live_voter.min(self.volatile.commit_index))
    }

    /// Whether `peer`'s latest response to this leader asked for a snapshot.
    /// Such a peer holds no state to lead from. `false` off the leader.
    pub fn peer_awaits_snapshot(&self, peer: u64) -> bool {
        self.leader_state
            .as_ref()
            .is_some_and(|leader| leader.awaiting_snapshot.contains(&peer))
    }

    /// The leader this node names at `now`, when it has proof the leader is
    /// live: this node leads, or the leader reached it within
    /// `election_timeout_min`. `0` otherwise.
    ///
    /// A follower keeps naming a crashed leader until its election timeout
    /// fires. This view drops that leader as soon as its contact goes stale.
    pub fn live_leader(&self, now: Instant) -> u64 {
        if self.role == NodeRole::Leader {
            return self.config.node_id;
        }
        if self.leader_id == 0 {
            return 0;
        }
        let fresh = self.leader_contact.is_some_and(|contact| {
            now.saturating_duration_since(contact.at) < self.config.election_timeout_min
        });
        if fresh { self.leader_id } else { 0 }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::message::{AppendEntriesRequest, RequestVoteResponse};
    use crate::node::core::RaftNode;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;
    use crate::test_support::{force_election, test_config};

    fn heartbeat(term: u64, leader_id: u64) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
            round: 1,
            replicated_floor: 0,
        }
    }

    /// A leadership transfer moves the term before the new leader reaches
    /// this node. In between, this node names no leader: never the old
    /// leader at the new term, which a routing hint at that term would keep.
    #[test]
    fn a_transfer_campaign_leaves_no_leader_named_at_its_term_until_the_winner_reaches_this_node() {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        let _ = node.handle_append_entries(&heartbeat(1, 1));
        let now = std::time::Instant::now();
        assert_eq!((node.live_leader(now), node.current_term()), (1, 1));

        // Node 3's transfer campaign at term 2.
        let vote = node.handle_request_vote(&crate::message::RequestVoteRequest {
            term: 2,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 1,
            transfer: true,
        });
        assert!(vote.vote_granted);
        assert_eq!(node.current_term(), 2);
        assert_eq!(node.leader_id(), 0, "the old leader does not lead term 2");
        assert_eq!(node.live_leader(std::time::Instant::now()), 0);

        let _ = node.handle_append_entries(&heartbeat(2, 3));
        let now = std::time::Instant::now();
        assert_eq!((node.live_leader(now), node.current_term()), (3, 2));
    }

    /// A leader that steps down for lost quorum names no leader.
    #[test]
    fn a_leader_that_steps_down_names_no_leader() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        force_election(&mut node);
        let _ = node.take_ready();
        node.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(node.leader_id(), 1);
        node.become_follower(node.current_term());
        assert_eq!(node.leader_id(), 0);
        assert_eq!(node.live_leader(std::time::Instant::now()), 0);
    }

    /// The leader's floor is the lowest voter's committed match. Within the
    /// window from the election win, a voter that has not answered holds it
    /// at 0.
    #[test]
    fn the_replicated_floor_waits_for_every_voter() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        force_election(&mut node);
        let _ = node.take_ready();
        node.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(node.role(), NodeRole::Leader);
        let ack = crate::message::AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: 1,
            round: 1,
            needs_snapshot: false,
        };
        node.handle_append_entries_response(2, &ack);
        assert_eq!(node.replicated_floor(), 0, "voter 3 holds nothing yet");
        node.handle_append_entries_response(3, &ack);
        assert_eq!(node.commit_index(), 1);
        assert_eq!(node.replicated_floor(), 1);
    }

    /// Elect node 1 over `cfg`, with every voter granting its vote.
    fn elect(cfg: crate::node::config::RaftConfig) -> RaftNode<MemStorage> {
        let peers = cfg.peers.clone();
        let mut node = RaftNode::new(cfg, MemStorage::new());
        force_election(&mut node);
        for peer in peers {
            if node.role() == NodeRole::Leader {
                break;
            }
            node.handle_request_vote_response(
                peer,
                &RequestVoteResponse {
                    term: node.current_term(),
                    vote_granted: true,
                },
            );
        }
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();
        node
    }

    /// Peer `peer` answers the latest round, acking `last_log_index`.
    fn answer(node: &mut RaftNode<MemStorage>, peer: u64, success: bool, last_log_index: u64) {
        let resp = crate::message::AppendEntriesResponse {
            term: node.current_term(),
            success,
            last_log_index,
            round: node.lease.next_round - 1,
            needs_snapshot: false,
        };
        node.handle_append_entries_response(peer, &resp);
    }

    /// Move the election win back past the check-quorum window.
    fn age_term_start(node: &mut RaftNode<MemStorage>) {
        let window = node.config.election_timeout_max;
        node.leader_since = Some(Instant::now() - window - Duration::from_millis(1));
    }

    /// A voter silent for the whole check-quorum window stops holding the
    /// floor. The voter that answered sets it.
    #[test]
    fn a_silent_voter_stops_holding_the_floor_after_the_window() {
        let mut node = elect(test_config(1, vec![2, 3]));
        answer(&mut node, 2, true, 1);
        assert_eq!(node.commit_index(), 1);
        assert_eq!(
            node.replicated_floor(),
            0,
            "voter 3 counts within the window from the election win"
        );
        age_term_start(&mut node);
        answer(&mut node, 2, true, 1);
        assert_eq!(
            node.replicated_floor(),
            1,
            "silent voter 3 no longer counts"
        );
    }

    /// A voter that answered within the window holds the floor at its
    /// `match_index`, even when that answer was a rejection. Voter 3 answers
    /// before the commit moves: the floor never moves back.
    #[test]
    fn a_voter_heard_within_the_window_still_holds_the_floor() {
        let mut node = elect(test_config(1, vec![2, 3]));
        age_term_start(&mut node);
        answer(&mut node, 3, false, 0);
        assert!(node.voter_heard_within_window(3, Instant::now()));
        answer(&mut node, 2, true, 1);
        assert_eq!(node.commit_index(), 1);
        assert_eq!(
            node.replicated_floor(),
            0,
            "voter 3 answered and holds nothing"
        );

        let stale = Instant::now() + node.config.election_timeout_max;
        assert!(
            !node.voter_heard_within_window(3, stale),
            "the answer ages out of the window"
        );
    }

    /// A learner never holds the floor, answered or silent.
    #[test]
    fn learners_never_count_toward_the_floor() {
        let mut cfg = test_config(1, vec![2, 3]);
        cfg.learners = vec![4];
        let mut node = elect(cfg);
        answer(&mut node, 4, false, 0);
        answer(&mut node, 2, true, 1);
        answer(&mut node, 3, true, 1);
        assert_eq!(node.commit_index(), 1);
        assert_eq!(node.replicated_floor(), 1, "learner 4 holds nothing");
        age_term_start(&mut node);
        assert_eq!(node.replicated_floor(), 1);
    }

    /// A follower keeps the highest floor a leader sent it.
    #[test]
    fn a_follower_keeps_the_highest_replicated_floor() {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        let heartbeat = |replicated_floor| AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
            round: 1,
            replicated_floor,
        };
        let _ = node.handle_append_entries(&heartbeat(5));
        assert_eq!(node.replicated_floor(), 5);
        let _ = node.handle_append_entries(&heartbeat(3));
        assert_eq!(node.replicated_floor(), 5, "the floor never moves back");
    }

    #[test]
    fn a_follower_names_the_leader_only_while_its_contact_is_fresh() {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        assert_eq!(node.live_leader(std::time::Instant::now()), 0);
        let _ = node.handle_append_entries(&AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 1,
            round: 1,
            replicated_floor: 0,
        });
        let now = std::time::Instant::now();
        assert_eq!(node.live_leader(now), 1);
        let stale = now + node.config.election_timeout_min + Duration::from_millis(1);
        assert_eq!(node.live_leader(stale), 0);
        assert_eq!(
            node.leader_id(),
            1,
            "the Raft leader id itself is unchanged"
        );
    }
}
