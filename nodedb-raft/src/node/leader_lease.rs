// SPDX-License-Identifier: BUSL-1.1

//! Leader lease: serving a linearizable read without a quorum round.
//!
//! A follower that hears the leader refuses every vote for
//! `election_timeout_min`, except a vote for a transfer campaign (see
//! [`RaftNode::vote_refusal_active`]). A node that boots refuses the same votes
//! for `election_timeout_max`: a restart forgets the leader contact, and a
//! lease taken before the restart can still be live. Once a quorum has answered a request
//! the leader sent at `t`, no successor can win before `t +
//! election_timeout_min` on the followers' clocks. The leader serves reads at
//! its own commit index until `t + RaftConfig::lease_duration()`, which keeps a
//! margin for clock-rate drift.
//!
//! The anchor is the send time of the request, never the arrival time of the
//! response. The follower opened its refusal window when it processed the
//! request, and the response can arrive much later. Check-quorum dates the
//! same answers at their arrival (see [`super::quorum_contact`]). That date
//! only keeps the leader in its role and never extends the lease.
//!
//! Each `AppendEntries` carries a round number, and the follower echoes it. The
//! leader keeps `round -> sent_at` for the rounds no quorum has covered yet, and
//! the highest round each voter has acknowledged.
//!
//! [`RaftConfig::lease_duration()`]: crate::node::config::RaftConfig::lease_duration

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::node::core::RaftNode;
use crate::node::quorum_contact::VoterAck;
use crate::state::NodeRole;
use crate::storage::LogStorage;

/// Round of a request that no lease tracks, such as one sent to an observer.
/// Also the acknowledged round of a voter that has not answered yet.
pub const UNTRACKED_ROUND: u64 = 0;

/// Cap on rounds that wait for a quorum. It bounds memory while no quorum
/// answers. Check-quorum steps the leader down long before a normal cluster
/// fills it. Evicting a round only loses lease evidence and never grants any.
const MAX_PENDING_ROUNDS: usize = 4096;

/// Leader-side lease bookkeeping for one Raft group.
#[derive(Debug)]
pub struct LeaseState {
    /// Next round to stamp. Never reset, so a response to a request from an
    /// earlier term can never name a round of this term.
    pub(super) next_round: u64,
    /// First round stamped in the current leadership term.
    pub(super) term_first_round: u64,
    /// `(round, sent_at)` in ascending round order, for rounds above the last
    /// quorum round.
    pub(super) sent: VecDeque<(u64, Instant)>,
    /// Send time of the highest round a quorum has acknowledged.
    pub(super) anchor: Option<Instant>,
    /// Set when a leadership transfer starts. A transfer campaign bypasses
    /// vote refusal, and its `RequestVote` can arrive after the transfer
    /// aborts, so the lease stays off until the next term.
    pub(super) revoked: bool,
}

impl LeaseState {
    pub fn new() -> Self {
        Self {
            next_round: UNTRACKED_ROUND + 1,
            term_first_round: UNTRACKED_ROUND + 1,
            sent: VecDeque::new(),
            anchor: None,
            revoked: false,
        }
    }

    /// Drop the previous term's evidence when this node becomes leader.
    pub(super) fn begin_term(&mut self) {
        self.term_first_round = self.next_round;
        self.sent.clear();
        self.anchor = None;
        self.revoked = false;
    }

    /// Allocate the round for a request sent at `now`.
    pub(super) fn stamp(&mut self, now: Instant) -> u64 {
        let round = self.next_round;
        self.next_round = self.next_round.saturating_add(1);
        if self.sent.len() >= MAX_PENDING_ROUNDS {
            self.sent.pop_front();
        }
        self.sent.push_back((round, now));
        round
    }

    /// Whether `round` was stamped in the current leadership term.
    pub(super) fn is_current(&self, round: u64) -> bool {
        round >= self.term_first_round && round < self.next_round
    }

    /// Record that a quorum acknowledged `round`, and return the anchor.
    pub(super) fn settle(&mut self, round: u64) -> Option<Instant> {
        if let Ok(pos) = self.sent.binary_search_by_key(&round, |&(r, _)| r) {
            let sent_at = self.sent[pos].1;
            if self.anchor.is_none_or(|anchor| sent_at > anchor) {
                self.anchor = Some(sent_at);
            }
            self.sent.drain(..=pos);
        }
        self.anchor
    }

    pub(super) fn revoke(&mut self) {
        self.revoked = true;
    }

    /// Forget the anchor. A voter-set change calls this, so the next anchor
    /// comes from a quorum of the new set.
    pub(super) fn clear_anchor(&mut self) {
        self.anchor = None;
    }

    /// When the lease ends, or `None` when there is no lease.
    pub(super) fn expires_at(&self, duration: Duration) -> Option<Instant> {
        if self.revoked {
            return None;
        }
        self.anchor.map(|anchor| anchor + duration)
    }

    /// Send time of pending `round` (for testing).
    #[cfg(test)]
    pub(super) fn sent_at(&self, round: u64) -> Option<Instant> {
        self.sent
            .iter()
            .find(|&&(r, _)| r == round)
            .map(|&(_, at)| at)
    }

    /// Replace the send time of pending `round` (for testing).
    #[cfg(test)]
    pub(super) fn set_sent_at(&mut self, round: u64, at: Instant) {
        if let Some(entry) = self.sent.iter_mut().find(|(r, _)| *r == round) {
            entry.1 = at;
        }
    }
}

impl Default for LeaseState {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: LogStorage> RaftNode<S> {
    /// Allocate the lease round for an `AppendEntries` sent now. The send is
    /// the one place the lease reads the clock itself: the round's anchor is
    /// the moment it leaves. Everything downstream takes `now` from its
    /// caller.
    pub(super) fn stamp_lease_round(&mut self) -> u64 {
        self.lease.stamp(Instant::now())
    }

    /// Record that voter `peer` answered `round`, in an answer that arrived at
    /// `now`. Rounds from an earlier term are ignored: the follower answered
    /// a request of a leadership that no longer exists.
    pub(super) fn record_lease_ack(&mut self, peer: u64, round: u64, now: Instant) {
        if !self.lease.is_current(round) {
            return;
        }
        match self.quorum_window.iter_mut().find(|ack| ack.peer == peer) {
            Some(ack) => {
                ack.round = ack.round.max(round);
                ack.arrived = ack.arrived.max(now);
            }
            None => self.quorum_window.push(VoterAck {
                peer,
                round,
                arrived: now,
            }),
        }
    }

    /// Highest round `peer` has acknowledged in this term.
    fn acked_round(&self, peer: u64) -> u64 {
        self.voter_ack(peer)
            .map_or(UNTRACKED_ROUND, |ack| ack.round)
    }

    /// Settle the highest round a quorum of voters has acknowledged, and
    /// return the lease anchor. `None` for a single voter, which has no
    /// round to settle.
    pub(super) fn settle_lease(&mut self) -> Option<Instant> {
        // This node acknowledges every round it sends, so a quorum needs
        // `quorum - 1` peers.
        let peers_needed = self.config.quorum().saturating_sub(1);
        if peers_needed == 0 {
            return None;
        }
        let mut acked: Vec<u64> = self
            .config
            .peers
            .iter()
            .map(|&peer| self.acked_round(peer))
            .collect();
        acked.sort_unstable_by(|a, b| b.cmp(a));
        let quorum_round = *acked.get(peers_needed - 1)?;
        if quorum_round == UNTRACKED_ROUND {
            return self.lease.anchor;
        }
        self.lease.settle(quorum_round)
    }

    /// Whether this node can serve a linearizable read at its commit index
    /// without a quorum round.
    ///
    /// Needs all of:
    /// - leader role;
    /// - no leadership transfer in progress or begun this term;
    /// - the current-term no-op committed, so the commit index covers every
    ///   earlier leader's commits;
    /// - `now` before the anchor plus `lease_duration()`.
    pub fn lease_valid(&self, now: Instant) -> bool {
        if self.role != NodeRole::Leader
            || self.leadership_transfer.is_some()
            || !self.current_term_committed()
        {
            return false;
        }
        // A single voter is the whole quorum. No other node can win a vote.
        if self.config.peers.is_empty() {
            return !self.lease.revoked;
        }
        self.lease
            .expires_at(self.config.lease_duration())
            .is_some_and(|end| now < end)
    }

    /// The commit index, when the lease lets a read be served there now.
    pub fn lease_read_index(&self, now: Instant) -> Option<u64> {
        self.lease_valid(now).then_some(self.volatile.commit_index)
    }

    /// Whether this node refuses pre-votes and every vote except a transfer
    /// vote at `now`.
    ///
    /// The follower half of the lease. It holds while a leader reached this
    /// node within `election_timeout_min`, and until the boot fence passes.
    pub(crate) fn vote_refusal_active(&self, now: Instant) -> bool {
        if now < self.boot_vote_fence {
            return true;
        }
        self.leader_contact.is_some_and(|contact| {
            now.saturating_duration_since(contact.at) < self.config.election_timeout_min
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{LeaseState, MAX_PENDING_ROUNDS};
    use crate::message::{
        AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
    };
    use crate::node::core::RaftNode;
    use crate::state::{HardState, NodeRole};
    use crate::storage::{LogStorage, MemStorage};
    use crate::test_support::{force_election, test_config};

    const ONE_TICK: Duration = Duration::from_nanos(1);

    /// Node 1 leading peers 2 and 3, with its election output drained.
    fn leader() -> RaftNode<MemStorage> {
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
        let _ = node.take_ready();
        node
    }

    /// Send one `AppendEntries` round and return the round sent to peer 2.
    fn send_round(node: &mut RaftNode<MemStorage>) -> u64 {
        node.replicate_to_all();
        let ready = node.take_ready();
        ready
            .messages
            .iter()
            .find(|(peer, _)| *peer == 2)
            .map(|(_, req)| req.round)
            .expect("the leader sends to peer 2")
    }

    fn answer(node: &mut RaftNode<MemStorage>, peer: u64, round: u64, last_log_index: u64) {
        node.handle_append_entries_response(
            peer,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index,
                round,
                replica: crate::state::ReplicaState::Whole,
            },
        );
    }

    /// A leader whose no-op is committed, with its lease anchored at the send
    /// time of one round. Returns the node and that send time.
    fn leased() -> (RaftNode<MemStorage>, Instant) {
        let mut node = leader();
        let round = send_round(&mut node);
        let sent = node.lease.sent_at(round).expect("the round is pending");
        let noop = node.last_log_index();
        answer(&mut node, 2, round, noop);
        assert_eq!(node.commit_index(), noop, "the answer commits the no-op");
        (node, sent)
    }

    fn lease_duration(node: &RaftNode<MemStorage>) -> Duration {
        node.config.election_timeout_min - node.config.lease_drift_margin()
    }

    #[test]
    fn the_lease_is_anchored_at_send_time() {
        let mut node = leader();
        let round = send_round(&mut node);
        let sent = node.lease.sent_at(round).expect("the round is pending");
        // The request left well before its answer is handled below.
        let sent = sent - Duration::from_millis(100);
        node.lease.set_sent_at(round, sent);
        let last = node.last_log_index();
        answer(&mut node, 2, round, last);

        assert_eq!(node.lease.anchor, Some(sent));
        assert!(node.lease_valid(sent));
        assert!(node.lease_valid(sent + lease_duration(&node) - ONE_TICK));
    }

    #[test]
    fn the_lease_expires_at_min_minus_the_drift_margin() {
        let (node, sent) = leased();
        let duration = lease_duration(&node);
        assert!(node.lease_valid(sent + duration - ONE_TICK));
        assert!(!node.lease_valid(sent + duration));
        assert!(!node.lease_valid(sent + node.config.election_timeout_min));
    }

    #[test]
    fn a_late_answer_does_not_move_the_anchor() {
        let mut node = leader();
        let first = send_round(&mut node);
        let first_sent = node.lease.sent_at(first).expect("the round is pending");
        let second = send_round(&mut node);
        let second_sent = first_sent + Duration::from_millis(40);
        node.lease.set_sent_at(second, second_sent);

        // The answer to the later round arrives, then a late answer to the
        // earlier one. Neither moves the anchor past the later send time.
        let last = node.last_log_index();
        answer(&mut node, 2, second, last);
        answer(&mut node, 2, first, last);
        assert_eq!(node.lease.anchor, Some(second_sent));
    }

    #[test]
    fn there_is_no_lease_before_the_noop_commits() {
        let mut node = leader();
        let round = send_round(&mut node);
        let sent = node.lease.sent_at(round).expect("the round is pending");
        // Peer 2 answers but has not stored the no-op yet.
        answer(&mut node, 2, round, 0);
        assert_eq!(node.lease.anchor, Some(sent), "a quorum answered the round");
        assert!(
            !node.lease_valid(sent),
            "the commit index may predate an earlier leader's commits"
        );
        assert_eq!(node.lease_read_index(sent), None);

        let round = send_round(&mut node);
        let sent = node.lease.sent_at(round).expect("the round is pending");
        let last = node.last_log_index();
        answer(&mut node, 2, round, last);
        assert_eq!(node.lease_read_index(sent), Some(node.commit_index()));
    }

    #[test]
    fn a_transfer_revokes_the_lease_for_the_term() {
        let (mut node, _) = leased();
        node.transfer_leadership(2).expect("peer 2 is a voter");
        let round = send_round(&mut node);
        let sent = node.lease.sent_at(round).expect("the round is pending");
        let last = node.last_log_index();
        answer(&mut node, 2, round, last);
        assert!(!node.lease_valid(sent));

        // The transfer aborts. Its campaign can still be in flight.
        node.transfer_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();
        assert!(!node.leadership_transfer_in_progress());
        let round = send_round(&mut node);
        let sent = node.lease.sent_at(round).expect("the round is pending");
        let last = node.last_log_index();
        answer(&mut node, 2, round, last);
        assert!(!node.lease_valid(sent));
    }

    #[test]
    fn an_answer_to_an_earlier_term_does_not_count() {
        let mut node = leader();
        let stale = send_round(&mut node);
        // Lose and regain leadership: a later term.
        node.handle_append_entries_response(
            3,
            &AppendEntriesResponse {
                term: node.current_term() + 1,
                success: false,
                last_log_index: 0,
                round: stale,
                replica: crate::state::ReplicaState::Whole,
            },
        );
        assert_eq!(node.role(), NodeRole::Follower);
        force_election(&mut node);
        let _ = node.take_ready();
        node.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: node.current_term(),
                vote_granted: true,
            },
        );
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();

        let last = node.last_log_index();
        answer(&mut node, 2, stale, last);
        assert!(node.lease.anchor.is_none());
    }

    #[test]
    fn pending_rounds_are_capped() {
        let mut lease = LeaseState::new();
        let now = Instant::now();
        for _ in 0..MAX_PENDING_ROUNDS + 10 {
            lease.stamp(now);
        }
        assert_eq!(lease.sent.len(), MAX_PENDING_ROUNDS);
        // An evicted round settles nothing.
        assert_eq!(lease.settle(1), None);
    }

    /// Node 2 following leader 1 at term 1, with its boot fence expired so
    /// only leader contact decides.
    fn follower() -> RaftNode<MemStorage> {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        node.expire_boot_vote_fence();
        node.handle_append_entries(&AppendEntriesRequest {
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
        node
    }

    fn vote_request(transfer: bool) -> RequestVoteRequest {
        RequestVoteRequest {
            term: 2,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 1,
            transfer,
        }
    }

    /// When leader contact stops blocking votes on `node`.
    fn window_end(node: &RaftNode<MemStorage>) -> Instant {
        let contact = node.leader_contact.expect("the leader reached this node");
        (contact.at + node.config.election_timeout_min).max(node.boot_vote_fence)
    }

    #[test]
    fn a_vote_is_refused_inside_the_window() {
        let mut node = follower();
        let inside = window_end(&node) - ONE_TICK;
        let resp = node.handle_request_vote_at(&vote_request(false), inside);
        assert!(!resp.vote_granted);
        assert_eq!(node.current_term(), 1, "the term is not adopted");
        assert_eq!(resp.term, 1);
        assert_eq!(node.leader_id(), 1);
    }

    #[test]
    fn a_vote_is_granted_once_the_window_closes() {
        let mut node = follower();
        let closed = window_end(&node);
        let resp = node.handle_request_vote_at(&vote_request(false), closed);
        assert!(resp.vote_granted);
        assert_eq!(node.current_term(), 2);
    }

    #[test]
    fn a_transfer_vote_bypasses_the_window() {
        let mut node = follower();
        let inside = window_end(&node) - ONE_TICK;
        let resp = node.handle_request_vote_at(&vote_request(true), inside);
        assert!(resp.vote_granted);
        assert_eq!(node.current_term(), 2);
    }

    /// A restart forgets the leader contact. The boot fence keeps the node
    /// from voting while a lease the old leader took can still be live.
    #[test]
    fn a_restarted_follower_refuses_a_vote_until_its_boot_fence_passes() {
        let mut storage = MemStorage::new();
        storage
            .save_hard_state(&HardState {
                current_term: 1,
                voted_for: 0,
                ordinal_floor: 0,
            })
            .expect("save hard state");
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), storage);
        node.restore().expect("restore");
        assert!(
            node.leader_contact.is_none(),
            "a restart has no leader contact"
        );
        let fence = node.boot_vote_fence;

        let refused = node.handle_request_vote_at(&vote_request(false), fence - ONE_TICK);
        assert!(!refused.vote_granted);
        assert_eq!(node.current_term(), 1, "the term is not adopted");

        let granted = node.handle_request_vote_at(&vote_request(false), fence);
        assert!(granted.vote_granted);
        assert_eq!(node.current_term(), 2);
    }

    #[test]
    fn a_transfer_vote_bypasses_the_boot_fence() {
        let mut node = RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new());
        let inside = node.boot_vote_fence - ONE_TICK;
        let resp = node.handle_request_vote_at(&vote_request(true), inside);
        assert!(resp.vote_granted);
    }

    #[test]
    fn a_single_voter_holds_the_lease_alone() {
        let mut node = RaftNode::new(test_config(1, vec![]), MemStorage::new());
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);
        assert_eq!(
            node.lease_read_index(Instant::now()),
            Some(node.commit_index())
        );
    }
}
