// SPDX-License-Identifier: BUSL-1.1

//! Check-quorum: a leader that can no longer reach a majority steps down.
//!
//! A partition does not notify the leader on the minority side. Left alone it
//! keeps its role indefinitely, accepting proposals that can never commit and
//! answering leader-only queries from a log the real leader has moved past.
//! Raft's remedy is check-quorum: the leader tracks when a majority of voters
//! last acknowledged it, and demotes itself once that gap reaches an election
//! timeout — by which point any surviving majority has had time to elect a
//! successor.
//!
//! # What counts as contact
//!
//! Contact is measured from `AppendEntries` **responses**, not from replication
//! progress. Any response proves the peer is reachable and still recognises
//! this term, which is the whole question check-quorum asks. Whether the
//! peer's log has caught up is a different question. Counting `match_index`
//! instead breaks in two cases a healthy cluster routinely hits:
//!
//! - A follower rebuilding from a snapshot, or backtracking after a log
//!   conflict, answers every heartbeat while its `match_index` sits far behind.
//! - Under a sustained write burst the leader's last index outruns the acks
//!   still in flight, so even fully healthy followers trail it.
//!
//! In both cases a `match_index` test sees no contact and deposes a leader
//! whose quorum is intact.
//!
//! # When contact happened
//!
//! Contact is dated at the **arrival time** of the answers. Check-quorum is a
//! liveness check: it asks whether a quorum still answers this leader. A slow
//! but steady round trip answers it, so queueing and disk waits on the way
//! never depose a leader whose quorum keeps answering.
//!
//! The leader lease is a safety check and keeps the **send time** of the
//! request a quorum answered (see [`super::leader_lease`]). The two clocks are
//! split on purpose. Check-quorum only decides when the leader steps down. It
//! never lets the leader serve a read, so a later date grants no lease.
//!
//! Only answers to rounds stamped in the current leadership term count. An
//! answer to a request of an earlier term is never contact.

use std::time::Instant;

use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

/// What the leader knows of one voter's answers in the current term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VoterAck {
    /// The voter.
    pub(crate) peer: u64,
    /// Highest lease round the voter answered.
    pub(crate) round: u64,
    /// When the latest answer arrived.
    pub(crate) arrived: Instant,
}

impl<S: LogStorage> RaftNode<S> {
    /// Open the contact record of a new leadership term at `now`.
    ///
    /// Called on winning an election. A quorum of voters just granted their
    /// votes, which is contact by definition. The lease starts empty: a vote
    /// grant does not make a voter refuse other candidates.
    pub(super) fn arm_quorum_window(&mut self, now: Instant) {
        self.last_quorum_contact = Some(now);
        self.leader_since = Some(now);
        self.quorum_window.clear();
        self.lease.begin_term();
    }

    /// Whether voter `peer` answered this leader within the check-quorum
    /// window at `now`. A voter not yet heard this term counts from the
    /// election win. Always false off the leader path.
    pub(super) fn voter_heard_within_window(&self, peer: u64, now: Instant) -> bool {
        let last_seen = match self.voter_ack(peer) {
            Some(ack) => Some(ack.arrived),
            None => self.leader_since,
        };
        last_seen.is_some_and(|seen| {
            now.saturating_duration_since(seen) < self.config.election_timeout_max
        })
    }

    /// Move contact up to the latest instant by which a quorum of voters
    /// answered this term. No-op for any role but leader.
    ///
    /// Called from the `AppendEntries` response path, and from
    /// [`RaftNode::tick`] so a single-voter group renews on its own quorum of
    /// one. It has no peers to answer and never reaches the response path.
    pub(super) fn refresh_quorum_contact(&mut self, now: Instant) {
        if self.role != NodeRole::Leader {
            return;
        }
        // This node answers itself at once, so a quorum needs `quorum - 1`
        // peers.
        let peers_needed = self.config.quorum().saturating_sub(1);
        if peers_needed == 0 {
            self.last_quorum_contact = Some(now);
            return;
        }
        let mut arrivals: Vec<Instant> = self
            .config
            .peers
            .iter()
            .filter_map(|&peer| self.voter_ack(peer).map(|ack| ack.arrived))
            .collect();
        arrivals.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&contact) = arrivals.get(peers_needed - 1) else {
            return;
        };
        if self.last_quorum_contact.is_none_or(|last| contact > last) {
            self.last_quorum_contact = Some(contact);
        }
    }

    /// What the leader knows of `peer`'s answers in this term.
    pub(super) fn voter_ack(&self, peer: u64) -> Option<VoterAck> {
        self.quorum_window
            .iter()
            .find(|ack| ack.peer == peer)
            .copied()
    }

    /// Push the last quorum contact, and every recorded arrival, back to `at`
    /// (for testing). The lease anchor is a separate clock and stays.
    pub fn quorum_contact_at_override(&mut self, at: Instant) {
        self.last_quorum_contact = Some(at);
        for ack in &mut self.quorum_window {
            ack.arrived = ack.arrived.min(at);
        }
    }

    /// Whether the leader has gone an entire election timeout without a
    /// quorum answering. Always false off the leader path.
    ///
    /// `election_timeout_max` is deliberate: a follower starts its own
    /// election somewhere in `[min, max]`, so waiting for the upper bound
    /// means a leader only steps down once every follower has had the chance
    /// to move on without it. Stepping down at `min` would demote leaders
    /// during ordinary jitter.
    pub(super) fn quorum_contact_lost(&self, now: Instant) -> bool {
        if self.role != NodeRole::Leader {
            return false;
        }
        self.last_quorum_contact
            .is_some_and(|last| now.duration_since(last) >= self.config.election_timeout_max)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::message::{AppendEntriesResponse, LogEntry, RequestVoteResponse};
    use crate::node::config::RaftConfig;
    use crate::node::core::RaftNode;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;
    use crate::test_support::{force_election, test_config};

    fn elect(cfg: RaftConfig) -> RaftNode<MemStorage> {
        let peers = cfg.peers.clone();
        let mut node = RaftNode::new(cfg, MemStorage::new());
        force_election(&mut node);
        let term = node.current_term();
        for peer in peers {
            if node.role() == NodeRole::Leader {
                break;
            }
            node.handle_request_vote_response(
                peer,
                &RequestVoteResponse {
                    term,
                    vote_granted: true,
                },
            );
        }
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();
        node
    }

    fn leader(peers: Vec<u64>) -> RaftNode<MemStorage> {
        elect(test_config(1, peers))
    }

    /// Age the contact window past the step-down threshold.
    fn go_silent(node: &mut RaftNode<MemStorage>) {
        node.quorum_contact_at_override(Instant::now() - Duration::from_secs(1));
    }

    /// The round of the latest `AppendEntries` the leader sent.
    fn latest_round(node: &RaftNode<MemStorage>) -> u64 {
        node.lease.next_round - 1
    }

    /// Peer `peer` answers `round` with success at the leader's last index.
    fn answer(node: &mut RaftNode<MemStorage>, peer: u64, round: u64) {
        node.handle_append_entries_response(
            peer,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index: node.last_log_index(),
                round,
                needs_snapshot: false,
            },
        );
    }

    /// A rejection is contact. A follower backtracking through a log conflict
    /// answers every round while its `match_index` stays put; deposing that
    /// leader would be a false positive.
    #[test]
    fn a_rejecting_follower_still_counts_as_contact() {
        let mut node = leader(vec![2, 3]);
        go_silent(&mut node);

        node.handle_append_entries_response(
            2,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: false,
                last_log_index: 0,
                round: latest_round(&node),
                needs_snapshot: false,
            },
        );

        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Leader,
            "a reachable follower that rejects must refresh contact"
        );
    }

    /// Contact must not depend on replication progress. A leader well ahead of
    /// its followers is the normal state under load, not evidence of a lost
    /// quorum.
    #[test]
    fn a_lagging_follower_still_counts_as_contact() {
        let mut node = leader(vec![2, 3]);
        for i in 0..64u8 {
            node.propose(vec![i]).unwrap();
        }
        go_silent(&mut node);

        // Peer 2 answers, acking an index far behind the leader's last.
        node.handle_append_entries_response(
            2,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index: 1,
                round: latest_round(&node),
                needs_snapshot: false,
            },
        );

        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Leader,
            "a lagging but reachable follower must refresh contact"
        );
    }

    /// The real case: nobody answers, so the leader demotes itself.
    #[test]
    fn silence_from_every_voter_steps_the_leader_down() {
        let mut node = leader(vec![2, 3]);
        go_silent(&mut node);

        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Follower,
            "no voter answered within the election timeout"
        );
    }

    /// One answer out of two peers is a minority in a three-voter group.
    #[test]
    fn a_single_answer_is_not_a_quorum_in_a_five_voter_group() {
        let mut node = leader(vec![2, 3, 4, 5]);
        go_silent(&mut node);

        node.handle_append_entries_response(
            2,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index: node.last_log_index(),
                round: latest_round(&node),
                needs_snapshot: false,
            },
        );

        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Follower,
            "self plus one of four peers is short of a quorum of three"
        );
    }

    /// A single-voter group has nobody to hear from and must never depose
    /// itself over it.
    #[test]
    fn a_single_voter_leader_never_steps_down() {
        let mut node = leader(vec![]);
        go_silent(&mut node);

        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);
    }

    /// Stepping down must leave no leader-term state behind for the next term
    /// to inherit.
    #[test]
    fn stepping_down_clears_the_contact_window() {
        let mut node = leader(vec![2, 3]);
        go_silent(&mut node);
        node.tick();

        assert_eq!(node.role(), NodeRole::Follower);
        assert!(node.last_quorum_contact.is_none());
        assert!(node.quorum_window.is_empty());
    }

    /// A demoted leader is an ordinary follower: it accepts the new leader's
    /// entries and applies them.
    #[test]
    fn a_demoted_leader_follows_the_next_one() {
        let mut node = leader(vec![2, 3]);
        go_silent(&mut node);
        node.tick();
        assert_eq!(node.role(), NodeRole::Follower);

        let term = node.current_term() + 1;
        let resp = node.handle_append_entries(&crate::message::AppendEntriesRequest {
            term,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term,
                index: 1,
                data: b"from-the-new-leader".to_vec(),
            }],
            leader_commit: 1,
            group_id: 1,
            round: 1,
            replicated_floor: 0,
        });

        assert!(resp.success);
        assert_eq!(node.leader_id(), 2);
        assert_eq!(node.current_term(), term);
        let ready = node.take_ready();
        assert_eq!(ready.committed_entries.len(), 1);
        assert_eq!(ready.committed_entries[0].data, b"from-the-new-leader");
    }

    /// Contact is a sliding window, not a one-off: answers must keep arriving.
    /// A peer that answered once and then went quiet does not hold the term
    /// open forever.
    #[test]
    fn contact_must_be_renewed_by_later_answers() {
        let mut node = leader(vec![2, 3]);

        node.handle_append_entries_response(
            2,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index: node.last_log_index(),
                round: latest_round(&node),
                needs_snapshot: false,
            },
        );
        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);

        // Nothing further arrives.
        go_silent(&mut node);
        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Follower,
            "a stale answer must not renew the window indefinitely"
        );
    }

    /// Contact is dated when the answer arrives. The lease keeps the send
    /// time of the answered request.
    #[test]
    fn contact_is_dated_at_arrival_time() {
        let mut node = leader(vec![2, 3]);
        go_silent(&mut node);
        let round = latest_round(&node);
        let silent_since = node.last_quorum_contact.expect("leader has contact");
        // The answered request left before the silence began.
        let sent = silent_since - Duration::from_millis(100);
        node.lease.set_sent_at(round, sent);

        let before = Instant::now();
        answer(&mut node, 2, round);

        let contact = node.last_quorum_contact.expect("leader has contact");
        assert!(
            contact >= before,
            "contact must be the arrival time, not the send time"
        );
        assert_eq!(
            node.lease.anchor,
            Some(sent),
            "the lease keeps the send time"
        );
    }

    /// A quorum that answers every round, each answer arriving long after its
    /// request left, keeps the leader. The lease stays anchored at the send
    /// times and gives no reads.
    #[test]
    fn a_steady_slow_quorum_keeps_the_leader() {
        let mut node = leader(vec![2, 3]);
        let round_trip = node.config.election_timeout_max + Duration::from_millis(200);
        for _ in 0..4 {
            node.replicate_to_all();
            let _ = node.take_ready();
            let round = latest_round(&node);
            let sent = Instant::now() - round_trip;
            node.lease.set_sent_at(round, sent);
            // Everything before this round is older than the step-down bound.
            go_silent(&mut node);

            answer(&mut node, 2, round);
            node.tick();

            assert_eq!(
                node.role(),
                NodeRole::Leader,
                "a quorum that keeps answering must keep the leader"
            );
            assert_eq!(node.lease.anchor, Some(sent));
            assert!(
                !node.lease_valid(Instant::now()),
                "a send time older than the lease gives no lease"
            );
        }
    }

    /// An answer to a round of an earlier leadership term is no contact, even
    /// when it carries the current term.
    #[test]
    fn an_answer_to_an_earlier_term_is_not_contact() {
        let mut node = leader(vec![2, 3]);
        let stale = latest_round(&node);
        node.handle_append_entries_response(
            3,
            &AppendEntriesResponse {
                term: node.current_term() + 1,
                success: false,
                last_log_index: 0,
                round: stale,
                needs_snapshot: false,
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
        go_silent(&mut node);

        answer(&mut node, 2, stale);
        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Follower,
            "an answer to an earlier term must not renew contact"
        );
    }

    /// A follower never runs the leader-side check.
    #[test]
    fn a_follower_is_never_deposed_by_check_quorum() {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        assert_eq!(node.role(), NodeRole::Follower);
        assert!(!node.quorum_contact_lost(std::time::Instant::now()));
        node.refresh_quorum_contact(std::time::Instant::now());
        assert!(node.last_quorum_contact.is_none());
    }

    /// Answers from a learner carry no weight: learners are not voters and
    /// cannot keep a leader's term alive.
    #[test]
    fn a_learner_answer_does_not_count_toward_quorum() {
        let mut cfg = test_config(1, vec![2, 3]);
        cfg.learners = vec![9];
        let mut node = elect(cfg);
        go_silent(&mut node);

        node.handle_append_entries_response(
            9,
            &AppendEntriesResponse {
                term: node.current_term(),
                success: true,
                last_log_index: node.last_log_index(),
                round: latest_round(&node),
                needs_snapshot: false,
            },
        );

        node.tick();
        assert_eq!(
            node.role(),
            NodeRole::Follower,
            "only voters can renew the contact window"
        );
    }

    /// The step-down waits for the upper bound of the election timeout, so
    /// ordinary jitter below it does not demote a healthy leader.
    #[test]
    fn contact_is_not_lost_before_the_upper_election_bound() {
        let node = leader(vec![2, 3]);
        let contact = node.last_quorum_contact.expect("leader has contact");
        let cfg_max = node.config.election_timeout_max;
        assert!(!node.quorum_contact_lost(contact + cfg_max - Duration::from_nanos(1)));
        assert!(node.quorum_contact_lost(contact + cfg_max));
    }
}
