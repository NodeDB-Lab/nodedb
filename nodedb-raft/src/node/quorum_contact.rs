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
//! this term, which is the whole question check-quorum asks; whether the peer's
//! log has caught up is a different one. Counting `match_index` instead would
//! break in both directions a healthy cluster routinely hits:
//!
//! - A follower rebuilding from a snapshot, or backtracking after a log
//!   conflict, answers every heartbeat while its `match_index` sits far behind.
//! - Under a sustained write burst the leader's last index outruns the acks
//!   still in flight, so even fully healthy followers trail it.
//!
//! In both cases a `match_index` test sees no contact and deposes a leader
//! whose quorum is intact. Responses are therefore counted through
//! [`LeaderState::ack_count_for`], the same monotonic per-peer counter
//! [`super::read_index`] uses, bumped on success and rejection alike.
//!
//! [`LeaderState::ack_count_for`]: crate::state::LeaderState::ack_count_for

use std::time::Instant;

use crate::node::core::RaftNode;
use crate::state::NodeRole;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// Start a fresh contact window at `now`, treating this instant as proven
    /// contact.
    ///
    /// Called on winning an election — a quorum of voters just granted their
    /// votes, which is contact by definition — and again each time a new
    /// quorum is observed.
    pub(super) fn arm_quorum_window(&mut self, now: Instant) {
        self.last_quorum_contact = Some(now);
        self.quorum_window = self
            .leader_state
            .as_ref()
            .map(|ls| {
                self.config
                    .peers
                    .iter()
                    .map(|&id| (id, ls.ack_count_for(id)))
                    .collect()
            })
            .unwrap_or_default();
    }

    /// Re-arm the contact window if a quorum of voters has answered since it
    /// opened. No-op for any role but leader.
    ///
    /// Called from the `AppendEntries` response path so contact tracks the
    /// responses themselves, and from [`RaftNode::tick`] so a single-voter
    /// group — which has no peers to answer and so never reaches the response
    /// path — still refreshes on its own quorum of one.
    pub(super) fn refresh_quorum_contact(&mut self, now: Instant) {
        if self.role != NodeRole::Leader {
            return;
        }
        let Some(leader) = self.leader_state.as_ref() else {
            return;
        };
        // Self is always in contact with itself.
        let mut count = 1usize;
        for &peer in &self.config.peers {
            let baseline = self
                .quorum_window
                .iter()
                .find(|&&(id, _)| id == peer)
                .map(|&(_, seen)| seen)
                .unwrap_or(0);
            if leader.ack_count_for(peer) > baseline {
                count += 1;
            }
        }
        if count >= self.config.quorum() {
            self.arm_quorum_window(now);
        }
    }

    /// Push the contact window back to `at` (for testing).
    pub fn quorum_contact_at_override(&mut self, at: Instant) {
        self.last_quorum_contact = Some(at);
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

    /// The commit index this node may serve a linearizable read at while its
    /// quorum-contact lease holds, or `None` when the lease is not live.
    ///
    /// The lease is the check-quorum window read the other way round: a
    /// majority acknowledged this leader at `last_quorum_contact`, and any
    /// successor needs a majority too, so no other node can have won an
    /// election that moved the log past this commit index before an election
    /// timeout has elapsed. Inside that window a linearizable read needs no
    /// ReadIndex round — the round exists to prove the same fact.
    ///
    /// Both instants are this node's monotonic clock, so unlike an
    /// `expires_at` stamped by another node there is no clock-skew bound to
    /// apply. The window closes at the same threshold
    /// [`Self::quorum_contact_lost`] demotes at, so a lease-answered read
    /// never outlives leader state.
    pub fn leader_lease_index(&self, now: Instant) -> Option<u64> {
        if self.role != NodeRole::Leader {
            return None;
        }
        let last = self.last_quorum_contact?;
        if now.duration_since(last) >= self.config.election_timeout_max {
            return None;
        }
        Some(self.commit_index())
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

    /// Inside the contact window a leader may serve a linearizable read from
    /// its own commit index; the lease lapses at the same threshold that
    /// deposes it, so a lease-answered read never outlives leader state.
    #[test]
    fn a_leader_holds_a_lease_only_inside_the_contact_window() {
        let mut node = leader(vec![2, 3]);
        assert!(
            node.leader_lease_index(Instant::now()).is_some(),
            "a freshly elected leader has just proven quorum contact"
        );

        go_silent(&mut node);
        assert!(
            node.leader_lease_index(Instant::now()).is_none(),
            "the lease must lapse at the step-down threshold"
        );
    }

    /// No lease off the leader path.
    #[test]
    fn a_follower_holds_no_lease() {
        let node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        assert!(node.leader_lease_index(Instant::now()).is_none());
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
        let cfg_max = Duration::from_millis(300);
        let just_inside = std::time::Instant::now() + cfg_max - Duration::from_millis(10);
        assert!(!node.quorum_contact_lost(just_inside));
        let past = std::time::Instant::now() + cfg_max + Duration::from_millis(10);
        assert!(node.quorum_contact_lost(past));
    }
}
