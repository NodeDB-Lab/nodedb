// SPDX-License-Identifier: BUSL-1.1

//! Internal state transitions and replication logic.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use rand::RngExt;
use tracing::{debug, info};

use crate::error::{RaftError, Result};
use crate::message::{AppendEntriesRequest, LogEntry, PreVoteRequest, RequestVoteRequest};
use crate::state::{LeaderState, NodeRole, PreVoteRound};
use crate::storage::LogStorage;

use super::core::RaftNode;

impl<S: LogStorage> RaftNode<S> {
    /// Ask peers whether they WOULD vote for this node at `current_term + 1`.
    ///
    /// Touches neither `current_term` nor `voted_for` and persists nothing, so
    /// a node cut off from the cluster can retry indefinitely without inflating
    /// its term and deposing the healthy leader when the partition heals.
    pub(super) fn start_pre_election(&mut self) {
        match self.role {
            NodeRole::Learner | NodeRole::Observer => return,
            NodeRole::Follower | NodeRole::Candidate | NodeRole::Leader => {}
        }

        // Single voter: there is nobody to probe, and the existing immediate
        // win must be preserved.
        if self.config.peers.is_empty() {
            self.start_election();
            return;
        }

        // A round that wins nothing retries on the usual randomized backoff.
        self.reset_election_timeout();

        let term = self.hard_state.current_term + 1;
        self.pre_vote = Some(PreVoteRound {
            term,
            granted: HashSet::new(),
        });

        debug!(
            node = self.config.node_id,
            group = self.config.group_id,
            term,
            "starting pre-vote round"
        );

        for &peer in &self.config.peers {
            self.ready.pre_vote_requests.push((
                peer,
                PreVoteRequest {
                    term,
                    candidate_id: self.config.node_id,
                    last_log_index: self.log.last_index(),
                    last_log_term: self.log.last_term(),
                    group_id: self.config.group_id,
                },
            ));
        }
    }

    pub(super) fn start_election(&mut self) {
        // Learners and observers never stand for election — defensive check
        // in case `tick()` is bypassed (e.g., tests forcing a deadline).
        match self.role {
            NodeRole::Learner | NodeRole::Observer => return,
            NodeRole::Follower | NodeRole::Candidate | NodeRole::Leader => {}
        }

        self.pre_vote = None;
        self.hard_state.current_term += 1;
        self.role = NodeRole::Candidate;
        self.hard_state.voted_for = self.config.node_id;
        self.votes_received.clear();
        self.leader_id = 0;

        self.persist_hard_state();
        self.reset_election_timeout();

        info!(
            node = self.config.node_id,
            group = self.config.group_id,
            term = self.hard_state.current_term,
            "starting election"
        );

        // Single-voter cluster: win immediately. (Learners in the group do
        // not count — single voter + N learners still elects the single
        // voter as leader.)
        if self.config.peers.is_empty() {
            self.become_leader();
            return;
        }

        for &peer in &self.config.peers {
            self.ready.vote_requests.push((
                peer,
                RequestVoteRequest {
                    term: self.hard_state.current_term,
                    candidate_id: self.config.node_id,
                    last_log_index: self.log.last_index(),
                    last_log_term: self.log.last_term(),
                    group_id: self.config.group_id,
                    transfer: false,
                },
            ));
        }
    }

    /// Step down to follower (or keep learner/observer role if the node is one).
    ///
    /// Learners and observers that receive an `AppendEntries` with a higher
    /// term update their term but do not transition to `Follower` — they stay
    /// in their non-election role so the tick loop continues to skip timeouts.
    pub(super) fn become_follower(&mut self, term: u64) {
        let was_leader = self.role == NodeRole::Leader;
        match self.role {
            NodeRole::Learner | NodeRole::Observer => {
                // Preserve the non-election role.
            }
            NodeRole::Follower | NodeRole::Candidate | NodeRole::Leader => {
                self.role = NodeRole::Follower;
            }
        }
        // A vote binds for its whole term: a node that steps down within the
        // term it voted in never votes in that term again.
        if term > self.hard_state.current_term {
            self.hard_state.voted_for = 0;
        }
        self.hard_state.current_term = term;
        // The leader of this term is unknown until it reaches this node. A
        // caller that knows it (an AppendEntries or InstallSnapshot sender)
        // sets it right after. So `leader_id`, when non-zero, always leads
        // `current_term`: a stepped-down leader, or a follower that adopted a
        // candidate's higher term, never names a leader of an older term.
        self.leader_id = 0;
        self.leader_state = None;
        self.votes_received.clear();
        // Any in-progress leadership transfer is moot once we step down.
        self.leadership_transfer = None;
        self.pre_vote = None;
        // The contact window belongs to a leader term; it means nothing here.
        self.last_quorum_contact = None;
        self.quorum_window.clear();
        self.leader_since = None;
        self.term_start_index = None;
        self.persist_hard_state();
        self.reset_election_timeout();

        if was_leader {
            info!(
                node = self.config.node_id,
                group = self.config.group_id,
                term,
                "stepped down from leader"
            );
        }
    }

    pub(super) fn become_leader(&mut self) {
        self.role = NodeRole::Leader;
        self.leader_id = self.config.node_id;

        // Leader tracks voter peers, learner peers, and observer peers for
        // replication. Only voters count toward the commit quorum (see
        // `try_advance_commit_index`). Learners still receive entries so they
        // can catch up for promotion. Observers receive entries and ack
        // advisorily but are never counted in quorum.
        let mut ls = LeaderState::new(
            &self.config.peers,
            &self.config.observers,
            self.log.last_index(),
        );
        for &learner in &self.config.learners {
            ls.add_peer(learner, self.log.last_index());
        }
        self.leader_state = Some(ls);
        // Winning the election required a quorum of votes, so the contact
        // window opens proven rather than empty.
        self.arm_quorum_window(std::time::Instant::now());

        info!(
            node = self.config.node_id,
            group = self.config.group_id,
            term = self.hard_state.current_term,
            voters = self.config.peers.len(),
            learners = self.config.learners.len(),
            "became leader"
        );

        // Raft paper §5.4.2: leader appends a no-op entry.
        let noop = LogEntry {
            term: self.hard_state.current_term,
            index: self.log.last_index() + 1,
            data: Vec::new(),
        };
        let noop_index = noop.index;
        let _ = self.log.append(noop);
        self.term_start_index = Some(noop_index);

        // Single-voter cluster: the no-op commits once it is durable here.
        if self.config.cluster_size() == 1 {
            self.try_advance_commit_index();
        }

        self.replicate_to_all();
    }

    /// Send `AppendEntries` to every tracked peer (voters + learners + observers).
    ///
    /// Observers are skipped when their advisory send queue is full — source
    /// commits are never gated on observer apply pace.
    pub(super) fn replicate_to_all(&mut self) {
        let voters_and_learners: Vec<u64> = self
            .config
            .peers
            .iter()
            .chain(self.config.learners.iter())
            .copied()
            .collect();
        for peer in voters_and_learners {
            self.send_append_entries(peer);
        }

        let observers: Vec<u64> = self.config.observers.clone();
        for observer in observers {
            self.send_append_entries_to_observer(observer);
        }
    }

    pub(super) fn send_append_entries(&mut self, peer: u64) {
        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        let next_index = leader.next_index_for(peer);
        let prev_log_index = next_index.saturating_sub(1);

        let prev_log_term = match self.log.term_at(prev_log_index) {
            Some(term) => term,
            None => {
                debug!(
                    node = self.config.node_id,
                    group = self.config.group_id,
                    peer,
                    next_index,
                    snapshot_index = self.log.snapshot_index(),
                    "peer needs snapshot (log compacted)"
                );
                self.ready.snapshots_needed.push(peer);
                return;
            }
        };

        let entries = if next_index <= self.log.last_index() {
            match self.log.entries_within(
                next_index,
                self.log.last_index(),
                crate::log::MAX_APPEND_BATCH_BYTES,
            ) {
                Ok(slice) => slice.to_vec(),
                Err(RaftError::LogCompacted { .. }) => {
                    debug!(
                        node = self.config.node_id,
                        group = self.config.group_id,
                        peer,
                        next_index,
                        "peer needs snapshot (entries compacted)"
                    );
                    self.ready.snapshots_needed.push(peer);
                    return;
                }
                Err(_) => vec![],
            }
        } else {
            vec![]
        };

        let round = self.stamp_lease_round();
        self.ready.messages.push((
            peer,
            AppendEntriesRequest {
                term: self.hard_state.current_term,
                leader_id: self.config.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.volatile.commit_index,
                group_id: self.config.group_id,
                round,
                replicated_floor: self.replicated_floor(),
            },
        ));
    }

    /// Send `AppendEntries` to an observer peer.
    ///
    /// If the observer's advisory send queue is full (backpressure threshold
    /// reached), the send is skipped. The observer will fall behind and recover
    /// via snapshot when it reconnects. Source commits are never delayed.
    pub(super) fn send_append_entries_to_observer(&mut self, observer: u64) {
        let can_receive = match &self.leader_state {
            Some(ls) => ls.observer_can_receive(observer),
            None => return,
        };
        if !can_receive {
            debug!(
                node = self.config.node_id,
                group = self.config.group_id,
                observer,
                "observer send queue full; skipping (advisory backpressure)"
            );
            return;
        }

        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        let obs_state = match leader
            .observer_states
            .iter()
            .find(|(id, _)| *id == observer)
        {
            Some((_, s)) => s.clone(),
            None => return,
        };

        let next_index = obs_state.next_index;
        let prev_log_index = next_index.saturating_sub(1);

        let prev_log_term = match self.log.term_at(prev_log_index) {
            Some(term) => term,
            None => {
                debug!(
                    node = self.config.node_id,
                    group = self.config.group_id,
                    observer,
                    next_index,
                    snapshot_index = self.log.snapshot_index(),
                    "observer needs snapshot (log compacted)"
                );
                self.ready.snapshots_needed.push(observer);
                return;
            }
        };

        let entries = if next_index <= self.log.last_index() {
            match self.log.entries_within(
                next_index,
                self.log.last_index(),
                crate::log::MAX_APPEND_BATCH_BYTES,
            ) {
                Ok(slice) => slice.to_vec(),
                Err(crate::error::RaftError::LogCompacted { .. }) => {
                    debug!(
                        node = self.config.node_id,
                        group = self.config.group_id,
                        observer,
                        next_index,
                        "observer needs snapshot (entries compacted)"
                    );
                    self.ready.snapshots_needed.push(observer);
                    return;
                }
                Err(_) => vec![],
            }
        } else {
            vec![]
        };

        let entry_count = entries.len() as u32;

        self.ready.messages.push((
            observer,
            crate::message::AppendEntriesRequest {
                term: self.hard_state.current_term,
                leader_id: self.config.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.volatile.commit_index,
                group_id: self.config.group_id,
                round: super::leader_lease::UNTRACKED_ROUND,
                replicated_floor: self.replicated_floor(),
            },
        ));

        // Increment pending count for advisory backpressure tracking.
        if let Some(ls) = self.leader_state.as_mut()
            && let Some(state) = ls.observer_state_mut(observer)
        {
            state.pending_count = state.pending_count.saturating_add(entry_count.max(1));
        }
    }

    /// Try to advance `commit_index` based on the voter quorum only.
    ///
    /// Learners' `match_index` is tracked (so we know when they are caught
    /// up for promotion) but intentionally excluded from this calculation
    /// so adding a learner never weakens the commit quorum.
    pub(super) fn try_advance_commit_index(&mut self) {
        let leader = match &self.leader_state {
            Some(ls) => ls,
            None => return,
        };

        let last = self.log.last_index();
        // This node acknowledges an entry once its storage holds it durably,
        // as every follower does before it answers.
        let self_stable = self.log.stable_index();
        for n in (self.volatile.commit_index + 1..=last).rev() {
            let term_at_n = match self.log.term_at(n) {
                Some(t) => t,
                None => continue,
            };

            if term_at_n != self.hard_state.current_term {
                continue;
            }

            let mut count = u64::from(self_stable >= n);
            for &peer in &self.config.peers {
                if leader.match_index_for(peer) >= n {
                    count += 1;
                }
            }

            if count as usize >= self.config.quorum() {
                self.volatile.commit_index = n;
                self.collect_committed_entries();
                break;
            }
        }
    }

    /// Queue newly committed entries into `Ready`. A range the log no longer
    /// holds lands in `Ready::committed_read_error` for the driver to surface.
    pub(super) fn collect_committed_entries(&mut self) {
        if let Err(e) = self.queue_committed_entries() {
            self.ready.committed_read_error = Some(e);
        }
    }

    /// Queue the committed range past everything applied or already queued.
    ///
    /// Returns [`RaftError::LogCompacted`] when that range starts below
    /// `first_available_index`: the state machine lacks entries only a
    /// snapshot can supply, so delivery halts rather than skip them.
    fn queue_committed_entries(&mut self) -> Result<()> {
        // Resume from the furthest index already queued into `Ready`, not only
        // from `last_applied`. This runs on every commit-index advance — a
        // follower's AppendEntries, a single voter's propose — while the loop
        // drains `Ready` and advances `last_applied` later. Two advances inside
        // one window would queue the same committed range twice, and the
        // applier then receives the same committed index twice in one batch.
        let queued_through = self
            .ready
            .committed_entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(0);
        let from = self.volatile.last_applied.max(queued_through) + 1;
        let to = self.volatile.commit_index;
        if from > to {
            return Ok(());
        }
        let entries = self.log.entries_range(from, to)?;
        self.ready.committed_entries.extend(entries.iter().cloned());
        Ok(())
    }

    pub(super) fn persist_hard_state(&mut self) {
        self.ready.hard_state = Some(self.hard_state.clone());
    }

    pub(super) fn reset_election_timeout(&mut self) {
        let mut rng = rand::rng();
        let min = self.config.election_timeout_min.as_millis() as u64;
        let max = self.config.election_timeout_max.as_millis() as u64;
        let timeout = Duration::from_millis(rng.random_range(min..=max));
        self.election_deadline = Instant::now() + timeout;
    }
}

#[cfg(test)]
mod tests {
    use crate::error::RaftError;
    use crate::node::core::RaftNode;
    use crate::storage::MemStorage;
    use crate::test_support::{
        apply_durably, force_election, leader_with_applied_noop, test_config,
    };

    /// Two commit advances inside one `Ready` window queue each index once:
    /// the second collect resumes past what the first already queued.
    #[test]
    fn repeated_collection_queues_each_index_once() {
        let mut node = RaftNode::new(test_config(1, vec![]), MemStorage::new());
        force_election(&mut node);
        let election = node.take_ready();
        if let Some(last) = election.committed_entries.last() {
            node.advance_applied(last.index);
        }

        node.propose(b"one".to_vec())
            .expect("single voter commits immediately");
        node.propose(b"two".to_vec())
            .expect("single voter commits immediately");

        let ready = node.take_ready();
        let indices: Vec<u64> = ready
            .committed_entries
            .iter()
            .map(|entry| entry.index)
            .collect();
        assert_eq!(indices.len(), 2, "one entry per propose: {indices:?}");
        assert!(
            indices.windows(2).all(|pair| pair[0] < pair[1]),
            "queued indices must be strictly increasing: {indices:?}"
        );
    }

    /// A leader names the index of the no-op it appended when it won its
    /// term. Later proposals leave it unchanged, and a step-down clears it.
    #[test]
    fn a_leader_names_its_term_start_index() {
        let mut node = RaftNode::new(test_config(1, vec![]), MemStorage::new());
        assert_eq!(node.term_start_index(), None, "a follower names none");
        force_election(&mut node);
        let noop = node.last_log_index();
        assert_eq!(node.term_start_index(), Some(noop));

        node.propose(b"write".to_vec())
            .expect("single voter commits immediately");
        assert_eq!(node.term_start_index(), Some(noop));

        let term = node.current_term();
        node.become_follower(term + 1);
        assert_eq!(node.term_start_index(), None);
    }

    /// A committed range below the log's first available index is a hard
    /// error in `Ready`, never a silently empty delivery.
    #[test]
    fn compacted_committed_range_surfaces_error() {
        let mut node = leader_with_applied_noop(test_config(1, vec![]));
        for _ in 0..3 {
            let idx = node
                .propose(b"write".to_vec())
                .expect("single voter commits");
            let _ = node.take_ready();
            apply_durably(&mut node, idx);
        }
        let snap = node.last_applied();
        assert!(
            node.compact_log_up_to(snap)
                .expect("durable prefix compacts")
        );

        // Delivery watermark behind the compacted prefix.
        node.volatile.last_applied = 1;
        node.propose(b"next".to_vec())
            .expect("single voter commits");

        let ready = node.take_ready();
        assert!(ready.committed_entries.is_empty());
        match ready.committed_read_error {
            Some(RaftError::LogCompacted {
                requested,
                first_available,
            }) => {
                assert_eq!(requested, 2);
                assert_eq!(first_available, snap + 1);
            }
            other => panic!("expected LogCompacted, got {other:?}"),
        }
    }
}
