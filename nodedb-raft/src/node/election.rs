// SPDX-License-Identifier: BUSL-1.1

//! Election entry points: the pre-vote round and the vote round. A replica
//! whose data diverged starts neither.

use std::collections::HashSet;

use tracing::{debug, info};

use crate::message::{PreVoteRequest, RequestVoteRequest};
use crate::state::{NodeRole, PreVoteRound};
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
        // A replica whose data diverged cannot lead.
        if !self.replica.may_lead() {
            return;
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
        // A replica whose data diverged cannot lead, so it never asks for
        // votes, and a single voter never elects itself.
        if !self.replica.may_lead() {
            return;
        }

        self.pre_vote = None;
        self.hard_state.current_term += 1;
        self.role = NodeRole::Candidate;
        self.publish_leadership();
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
}
