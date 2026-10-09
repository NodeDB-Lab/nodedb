// SPDX-License-Identifier: BUSL-1.1

//! `RaftNode` struct, constructors, simple accessors, `tick`, and `propose`.
//!
//! Applied-index durability and log compaction live in
//! [`super::durability`]. Check-quorum contact tracking lives in
//! [`super::quorum_contact`]. Membership mutation (add/remove voter,
//! add/remove/promote learner) lives in [`super::membership`]. State transitions (election, `become_leader`,
//! replication) live in [`super::internal`]. RPC handlers live in
//! [`super::rpc`].

use std::collections::HashSet;
use std::time::Instant;

use crate::error::{RaftError, Result};
use crate::log::RaftLog;
use crate::message::{AppendEntriesRequest, LogEntry, PreVoteRequest, TimeoutNowRequest};
use crate::state::{
    HardState, LeaderState, LeadershipTransfer, NodeRole, PreVoteRound, VolatileState,
};
use crate::storage::LogStorage;

use super::config::RaftConfig;
use super::leader_lease::LeaseState;
use tracing::info;

/// Output actions produced by a tick or RPC handler.
///
/// The caller (Multi-Raft coordinator) is responsible for executing these
/// via the transport and applying committed entries to the state machine.
#[derive(Debug, Default)]
pub struct Ready {
    /// Hard state to persist (if changed).
    pub hard_state: Option<HardState>,
    /// Entries to send to specific peers (peer_id, request).
    pub messages: Vec<(u64, AppendEntriesRequest)>,
    /// Vote requests to send (peer_id, request).
    pub vote_requests: Vec<(u64, crate::message::RequestVoteRequest)>,
    /// Pre-vote probes to send (peer_id, request).
    pub pre_vote_requests: Vec<(u64, PreVoteRequest)>,
    /// `TimeoutNow` triggers to send to leadership-transfer targets
    /// (dest_node_id, request). Drained and dispatched by the caller; until a
    /// caller wires the transport this field is simply ignored.
    pub timeout_now: Vec<(u64, TimeoutNowRequest)>,
    /// Newly committed entries to apply to the state machine.
    pub committed_entries: Vec<LogEntry>,
    /// Peers that need an InstallSnapshot RPC because their next_index
    /// falls behind the leader's snapshot_index (log compacted).
    pub snapshots_needed: Vec<u64>,
    /// Set when the next committed range to deliver is no longer in the log.
    /// Delivery for the group halts until a snapshot covers the gap.
    pub committed_read_error: Option<RaftError>,
}

impl Ready {
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.messages.is_empty()
            && self.vote_requests.is_empty()
            && self.pre_vote_requests.is_empty()
            && self.timeout_now.is_empty()
            && self.committed_entries.is_empty()
            && self.snapshots_needed.is_empty()
            && self.committed_read_error.is_none()
    }
}

/// A single Raft group's state machine.
///
/// This is a deterministic, event-driven core. It does NOT own any threads
/// or timers — the caller drives it via `tick()` and RPC handler methods,
/// and reads output via `take_ready()`.
pub struct RaftNode<S: LogStorage> {
    pub(super) config: RaftConfig,
    pub(super) role: NodeRole,
    pub(super) hard_state: HardState,
    pub(super) volatile: VolatileState,
    pub(super) leader_state: Option<LeaderState>,
    pub(super) log: RaftLog<S>,
    /// When the next election timeout fires.
    pub(super) election_deadline: Instant,
    /// When the next heartbeat should be sent (leader only).
    pub(super) heartbeat_deadline: Instant,
    /// Votes received in current election.
    pub(super) votes_received: HashSet<u64>,
    /// Pending ready output.
    pub(super) ready: Ready,
    /// Known leader ID (0 = unknown).
    pub(super) leader_id: u64,
    /// In-progress leadership transfer, if any (leader-side, volatile).
    pub(super) leadership_transfer: Option<LeadershipTransfer>,
    /// In-flight pre-vote round, if any. `None` outside a round.
    pub(super) pre_vote: Option<PreVoteRound>,
    /// Highest log index whose apply is durable on this node, mirroring
    /// `LogStorage::save_applied_index`.
    ///
    /// Deliberately distinct from `volatile.last_applied`, which advances the
    /// moment an entry is DELIVERED to the state machine. This index only
    /// advances once that entry's effects are durable, which makes it two
    /// things `last_applied` cannot be: the floor a restart resumes delivery
    /// from, and the ceiling compaction may discard up to.
    pub(super) durable_applied: u64,
    /// What the leader had committed when it last reached this node, and
    /// when that was. `None` until a leader makes contact.
    ///
    /// This is the follower's only honest measure of how far behind it is.
    /// Time since the last local apply cannot answer it: a follower that
    /// applies steadily while thousands of entries behind looks fresh by
    /// that measure, and a fully caught-up follower in an idle cluster looks
    /// stale. Heartbeats refresh this even when nothing is being written.
    pub(super) leader_contact: Option<LeaderContact>,
    /// Latest instant by which a quorum of voters answered this term, or the
    /// election win before any. `None` off the leader path. Drives
    /// check-quorum step-down (see [`super::quorum_contact`]).
    pub(super) last_quorum_contact: Option<Instant>,
    /// Per-voter highest lease round acknowledged in the current term, and
    /// when the latest answer arrived. Cleared on step-down.
    pub(super) quorum_window: Vec<super::quorum_contact::VoterAck>,
    /// When this node won its current leadership term. `None` off the
    /// leader path. A voter not yet heard this term counts as last seen here.
    pub(super) leader_since: Option<Instant>,
    /// Leader-lease rounds and anchor (see [`super::leader_lease`]).
    pub(super) lease: LeaseState,
    /// Until this instant the node refuses every vote but a transfer vote.
    ///
    /// Set to `election_timeout_max` after construction. A restarted node has
    /// forgotten its leader contact, and a lease the leader took before the
    /// restart can still be live. Kept apart from `leader_contact`, which
    /// also feeds the staleness bound: a boot is no contact with any leader.
    pub(super) boot_vote_fence: Instant,
    /// An outside bound on compaction: the log never discards an entry above
    /// it. An archiver that must copy entries before they go raises it.
    pub(super) compaction_ceiling: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    /// Whether this follower holds no state it can resume the log from. It
    /// then refuses every `AppendEntries` with `needs_snapshot`, and its
    /// leader sends a snapshot instead. The driver sets and clears it.
    pub(super) snapshot_required: bool,
    /// The highest `replicated_floor` a leader sent this node, or this
    /// node's own as leader. See [`Self::replicated_floor`].
    pub(super) replicated_floor: u64,
}

/// A leader's commit index as of its last contact with this node.
#[derive(Debug, Clone, Copy)]
pub struct LeaderContact {
    /// The leader's `commit_index` at that moment.
    pub leader_commit: u64,
    /// When the contact arrived.
    pub at: Instant,
}

impl<S: LogStorage> RaftNode<S> {
    /// Create a new Raft node. Call `restore()` before ticking.
    ///
    /// If `config.starts_as_learner` is `true`, the node boots in the
    /// `Learner` role and will never run an election timeout or become a
    /// leader until it is promoted via `promote_self_to_voter`.
    pub fn new(config: RaftConfig, storage: S) -> Self {
        let now = Instant::now();
        let role = if config.starts_as_observer {
            NodeRole::Observer
        } else if config.starts_as_learner {
            NodeRole::Learner
        } else {
            NodeRole::Follower
        };
        Self {
            log: RaftLog::new(storage),
            role,
            hard_state: HardState::new(),
            volatile: VolatileState::new(),
            leader_state: None,
            election_deadline: now + config.election_timeout_max,
            heartbeat_deadline: now,
            votes_received: HashSet::new(),
            ready: Ready::default(),
            leader_id: 0,
            leadership_transfer: None,
            pre_vote: None,
            durable_applied: 0,
            leader_contact: None,
            last_quorum_contact: None,
            quorum_window: Vec::new(),
            leader_since: None,
            lease: LeaseState::new(),
            boot_vote_fence: now + config.election_timeout_max,
            compaction_ceiling: None,
            snapshot_required: false,
            replicated_floor: 0,
            config,
        }
    }

    /// Require a snapshot before this follower takes log entries again, or
    /// lift the requirement. See [`Self::snapshot_required`].
    pub fn set_snapshot_required(&mut self, required: bool) {
        self.snapshot_required = required;
    }

    /// Whether this follower refuses log entries until a snapshot installs.
    pub fn snapshot_required(&self) -> bool {
        self.snapshot_required
    }

    /// Restore state from persistent storage. Must be called before ticking.
    ///
    /// Seeds `volatile.last_applied` from the durable applied index so
    /// delivery resumes at the first entry whose effects are NOT already
    /// durable. Storage written before the durable index existed reports 0 and
    /// degrades to a full replay of the retained log.
    pub fn restore(&mut self) -> Result<()> {
        self.hard_state = self.log.storage().load_hard_state()?;
        self.durable_applied = self.log.storage().load_applied_index()?;
        self.volatile = VolatileState::restored(self.durable_applied);
        self.log.restore()?;
        self.reset_election_timeout();
        Ok(())
    }

    pub fn node_id(&self) -> u64 {
        self.config.node_id
    }

    pub fn group_id(&self) -> u64 {
        self.config.group_id
    }

    /// The log storage this node writes through.
    pub fn storage(&self) -> &S {
        self.log.storage()
    }

    pub fn role(&self) -> NodeRole {
        self.role
    }

    pub fn leader_id(&self) -> u64 {
        self.leader_id
    }

    pub fn current_term(&self) -> u64 {
        self.hard_state.current_term
    }

    pub fn commit_index(&self) -> u64 {
        self.volatile.commit_index
    }

    pub fn last_applied(&self) -> u64 {
        self.volatile.last_applied
    }

    pub fn last_log_index(&self) -> u64 {
        self.log.last_index()
    }

    /// Override election deadline (for testing).
    pub fn election_deadline_override(&mut self, deadline: Instant) {
        self.election_deadline = deadline;
    }

    /// Backdate the last leader contact (for testing staleness bounds).
    pub fn leader_contact_at_override(&mut self, at: Instant) {
        if let Some(contact) = self.leader_contact.as_mut() {
            contact.at = at;
        }
    }

    /// End the boot-time vote refusal now (for testing). A test that builds
    /// nodes and votes at once calls this in place of waiting out
    /// `election_timeout_max`.
    pub fn expire_boot_vote_fence(&mut self) {
        self.boot_vote_fence = Instant::now();
    }

    /// Whether a leadership transfer is currently in progress.
    pub fn leadership_transfer_in_progress(&self) -> bool {
        self.leadership_transfer.is_some()
    }

    /// Override the in-progress leadership-transfer deadline (for testing).
    /// No-op when no transfer is pending.
    pub fn transfer_deadline_override(&mut self, deadline: Instant) {
        if let Some(t) = self.leadership_transfer.as_mut() {
            t.deadline = deadline;
        }
    }

    /// Commit what a quorum holds now that storage made more of this node's
    /// log durable. A leader counts its own entries only once they are
    /// durable (see [`crate::RaftLog::stable_index`]), so a driver whose
    /// storage stages writes calls this after its disk advances. A no-op on a
    /// node that does not lead.
    pub fn on_storage_progress(&mut self) {
        if self.leader_state.is_some() {
            self.try_advance_commit_index();
        }
    }

    /// Take the pending `Ready` output. Caller must execute messages,
    /// persist hard state, and apply committed entries.
    pub fn take_ready(&mut self) -> Ready {
        std::mem::take(&mut self.ready)
    }

    /// Durably persist HardState iff it changed since the last persist.
    /// Must run before a vote grant / vote requests leave this node
    /// (Raft: persist voted_for/current_term to stable storage before replying).
    pub fn persist_hard_state_if_dirty(&mut self) -> crate::error::Result<()> {
        if self.ready.hard_state.is_some() {
            self.log.storage_mut().save_hard_state(&self.hard_state)?;
            self.ready.hard_state = None;
        }
        Ok(())
    }

    /// Query a peer's match_index from the leader's replication state.
    /// Returns `None` if this node is not the leader or the peer is unknown.
    pub fn match_index_for(&self, peer: u64) -> Option<u64> {
        self.leader_state
            .as_ref()
            .map(|ls| ls.match_index_for(peer))
    }

    pub fn log_snapshot_index(&self) -> u64 {
        self.log.snapshot_index()
    }

    pub fn log_snapshot_term(&self) -> u64 {
        self.log.snapshot_term()
    }

    /// Term of the entry at `index`, or `None` when the log no longer holds
    /// it.
    pub fn log_term_at(&self, index: u64) -> Option<u64> {
        self.log.term_at(index)
    }

    /// The entry at `index`, committed or not, or `None` when the log does
    /// not hold it.
    pub fn log_entry_at(&self, index: u64) -> Option<&crate::message::LogEntry> {
        self.log.entry_at(index)
    }

    /// Return committed log entries in the inclusive range `[lo, hi]`.
    ///
    /// Clamps `hi` to `commit_index` so callers that pass `u64::MAX` never
    /// read uncommitted entries.  Returns `Err(RaftError::LogCompacted)` if
    /// `lo` has already been compacted into a snapshot.
    pub fn log_entries_range(
        &self,
        lo: u64,
        hi: u64,
    ) -> crate::error::Result<&[crate::message::LogEntry]> {
        let hi = hi.min(self.volatile.commit_index);
        self.log.entries_range(lo, hi)
    }

    /// Current voter peer list (excluding self).
    pub fn peers(&self) -> &[u64] {
        &self.config.peers
    }

    /// Current voter peer list — alias for `peers()`, clearer at call sites
    /// that need to distinguish voters from learners.
    pub fn voters(&self) -> &[u64] {
        &self.config.peers
    }

    /// Current learner peer list (excluding self).
    pub fn learners(&self) -> &[u64] {
        &self.config.learners
    }

    /// Current observer peer list tracked by this leader (excluding self).
    pub fn observers(&self) -> &[u64] {
        &self.config.observers
    }

    /// Whether `peer` is currently tracked as a learner in this group.
    pub fn is_learner_peer(&self, peer: u64) -> bool {
        self.config.learners.contains(&peer)
    }

    /// Drive time-based events: election timeout, heartbeat.
    pub fn tick(&mut self) {
        let now = Instant::now();

        match self.role {
            NodeRole::Follower | NodeRole::Candidate => {
                if now >= self.election_deadline {
                    self.start_pre_election();
                }
            }
            NodeRole::Leader => {
                // Abort an in-progress leadership transfer whose deadline has
                // passed: clear the volatile state so proposals unblock and
                // the leader resumes normal operation.
                let transfer_expired = self
                    .leadership_transfer
                    .as_ref()
                    .is_some_and(|t| now >= t.deadline);
                if transfer_expired {
                    self.leadership_transfer = None;
                }

                // Check-quorum. Refresh first so a single-voter group, which
                // has no peers to answer and never reaches the response path,
                // renews on its own quorum of one.
                self.refresh_quorum_contact(now);
                if self.quorum_contact_lost(now) {
                    info!(
                        node = self.config.node_id,
                        group = self.config.group_id,
                        term = self.hard_state.current_term,
                        "no quorum contact within the election timeout; stepping down"
                    );
                    self.become_follower(self.hard_state.current_term);
                    return;
                }

                if now >= self.heartbeat_deadline {
                    self.replicate_to_all();
                    self.heartbeat_deadline = now + self.config.heartbeat_interval;
                }
            }
            NodeRole::Learner => {
                // Learners never run election timeouts. They catch up
                // passively via AppendEntries from the leader.
            }
            NodeRole::Observer => {
                // Observers never run election timeouts. They receive entries
                // from the source leader and apply them locally. Acks are
                // advisory and never gate commit on the source.
            }
        }
    }

    /// Propose a new entry (leader only). Returns the log index.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<u64> {
        if self.role != NodeRole::Leader {
            return Err(RaftError::NotLeader {
                leader_hint: if self.leader_id != 0 {
                    Some(self.leader_id)
                } else {
                    None
                },
                term: self.hard_state.current_term,
            });
        }

        // While a leadership transfer is pending the leader holds the log
        // frontier fixed so the target can catch up to it. Reject new
        // proposals (retryable) until the transfer completes or aborts.
        if self.leadership_transfer.is_some() {
            return Err(RaftError::LeadershipTransferInProgress);
        }

        let index = self.log.last_index() + 1;
        let entry = LogEntry {
            term: self.hard_state.current_term,
            index,
            data,
        };

        self.log.append(entry)?;
        self.replicate_to_all();

        // Single-voter cluster: the entry commits once it is durable here.
        // Learners do not count.
        if self.config.cluster_size() == 1 {
            self.try_advance_commit_index();
        }

        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;
    use crate::test_support::{force_election, test_config};
    use std::time::Duration;

    #[test]
    fn single_node_election() {
        let config = test_config(1, vec![]);
        let mut node = RaftNode::new(config, MemStorage::new());

        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();

        assert_eq!(node.role(), NodeRole::Leader);
        assert_eq!(node.current_term(), 1);
        assert_eq!(node.leader_id(), 1);
    }

    #[test]
    fn single_node_propose_and_commit() {
        let config = test_config(1, vec![]);
        let mut node = RaftNode::new(config, MemStorage::new());
        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        assert_eq!(node.role(), NodeRole::Leader);

        let ready = node.take_ready();
        assert!(!ready.committed_entries.is_empty());
        node.advance_applied(ready.committed_entries.last().unwrap().index);

        let idx = node.propose(b"hello".to_vec()).unwrap();
        assert_eq!(idx, 2);

        let ready = node.take_ready();
        assert_eq!(ready.committed_entries.len(), 1);
        assert_eq!(ready.committed_entries[0].data, b"hello");
    }

    #[test]
    fn propose_as_follower_fails() {
        let config = test_config(1, vec![2, 3]);
        let node = &mut RaftNode::new(config, MemStorage::new());
        let err = node.propose(b"data".to_vec()).unwrap_err();
        assert!(matches!(err, RaftError::NotLeader { .. }));
    }

    #[test]
    fn snapshot_needed_after_compaction() {
        let config = test_config(1, vec![2, 3]);
        let mut node = RaftNode::new(config, MemStorage::new());

        force_election(&mut node);
        let _ready = node.take_ready();
        let resp = crate::message::RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        node.handle_request_vote_response(2, &resp);
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();

        for i in 0..9 {
            node.propose(vec![i]).unwrap();
        }
        let _ = node.take_ready();

        node.log.apply_snapshot(8, 1).unwrap();

        node.replicate_to_all();
        let ready = node.take_ready();

        assert!(
            !ready.snapshots_needed.is_empty(),
            "expected snapshots_needed to be non-empty"
        );
    }

    #[test]
    fn starts_as_learner_role() {
        let mut cfg = test_config(2, vec![1]);
        cfg.starts_as_learner = true;
        let node = RaftNode::new(cfg, MemStorage::new());
        assert_eq!(node.role(), NodeRole::Learner);
    }

    /// A restart resumes delivery ABOVE the durable floor: entries whose
    /// effects are already durable must never be handed to the state machine a
    /// second time.
    #[test]
    fn restore_seeds_last_applied_from_durable_index() {
        let mut storage = MemStorage::new();
        storage
            .append(&[
                LogEntry {
                    term: 1,
                    index: 1,
                    data: b"a".to_vec(),
                },
                LogEntry {
                    term: 1,
                    index: 2,
                    data: b"b".to_vec(),
                },
                LogEntry {
                    term: 1,
                    index: 3,
                    data: b"c".to_vec(),
                },
            ])
            .unwrap();
        storage.save_applied_index(2).unwrap();

        let mut node = RaftNode::new(test_config(1, vec![]), storage);
        node.restore().unwrap();
        assert_eq!(node.last_applied(), 2);
        assert_eq!(node.durable_applied_index(), 2);

        // Learning the commit index re-delivers ONLY the tail above the floor.
        node.volatile.commit_index = 3;
        node.collect_committed_entries();
        let ready = node.take_ready();
        assert_eq!(ready.committed_entries.len(), 1);
        assert_eq!(ready.committed_entries[0].index, 3);
    }

    /// A restart must NOT reset commit_index below the durable applied floor.
    ///
    /// Everything at or below `durable_applied` is provably committed (apply
    /// only ever runs on committed entries, and the floor is only advanced
    /// after durable apply or snapshot install). Seeding `commit_index` from
    /// that floor is therefore safe, avoids re-scanning the whole log after a
    /// restart, and keeps the commit index monotonic across restarts.
    #[test]
    fn restore_seeds_commit_index_from_durable_floor() {
        let mut storage = MemStorage::new();
        storage
            .append(&[
                LogEntry {
                    term: 1,
                    index: 1,
                    data: b"a".to_vec(),
                },
                LogEntry {
                    term: 1,
                    index: 2,
                    data: b"b".to_vec(),
                },
                LogEntry {
                    term: 1,
                    index: 3,
                    data: b"c".to_vec(),
                },
            ])
            .unwrap();
        storage.save_applied_index(2).unwrap();

        let mut node = RaftNode::new(test_config(1, vec![]), storage);
        node.restore().unwrap();
        assert_eq!(node.durable_applied_index(), 2);
        assert!(
            node.commit_index() >= 2,
            "commit_index must be seeded from the durable floor, got {}",
            node.commit_index()
        );

        // Advancing above the floor still works normally.
        node.volatile.commit_index = 3;
        node.collect_committed_entries();
        let ready = node.take_ready();
        assert_eq!(ready.committed_entries.len(), 1);
        assert_eq!(ready.committed_entries[0].index, 3);
    }

    #[test]
    fn learner_tick_does_not_start_election() {
        let mut cfg = test_config(2, vec![1]);
        cfg.starts_as_learner = true;
        let mut node = RaftNode::new(cfg, MemStorage::new());
        // Force "election deadline" in the past: a follower would immediately
        // start an election, but a learner must ignore it.
        node.election_deadline = Instant::now() - Duration::from_millis(1);
        node.tick();
        assert_eq!(node.role(), NodeRole::Learner);
        assert_eq!(node.current_term(), 0);
    }
}
