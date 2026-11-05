// SPDX-License-Identifier: BUSL-1.1

//! Inbound RPC dispatch — look up the target group and delegate.
//!
//! Also holds the response handlers (`handle_append_entries_response`,
//! `handle_request_vote_response`) and the helpers for the tick loop
//! (`snapshot_metadata`, `advance_applied`, `match_index_for`).

use nodedb_raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    LogEntry, PreVoteRequest, PreVoteResponse, RequestVoteRequest, RequestVoteResponse,
    TimeoutNowRequest,
};

use crate::error::{ClusterError, Result};

use super::core::MultiRaft;

impl MultiRaft {
    /// Refuse a Raft request to a replica whose disk fail-stopped: no answer
    /// of it can rest on a durable write. Peers see a replica that is down,
    /// and the group's other replicas carry it.
    fn refuse_failed_disk(&self, group_id: u64) -> Result<()> {
        match self.groups.get(&group_id) {
            Some(node) if node.storage().disk_failed() => Err(ClusterError::Storage {
                detail: format!(
                    "raft group {group_id} on node {}: the disk fail-stopped after a write \
                     error, so this replica answers no Raft request. Fix the disk and restart \
                     the node.",
                    self.node_id
                ),
            }),
            _ => Ok(()),
        }
    }

    /// Route an AppendEntries RPC to the correct group.
    pub fn handle_append_entries(
        &mut self,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        self.refuse_failed_disk(req.group_id)?;
        let node = self
            .groups
            .get_mut(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;
        Ok(node.handle_append_entries(req))
    }

    /// Route a RequestVote RPC to the correct group.
    pub fn handle_request_vote(&mut self, req: &RequestVoteRequest) -> Result<RequestVoteResponse> {
        self.refuse_failed_disk(req.group_id)?;
        let node = self
            .groups
            .get_mut(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;
        Ok(node.handle_request_vote(req))
    }

    /// Route a PreVote RPC to the correct group.
    pub fn handle_pre_vote(&mut self, req: &PreVoteRequest) -> Result<PreVoteResponse> {
        let node = self
            .groups
            .get_mut(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;
        Ok(node.handle_pre_vote(req))
    }

    /// Route an InstallSnapshot RPC to the correct group.
    pub fn handle_install_snapshot(
        &mut self,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        self.refuse_failed_disk(req.group_id)?;
        let node = self
            .groups
            .get_mut(&req.group_id)
            .ok_or(ClusterError::GroupNotFound {
                group_id: req.group_id,
            })?;
        Ok(node.handle_install_snapshot(req)?)
    }

    /// Whether a snapshot at `last_included_index`, sent at `term`, must be
    /// applied to the local state machine.
    ///
    /// False for a stale leader's snapshot, and for one this node already
    /// applied past. Installing either would move the state machine behind
    /// the log it continues to apply from. A replica that requires a snapshot
    /// also takes one at exactly its applied index: its log is whole there,
    /// and the state beside the log is what it lacks.
    pub fn snapshot_install_needed(
        &self,
        group_id: u64,
        term: u64,
        last_included_index: u64,
    ) -> Result<bool> {
        self.refuse_failed_disk(group_id)?;
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let applied = node.last_applied();
        let ahead = last_included_index > applied
            || (node.snapshot_required() && last_included_index == applied);
        Ok(term >= node.current_term() && ahead)
    }

    /// Adopt a snapshot the local state machine already holds as the group's
    /// log boundary and durable applied floor. See
    /// [`nodedb_raft::RaftNode::adopt_snapshot_boundary`].
    pub fn adopt_snapshot_boundary(
        &mut self,
        group_id: u64,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        Ok(node.adopt_snapshot_boundary(last_included_index, last_included_term)?)
    }

    /// Route a TimeoutNow RPC to the correct group.
    ///
    /// One-way — no response is produced. Silently ignored if the group is
    /// not mounted on this node (mirrors `handle_request_vote` for absent
    /// groups). The term+leader_id guard inside `RaftNode::handle_timeout_now`
    /// remains in place as an additional correctness check.
    pub fn handle_timeout_now(&mut self, req: &TimeoutNowRequest) {
        if let Some(node) = self.groups.get_mut(&req.group_id) {
            node.handle_timeout_now(req);
        }
    }

    /// Stage a group's HardState (current_term/voted_for) on its disk if it
    /// changed since the last persist. Runs under the `MultiRaft` lock and
    /// never waits on disk. Before an RPC reply that granted a vote or bumped
    /// the term leaves this node, the caller awaits the group's
    /// [`MultiRaft::reply_ticket`], so a restart cannot forget the vote and
    /// let two leaders form.
    ///
    /// No-op when the group is not mounted on this node.
    pub fn persist_group_hard_state(&mut self, group_id: u64) -> Result<()> {
        if let Some(node) = self.groups.get_mut(&group_id) {
            node.persist_hard_state_if_dirty()?;
        }
        Ok(())
    }

    /// The sequence number of the last write `group_id` staged, or 0 when the
    /// group is not mounted. A reply handler reads it before its Raft call
    /// and passes it to [`Self::reply_ticket`].
    pub fn staged_through(&self, group_id: u64) -> u64 {
        self.groups
            .get(&group_id)
            .map_or(0, |node| node.storage().staged_through())
    }

    /// A ticket for the writes a reply depends on: the group's latest hard
    /// state, and every write staged after `mark` when there is one. `None`
    /// when those are durable or the group is not mounted. Writes the apply
    /// loop staged before `mark` hold no reply back.
    pub fn reply_ticket(
        &self,
        group_id: u64,
        mark: u64,
    ) -> Option<crate::group_disk::DurabilityTicket> {
        self.groups.get(&group_id)?.storage().reply_ticket(mark)
    }

    /// Get the current term and snapshot metadata for a group (for building
    /// InstallSnapshot RPCs).
    pub fn snapshot_metadata(&self, group_id: u64) -> Result<(u64, u64, u64)> {
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        Ok((
            node.current_term(),
            node.log_snapshot_index(),
            node.log_snapshot_term(),
        ))
    }

    /// Record that `peer` installed a snapshot of `group_id` through
    /// `last_included_index`. No-op when the group is not mounted here.
    pub fn record_snapshot_installed(
        &mut self,
        group_id: u64,
        peer: u64,
        last_included_index: u64,
    ) {
        if let Some(node) = self.groups.get_mut(&group_id) {
            node.record_snapshot_installed(peer, last_included_index);
        }
    }

    /// Term of the entry at `index` in a group's log, or `None` when the
    /// group is not mounted here or the log no longer holds that entry.
    pub fn log_term_at(&self, group_id: u64, index: u64) -> Option<u64> {
        self.groups.get(&group_id)?.log_term_at(index)
    }

    /// Handle AppendEntries response for a specific group.
    pub fn handle_append_entries_response(
        &mut self,
        group_id: u64,
        peer: u64,
        resp: &AppendEntriesResponse,
    ) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.handle_append_entries_response(peer, resp);
        Ok(())
    }

    /// Handle RequestVote response for a specific group.
    pub fn handle_request_vote_response(
        &mut self,
        group_id: u64,
        peer: u64,
        resp: &RequestVoteResponse,
    ) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.handle_request_vote_response(peer, resp);
        Ok(())
    }

    /// Handle PreVote response for a specific group.
    pub fn handle_pre_vote_response(
        &mut self,
        group_id: u64,
        peer: u64,
        resp: &PreVoteResponse,
    ) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.handle_pre_vote_response(peer, resp);
        Ok(())
    }

    /// Advance applied index for a group after processing committed entries.
    ///
    /// This is the DELIVERY watermark. See [`Self::save_applied_index`] for the
    /// durable floor a restart resumes from.
    pub fn advance_applied(&mut self, group_id: u64, applied_to: u64) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.advance_applied(applied_to);
        Ok(())
    }

    /// Return a committed batch the tick took but did not apply. See
    /// [`nodedb_raft::RaftNode::requeue_committed`].
    pub fn requeue_committed(&mut self, group_id: u64, batch: Vec<LogEntry>) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.requeue_committed(batch);
        Ok(())
    }

    /// Durably record `applied_to` as the group's applied floor.
    ///
    /// `applied_to` MUST name an entry whose state-machine effects are already
    /// durable — for data groups, one whose redo record the WAL has fsynced.
    /// The next boot resumes delivery at `applied_to + 1`, so this is what
    /// keeps WAL replay and Raft replay from applying the same entry twice.
    ///
    /// Monotonic per group: an index at or below the current floor is a no-op.
    pub fn save_applied_index(&mut self, group_id: u64, applied_to: u64) -> Result<()> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        node.save_durable_applied_index(applied_to)?;
        Ok(())
    }

    /// Query a peer's match_index from a specific Raft group's leader state.
    pub fn match_index_for(&self, group_id: u64, peer: u64) -> Option<u64> {
        self.groups.get(&group_id)?.match_index_for(peer)
    }

    /// Read the locally-applied index for a Raft group hosted on this
    /// node. Returns `None` if the group is not mounted here.
    ///
    /// Used by the tick loop to mirror `last_applied` into the
    /// per-group [`crate::applied_watcher::AppliedIndexWatcher`] —
    /// covers both the regular apply path and the snapshot-install
    /// path (which sets `last_applied = last_included_index`
    /// directly without producing committed entries).
    pub fn last_applied(&self, group_id: u64) -> Option<u64> {
        self.groups.get(&group_id).map(|n| n.last_applied())
    }

    /// Highest index present in a group's local log — committed or not — or
    /// `None` if the group is not mounted here.
    ///
    /// Read alongside [`Self::last_applied`] to answer "has this node applied
    /// everything its log holds?". That question needs the LOG TIP, not
    /// `commit_index`: a node that has just won an election observes its own
    /// `commit_index` still behind its log until its term's no-op commits, yet
    /// every entry already in a leader's log commits moments later — so only
    /// the tip bounds what the node is about to be responsible for.
    pub fn last_log_index(&self, group_id: u64) -> Option<u64> {
        self.groups.get(&group_id).map(|n| n.last_log_index())
    }

    /// `(group_id, last_applied)` pairs for every locally-mounted
    /// group. Cheap O(groups) snapshot — groups are few (one
    /// metadata + handful of vshard groups per node).
    pub fn applied_indices(&self) -> Vec<(u64, u64)> {
        self.groups
            .iter()
            .map(|(gid, node)| (*gid, node.last_applied()))
            .collect()
    }
}
