// SPDX-License-Identifier: BUSL-1.1

//! Leadership checks, proposals and log access on hosted Raft groups.

use crate::error::{ClusterError, Result};
use crate::multi_raft::core::MultiRaft;

impl MultiRaft {
    /// Get the leader for a given vShard (from local group state).
    pub fn leader_for_vshard(&self, vshard_id: u32) -> Result<Option<u64>> {
        let group_id = self
            .routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_for_vshard(vshard_id)?;
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let lid = node.leader_id();
        Ok(if lid == 0 { None } else { Some(lid) })
    }

    /// Whether THIS node is currently the leader of the data-group that owns
    /// `vshard_id`.
    ///
    /// Maps the vshard to its Raft group via the routing table and reuses the
    /// existing local leader-role check — no new election. Returns `false` when
    /// the vshard has no group mapping or this node is a follower/learner for
    /// the owning group. Used by the Calvin scheduler to stamp the per-node,
    /// non-replicated `is_group_leader` dispatch flag so the OLLP optimistic-lock
    /// verification runs only on the leader while every replica applies the same
    /// predicted write-set (determinism).
    pub fn vshard_role_is_leader(&self, vshard_id: u32) -> bool {
        match self
            .routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_for_vshard(vshard_id)
        {
            Ok(group_id) => self.is_group_leader(group_id),
            Err(_) => false,
        }
    }

    /// Propose a command to the Raft group that owns the given vShard.
    ///
    /// Returns `(group_id, log_index)` on success.
    pub fn propose(&mut self, vshard_id: u32, data: Vec<u8>) -> Result<(u64, u64)> {
        let group_id = self
            .routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_for_vshard(vshard_id)?;
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let log_index = node.propose(data)?;
        Ok((group_id, log_index))
    }

    /// Returns `true` if this node is currently the leader of `group_id`.
    ///
    /// Returns `false` when the group does not exist on this node or when the
    /// node is a follower, candidate, or learner in the group.
    pub fn is_group_leader(&self, group_id: u64) -> bool {
        use nodedb_raft::state::NodeRole;
        self.groups
            .get(&group_id)
            .map(|n| n.role() == NodeRole::Leader)
            .unwrap_or(false)
    }

    /// Propose a command directly to a specific Raft group (e.g. the
    /// metadata group, which has no vShard mapping).
    ///
    /// Returns the committed log index on success.
    pub fn propose_to_group(&mut self, group_id: u64, data: Vec<u8>) -> Result<u64> {
        let node = self
            .groups
            .get_mut(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        Ok(node.propose(data)?)
    }

    /// Read committed log entries for a Raft group in the inclusive index
    /// range `[lo, hi]`.
    ///
    /// `hi` is clamped to the group's `commit_index` so callers that pass
    /// `u64::MAX` never read uncommitted entries.
    ///
    /// Used by the Calvin scheduler's rebuild path to replay sequenced
    /// transactions from the sequencer Raft log after a restart.
    ///
    /// Returns `Err(ClusterError::Raft(RaftError::LogCompacted))` if `lo`
    /// has been compacted into a snapshot (caller must install a snapshot
    /// instead of replaying from log).
    pub fn read_committed_entries(
        &self,
        group_id: u64,
        lo: u64,
        hi: u64,
    ) -> Result<Vec<nodedb_raft::message::LogEntry>> {
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let entries = node.log_entries_range(lo, hi)?;
        Ok(entries.to_vec())
    }

    /// The lowest committed index still available in `group_id`'s retained log
    /// (`snapshot_index + 1`), or `None` when the group is absent on this node.
    ///
    /// Used to arm a Calvin scheduler catch-up from the earliest replayable
    /// sequencer index so its drain reads exactly the retained log and never
    /// faults on a compacted range.
    pub fn first_available_index(&self, group_id: u64) -> Option<u64> {
        self.groups
            .get(&group_id)
            .map(|n| n.first_available_index())
    }

    /// Auto-compact a group's log if its configured threshold has been
    /// reached, given the DATA-PLANE applied watermark `applied_index`.
    ///
    /// `applied_index` MUST be the index the data-plane state machine has
    /// durably applied to (NOT raft's commit index). Compacting past an
    /// unapplied index would let the `SnapshotBuilder` serialize
    /// incomplete state and corrupt a lagging follower's snapshot.
    ///
    /// No-op (returns `Ok(false)`) when the group is absent on this node,
    /// the threshold is `None`, or the retained-entry count is below the
    /// threshold. Returns `Ok(true)` when a compaction was performed.
    pub fn maybe_compact_group(&mut self, group_id: u64, applied_index: u64) -> Result<bool> {
        // Defer compaction while a snapshot transfer for this group is in
        // flight: advancing the snapshot boundary mid-transfer would corrupt
        // the catching-up peer. The apply loop retries on the next applied
        // entry, so the watermark still advances once the transfer completes.
        if self.in_flight_snapshots.is_active(group_id) {
            return Ok(false);
        }
        let Some(node) = self.groups.get_mut(&group_id) else {
            return Ok(false);
        };
        Ok(node.maybe_compact_log(applied_index)?)
    }
}
