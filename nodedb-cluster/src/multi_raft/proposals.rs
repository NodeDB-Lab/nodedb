// SPDX-License-Identifier: BUSL-1.1

//! Leadership checks, proposals and log access on hosted Raft groups.

use nodedb_raft::RaftNode;

use crate::error::{ClusterError, Result};
use crate::group_disk::StagedLogStorage;
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
    /// the owning group. The Calvin scheduler proposes its owed sequencer
    /// entries only while this holds.
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

    /// Make this node's replica of `group_id` refuse log entries until a
    /// snapshot installs, or lift that. Its leader answers the refusal with
    /// a snapshot. Returns whether this node hosts the group.
    pub fn set_snapshot_required(&mut self, group_id: u64, required: bool) -> bool {
        match self.groups.get_mut(&group_id) {
            Some(node) => {
                node.set_snapshot_required(required);
                true
            }
            None => false,
        }
    }

    /// Decide again whether this node's replica of data group `group_id`
    /// requires a snapshot, by the installed requirement. Run after a
    /// snapshot of `group_id` installs: the host recorded the state it
    /// brought. A sequencer snapshot moves the first index the sequencer log
    /// holds, so every data group mounted here is decided again.
    pub fn refresh_snapshot_requirement(&mut self, group_id: u64) {
        let Some(requirement) = self.snapshot_requirement.clone() else {
            return;
        };
        let groups: Vec<u64> = if group_id == crate::calvin::SEQUENCER_GROUP_ID {
            self.groups
                .keys()
                .copied()
                .filter(|&g| super::core::is_data_group(g))
                .collect()
        } else if super::core::is_data_group(group_id) {
            vec![group_id]
        } else {
            return;
        };
        let sequencer_first = self.sequencer_first_available();
        for group_id in groups {
            if let Some(node) = self.groups.get_mut(&group_id) {
                node.set_snapshot_required(requirement(group_id, sequencer_first));
            }
        }
    }

    /// The first index this node's sequencer log holds, once the log has a
    /// known start: it holds an entry or the boundary of a snapshot. `None`
    /// while it holds neither, or the group is not mounted here. A replica
    /// with an empty log learns only from its leader whether the log
    /// reaches it from index 1 or from a snapshot.
    pub fn sequencer_log_start(&self) -> Option<u64> {
        let node = self.groups.get(&crate::calvin::SEQUENCER_GROUP_ID)?;
        (node.last_log_index() >= 1).then(|| node.first_available_index())
    }

    /// Whether this node's replica of `group_id` refuses log entries until a
    /// snapshot installs.
    pub fn snapshot_required(&self, group_id: u64) -> bool {
        self.groups
            .get(&group_id)
            .is_some_and(|node| node.snapshot_required())
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
        // A sequencer replica that installs a snapshot never receives the
        // Calvin inputs the snapshot covers. So no replica compacts past an
        // entry a live voter's log may lack. A voter silent past the
        // check-quorum window does not hold the floor. It recovers through a
        // sequencer snapshot install once it answers again.
        let applied_index = if group_id == crate::calvin::SEQUENCER_GROUP_ID {
            applied_index.min(node.replicated_floor())
        } else {
            applied_index
        };
        Ok(node.maybe_compact_log(applied_index)?)
    }

    /// Stamp every metadata entry this node appends as leader from `clock`,
    /// the node HLC.
    pub fn set_metadata_clock(&mut self, clock: std::sync::Arc<nodedb_types::HlcClock>) {
        self.metadata_clock = Some(clock);
    }

    /// Propose the encoded metadata entry `data` to group 0, stamped with
    /// the metadata clock when one is set. Returns its log index.
    ///
    /// The stamp is taken under the `MultiRaft` lock, on the leader, as the
    /// entry is appended. It is above the metadata clock's reading and above
    /// every stamp the log holds. Every node folds each applied stamp into
    /// its clock, and restores the folded high-water at boot and at a
    /// snapshot install. So stamps rise with the log index, and every entry
    /// stamped at or below a committed entry's stamp sits at a lower index.
    pub fn propose_stamped_metadata(&mut self, data: &[u8]) -> Result<u64> {
        let group_id = crate::metadata_group::METADATA_GROUP_ID;
        let data = match &self.metadata_clock {
            Some(clock) => {
                let node = self
                    .groups
                    .get(&group_id)
                    .ok_or(ClusterError::GroupNotFound { group_id })?;
                let floor = newest_log_stamp(node).map_or(0, |stamp| stamp.saturating_add(1));
                let stamp = clock.now().wall_ns.max(floor);
                crate::metadata_group::codec::stamp_entry(data, stamp)
            }
            None => data.to_vec(),
        };
        // A node that does not lead refuses here, and its stamp stays unused.
        self.propose_to_group(group_id, data)
    }

    /// The stamp of the newest stamped entry the metadata log holds in
    /// memory, committed or not. `None` when it holds none.
    pub fn newest_metadata_stamp(&self) -> Option<u64> {
        self.groups
            .get(&crate::metadata_group::METADATA_GROUP_ID)
            .and_then(newest_log_stamp)
    }

    /// Bound `group_id`'s log compaction by `ceiling`: the log never
    /// compacts past the index it holds, on every path. An archiver raises
    /// it once it holds a copy of every entry at or below the new value. The
    /// bound also holds for the group when it is added again.
    pub fn set_compaction_ceiling(
        &mut self,
        group_id: u64,
        ceiling: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        if let Some(node) = self.groups.get_mut(&group_id) {
            node.set_compaction_ceiling(std::sync::Arc::clone(&ceiling));
        }
        self.compaction_ceilings.insert(group_id, ceiling);
    }
}

/// The stamp of the newest stamped entry `node` holds in memory. Stamps rise
/// with the index, so the walk stops at the first stamped entry from the end.
fn newest_log_stamp(node: &RaftNode<StagedLogStorage>) -> Option<u64> {
    let first = node.log_snapshot_index().saturating_add(1);
    (first..=node.last_log_index())
        .rev()
        .filter_map(|index| node.log_entry_at(index))
        .find_map(|entry| crate::metadata_group::codec::entry_stamp(&entry.data))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::metadata_group::codec::{entry_stamp, stamp_entry};
    use crate::routing::RoutingTable;

    /// A one-node metadata group that leads, stamping from a fresh clock.
    fn leading_group(dir: &std::path::Path) -> MultiRaft {
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.to_path_buf());
        mr.add_group(0, vec![]).unwrap();
        for node in mr.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        for _ in 0..3 {
            if mr.is_group_leader(0) {
                break;
            }
            mr.tick().unwrap();
        }
        mr.set_metadata_clock(Arc::new(nodedb_types::HlcClock::new()));
        assert!(mr.is_group_leader(0));
        mr
    }

    fn stamp_at(mr: &MultiRaft, index: u64) -> Option<u64> {
        mr.groups[&0]
            .log_entry_at(index)
            .and_then(|entry| entry_stamp(&entry.data))
    }

    /// An entry stamped ahead of the clock reaches the log, as a previous
    /// leader with a faster clock appends one. Every entry this leader stamps
    /// after it lands above it, so no entry stamped below a stamp the archive
    /// holds can follow that stamp in the log.
    #[test]
    fn stamps_rise_with_the_index_when_the_clock_lags_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let mut mr = leading_group(dir.path());
        let first = mr.propose_stamped_metadata(b"a").unwrap();
        let ahead = stamp_at(&mr, first).unwrap() + 3_600_000_000_000;
        let future = mr.propose_to_group(0, stamp_entry(b"b", ahead)).unwrap();
        // An unstamped entry between them hides nothing: the walk goes past it.
        mr.propose_to_group(0, b"no stamp".to_vec()).unwrap();
        let mut indexes = vec![first, future];
        for data in [b"c", b"d", b"e"] {
            indexes.push(mr.propose_stamped_metadata(data).unwrap());
        }
        let stamps: Vec<u64> = indexes
            .iter()
            .map(|index| stamp_at(&mr, *index).unwrap())
            .collect();
        assert!(
            stamps.windows(2).all(|pair| pair[0] < pair[1]),
            "stamps must rise with the index: {stamps:?}"
        );
        assert_eq!(mr.newest_metadata_stamp(), stamps.last().copied());
    }

    /// A follower takes no stamp into the log: its propose is refused.
    #[test]
    fn a_follower_refuses_a_stamped_proposal() {
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(1, &[1, 2], 2);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![2]).unwrap();
        mr.set_metadata_clock(Arc::new(nodedb_types::HlcClock::new()));
        assert!(mr.propose_stamped_metadata(b"a").is_err());
        assert_eq!(mr.newest_metadata_stamp(), None);
    }
}
