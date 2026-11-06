// SPDX-License-Identifier: BUSL-1.1

//! A bounded read of one group's committed log, for a reader outside it.
//!
//! Committed indexes hold the same entries on every replica, so any replica can
//! serve this. An entry above the commit index never leaves.

use crate::error::{ClusterError, Result};

use super::core::MultiRaft;

/// What one read of a group's log found.
#[derive(Debug, Clone)]
pub struct CommittedRead {
    /// Committed entries from the read's start.
    pub entries: Vec<nodedb_raft::message::LogEntry>,
    pub commit_index: u64,
    /// The lowest index this replica still holds. A read below it has no
    /// entries here: another replica can still hold them.
    pub first_available: u64,
}

/// One replica's progress, and a copy of the committed entries a probe compares.
#[derive(Debug, Clone)]
pub struct LogProbeView {
    pub commit_index: u64,
    /// The highest index whose apply is durable on this node.
    pub durable_applied: u64,
    /// The committed entries of the range this replica still holds.
    pub entries: Vec<nodedb_raft::message::LogEntry>,
}

impl MultiRaft {
    /// What this node's replica of `group_id` holds of `range`, or `None` when
    /// it hosts no replica. Without a range the view holds no entries.
    pub fn log_probe_view(&self, group_id: u64, range: Option<(u64, u64)>) -> Option<LogProbeView> {
        let node = self.groups.get(&group_id)?;
        let entries = range.map_or_else(Vec::new, |(lo, hi)| {
            let start = lo.max(node.first_available_index());
            if start > hi {
                return Vec::new();
            }
            // A read the log cannot serve leaves the probe with no entry to
            // vouch for, which it answers as a mismatch.
            node.log_entries_range(start, hi)
                .map_or_else(|_| Vec::new(), <[_]>::to_vec)
        });
        Some(LogProbeView {
            commit_index: node.commit_index(),
            durable_applied: node.durable_applied_index(),
            entries,
        })
    }

    /// Read committed entries of `group_id` from `lo`, whose data sums to at
    /// most `max_bytes`. A start below this replica's log returns no entries.
    pub fn read_committed_within(
        &self,
        group_id: u64,
        lo: u64,
        max_bytes: usize,
    ) -> Result<CommittedRead> {
        let node = self
            .groups
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        let commit_index = node.commit_index();
        let first_available = node.first_available_index();
        let entries = if lo < first_available || lo > commit_index {
            Vec::new()
        } else {
            node.log_committed_within(lo, max_bytes)?.to_vec()
        };
        Ok(CommittedRead {
            entries,
            commit_index,
            first_available,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::routing::RoutingTable;

    fn leader() -> (MultiRaft, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let routing = RoutingTable::uniform(1, &[1], 1);
        let mut multi_raft = MultiRaft::new(1, routing, dir.path().to_path_buf());
        multi_raft.add_group(0, vec![]).expect("group");
        for node in multi_raft.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        multi_raft.tick().expect("tick");
        apply(&mut multi_raft);
        (multi_raft, dir)
    }

    fn apply(multi_raft: &mut MultiRaft) {
        multi_raft.wait_all_durable_blocking().expect("durable");
        for (group_id, ready) in multi_raft.tick().expect("tick").groups {
            if let Some(last) = ready.committed_entries.last() {
                multi_raft
                    .advance_applied(group_id, last.index)
                    .expect("advance");
            }
        }
    }

    #[test]
    fn a_read_stops_at_the_commit_index_and_the_byte_budget() {
        let (mut multi_raft, _dir) = leader();
        for byte in 0..5u8 {
            multi_raft
                .propose_to_group(0, vec![byte; 10])
                .expect("propose");
        }
        for _ in 0..3 {
            apply(&mut multi_raft);
        }
        let whole = multi_raft
            .read_committed_within(0, 1, usize::MAX)
            .expect("read");
        assert!(whole.entries.len() >= 5, "{whole:?}");
        let tight = multi_raft.read_committed_within(0, 1, 1).expect("read");
        assert_eq!(tight.entries.len(), 1, "the first entry always ships");

        // Proposed and not yet committed: it never leaves.
        multi_raft
            .propose_to_group(0, vec![9; 10])
            .expect("propose");
        let read = multi_raft
            .read_committed_within(0, 1, usize::MAX)
            .expect("read");
        assert!(
            read.entries
                .iter()
                .all(|entry| entry.index <= read.commit_index)
        );
        let past = multi_raft
            .read_committed_within(0, read.commit_index + 1, usize::MAX)
            .expect("read");
        assert!(past.entries.is_empty());
    }

    #[test]
    fn a_group_this_node_lacks_is_an_error() {
        let (multi_raft, _dir) = leader();
        assert!(matches!(
            multi_raft.read_committed_within(9, 1, 1),
            Err(ClusterError::GroupNotFound { group_id: 9 })
        ));
    }

    #[test]
    fn a_read_below_the_log_start_returns_no_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let routing = RoutingTable::uniform(1, &[1], 1);
        let mut multi_raft = MultiRaft::new(1, routing, dir.path().to_path_buf())
            .with_log_compaction_threshold(Some(1));
        multi_raft.add_group(0, vec![]).expect("group");
        for node in multi_raft.groups.values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        multi_raft.tick().expect("tick");
        apply(&mut multi_raft);
        for byte in 0..3u8 {
            multi_raft.propose_to_group(0, vec![byte]).expect("propose");
        }
        for _ in 0..3 {
            multi_raft.wait_all_durable_blocking().expect("durable");
            for (group_id, ready) in multi_raft.tick().expect("tick").groups {
                if let Some(last) = ready.committed_entries.last() {
                    multi_raft
                        .advance_applied(group_id, last.index)
                        .expect("advance");
                    multi_raft
                        .save_applied_index(group_id, last.index)
                        .expect("save");
                    multi_raft
                        .maybe_compact_group(group_id, last.index)
                        .expect("compact");
                }
            }
        }
        let read = multi_raft
            .read_committed_within(0, 1, usize::MAX)
            .expect("read");
        assert!(read.first_available > 1, "{read:?}");
        assert!(read.entries.is_empty());
    }
}
