//! Partition state transitions: delete, merge begin/commit/rollback, purge.

use nodedb_types::timeseries::{PartitionMeta, PartitionState};

use super::entry::PartitionEntry;
use super::registry::PartitionRegistry;

impl PartitionRegistry {
    /// Mark a partition as deleted.
    pub fn mark_deleted(&mut self, start_ts: i64) -> bool {
        if let Some(entry) = self.partitions.get_mut(&start_ts) {
            entry.meta.state = PartitionState::Deleted;
            true
        } else {
            false
        }
    }

    /// Mark a partition as merging.
    pub fn mark_merging(&mut self, start_ts: i64) -> bool {
        if let Some(entry) = self.partitions.get_mut(&start_ts)
            && entry.meta.state == PartitionState::Sealed
        {
            entry.meta.state = PartitionState::Merging;
            return true;
        }
        false
    }

    /// Insert a merged partition and mark sources as deleted.
    pub fn commit_merge(
        &mut self,
        merged_meta: PartitionMeta,
        merged_dir: String,
        source_starts: &[i64],
    ) {
        // Mark sources as deleted first (before inserting merged, in case
        // the merged partition's start_ts overlaps a source key).
        for &src in source_starts {
            self.mark_deleted(src);
        }
        // Insert (or overwrite) the merged partition.
        let start_ts = merged_meta.min_ts;
        self.partitions.insert(
            start_ts,
            PartitionEntry {
                meta: merged_meta,
                dir_name: merged_dir,
            },
        );
    }

    /// Remove deleted partitions from the registry (after physical cleanup).
    pub fn purge_deleted(&mut self) -> Vec<String> {
        let deleted: Vec<(i64, String)> = self
            .partitions
            .iter()
            .filter(|(_, e)| e.meta.state == PartitionState::Deleted)
            .map(|(&start, e)| (start, e.dir_name.clone()))
            .collect();

        let mut dirs = Vec::new();
        for (start, dir) in deleted {
            self.partitions.remove(&start);
            dirs.push(dir);
        }
        dirs
    }

    /// Roll back a partition from Merging to Sealed (merge failure recovery).
    pub fn rollback_merging(&mut self, start_ts: i64) {
        if let Some(entry) = self.partitions.get_mut(&start_ts)
            && entry.meta.state == PartitionState::Merging
        {
            entry.meta.state = PartitionState::Sealed;
        }
    }
}
