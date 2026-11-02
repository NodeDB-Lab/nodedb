// SPDX-License-Identifier: BUSL-1.1

//! Partition state transitions: delete, merge install, purge.

use nodedb_types::timeseries::PartitionState;

use super::entry::{PartitionEntry, PartitionKey};
use super::registry::PartitionRegistry;

impl PartitionRegistry {
    /// Mark a partition as deleted.
    pub fn mark_deleted(&mut self, key: &PartitionKey) -> bool {
        if let Some(entry) = self.partitions.get_mut(key) {
            entry.meta.state = PartitionState::Deleted;
            true
        } else {
            false
        }
    }

    /// Install a committed merge: every partition named in `sources` leaves
    /// the registry and `merged` joins it. `merged` goes through
    /// [`PartitionRegistry::insert_partition`], so a partition that shares
    /// its `min_ts` and is not a source stays reachable. Returns the key
    /// `merged` is filed under.
    pub fn install_merge(&mut self, merged: PartitionEntry, sources: &[String]) -> PartitionKey {
        self.partitions
            .retain(|_, entry| !sources.contains(&entry.dir_name));
        self.insert_partition(merged)
    }

    /// Take the partition filed under `key` out of the registry. The undo
    /// of an install calls it once the partition's directory is gone.
    pub fn remove(&mut self, key: &PartitionKey) -> Option<PartitionEntry> {
        self.partitions.remove(key)
    }

    /// Remove deleted partitions from the registry (after physical cleanup).
    pub fn purge_deleted(&mut self) -> Vec<String> {
        let deleted: Vec<PartitionKey> = self
            .partitions
            .iter()
            .filter(|(_, e)| e.meta.state == PartitionState::Deleted)
            .map(|(key, _)| key.clone())
            .collect();

        let mut dirs = Vec::new();
        for key in deleted {
            self.partitions.remove(&key);
            dirs.push(key.dir_name);
        }
        dirs
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{PartitionInterval, PartitionMeta, TieredPartitionConfig};

    use super::*;

    fn test_config() -> TieredPartitionConfig {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000); // 1d
        cfg
    }

    fn entry(dir: &str, min_ts: i64, state: PartitionState) -> PartitionEntry {
        PartitionEntry {
            meta: PartitionMeta {
                min_ts,
                max_ts: min_ts + 10,
                row_count: 10,
                size_bytes: 100,
                schema_version: 1,
                state,
                interval_ms: 86_400_000,
                last_flushed_wal_lsn: 1,
                column_stats: std::collections::HashMap::new(),
                max_system_ts: 0,
                ended_at: None,
            },
            dir_name: dir.to_string(),
        }
    }

    /// The sources leave, the merged partition joins, and a partition that
    /// shares the merged `min_ts` but is no source stays reachable.
    #[test]
    fn install_merge_replaces_only_the_sources() {
        let mut reg = PartitionRegistry::new(test_config());
        reg.insert_partition(entry("ts-a", 100, PartitionState::Sealed));
        reg.insert_partition(entry("ts-b", 105, PartitionState::Sealed));
        reg.insert_partition(entry("ts-other", 100, PartitionState::Sealed));

        reg.install_merge(
            entry("ts-merged", 100, PartitionState::Merged),
            &["ts-a".to_string(), "ts-b".to_string()],
        );

        let mut names: Vec<&str> = reg.iter().map(|(_, e)| e.dir_name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["ts-merged", "ts-other"]);
    }

    #[test]
    fn purge_removes_deleted_entries() {
        let mut reg = PartitionRegistry::new(test_config());
        reg.insert_partition(entry("ts-a", 100, PartitionState::Sealed));
        let b = reg.insert_partition(entry("ts-b", 200, PartitionState::Sealed));
        reg.mark_deleted(&b);
        assert_eq!(reg.purge_deleted(), vec!["ts-b".to_string()]);
        assert_eq!(reg.partition_count(), 1);
    }
}
