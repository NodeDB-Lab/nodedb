// SPDX-License-Identifier: BUSL-1.1

//! Manifest persistence, recovery, and orphan cleanup.

use nodedb_types::timeseries::{PartitionState, TieredPartitionConfig};

use super::entry::{PartitionEntry, PartitionKey};
use super::registry::PartitionRegistry;

impl PartitionRegistry {
    /// Export registry state for persistence.
    pub fn export(&self) -> Vec<(PartitionKey, PartitionEntry)> {
        self.partitions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Import persisted registry state.
    ///
    /// Callers that already hold a key issued by this registry (re-import of an
    /// export) use this. Anything registering a NEWLY written partition must go
    /// through [`PartitionRegistry::insert_partition`], which cannot displace a
    /// partition that happens to share a start timestamp.
    pub fn import(&mut self, entries: Vec<(PartitionKey, PartitionEntry)>) {
        for (key, entry) in entries {
            self.partitions.insert(key, entry);
        }
    }

    /// Register a newly written partition, returning the key it was filed under.
    ///
    /// The key is the partition's `min_ts` and directory name
    /// ([`PartitionKey`]). Two partitions can legitimately share a `min_ts`
    /// (late or duplicate-timestamp ingest), and their directory names differ,
    /// so neither ever displaces the other.
    ///
    /// Re-registering the SAME directory is idempotent — that is a restore or a
    /// boot rescan seeing a partition it already knows, not a second partition.
    /// It replaces the entry under the key the directory is already filed
    /// under.
    pub fn insert_partition(&mut self, entry: PartitionEntry) -> PartitionKey {
        let key = self
            .key_of_dir(&entry.dir_name)
            .unwrap_or_else(|| PartitionKey::of(&entry));
        self.partitions.insert(key.clone(), entry);
        key
    }

    /// Persist the registry as the JSON file `name` inside `dir` (atomic via
    /// write + rename).
    ///
    /// The write-then-rename pattern gives crash safety:
    /// - Write to `{name}.tmp`
    /// - Rename `{name}.tmp` → `{name}` (atomic on most filesystems)
    ///   If crash during write: the `.tmp` file is orphaned, original intact.
    ///   If crash during rename: atomic — either old or new version visible.
    pub fn persist(&self, dir: &std::path::Path, name: &str) -> crate::Result<()> {
        let entries = self.export();
        let json = sonic_rs::to_vec_pretty(&entries).map_err(|e| crate::Error::Serialization {
            format: "json".to_string(),
            detail: format!("serialize partition registry: {e}"),
        })?;

        nodedb_wal::segment::atomic_write_fsync(dir, name, &json).map_err(|e| {
            crate::Error::Storage {
                engine: "timeseries".to_string(),
                detail: format!("atomic write {}: {e}", dir.join(name).display()),
            }
        })?;
        Ok(())
    }

    /// Recover registry from a persisted JSON file.
    ///
    /// Loads partition entries, filters out stale states:
    /// - `Merging` → rolled back to `Sealed` (incomplete merge on crash)
    /// - `Deleted` → removed (cleanup on recovery)
    pub fn recover(path: &std::path::Path, config: TieredPartitionConfig) -> crate::Result<Self> {
        let data = std::fs::read(path).map_err(|e| crate::Error::Storage {
            engine: "timeseries".to_string(),
            detail: format!("read {}: {e}", path.display()),
        })?;
        let entries: Vec<(PartitionKey, PartitionEntry)> =
            sonic_rs::from_slice(&data).map_err(|e| crate::Error::Serialization {
                format: "json".to_string(),
                detail: format!("parse {}: {e}", path.display()),
            })?;

        let mut registry = Self::new(config);

        for (key, mut entry) in entries {
            match entry.meta.state {
                PartitionState::Merging => {
                    // Incomplete merge — roll back to Sealed.
                    entry.meta.state = PartitionState::Sealed;
                }
                PartitionState::Deleted => {
                    // Skip deleted partitions (cleanup).
                    continue;
                }
                _ => {}
            }
            registry.partitions.insert(key, entry);
        }

        Ok(registry)
    }

    /// Clean up orphaned partition directories that have no manifest entry.
    ///
    /// Called on startup after `recover()`. Scans the timeseries data directory
    /// and removes directories that aren't in the registry (partial merge output).
    pub fn cleanup_orphans(&self, base_dir: &std::path::Path) -> Vec<String> {
        let mut removed = Vec::new();
        let known_dirs: std::collections::HashSet<&str> = self
            .partitions
            .values()
            .map(|e| e.dir_name.as_str())
            .collect();

        if let Ok(entries) = std::fs::read_dir(base_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str()
                    && name.starts_with("ts-")
                    && !known_dirs.contains(name)
                {
                    if let Err(e) = std::fs::remove_dir_all(entry.path()) {
                        tracing::warn!(dir = name, error = %e, "failed to cleanup orphan partition");
                    } else {
                        removed.push(name.to_string());
                    }
                }
            }
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{PartitionInterval, PartitionMeta, TimeRange};

    use super::*;

    fn test_config() -> TieredPartitionConfig {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000); // 1d
        cfg.merge_after_ms = 7 * 86_400_000;
        cfg.merge_count = 3;
        cfg.retention_period_ms = 30 * 86_400_000;
        cfg
    }

    /// Two partitions can legitimately span the same start timestamp (late or
    /// duplicate-timestamp ingest). Both must stay reachable — filing the second
    /// under the first's key would make its on-disk rows invisible to every query
    /// even though a checkpoint reported them durable.
    #[test]
    fn colliding_start_timestamps_keep_both_partitions() {
        fn entry(dir: &str, min_ts: i64) -> PartitionEntry {
            PartitionEntry {
                meta: PartitionMeta {
                    min_ts,
                    max_ts: min_ts,
                    row_count: 1,
                    size_bytes: 1,
                    schema_version: 1,
                    state: PartitionState::Sealed,
                    interval_ms: 0,
                    last_flushed_wal_lsn: 0,
                    column_stats: std::collections::HashMap::new(),
                    max_system_ts: 0,
                    ended_at: None,
                },
                dir_name: dir.to_string(),
            }
        }

        let mut reg = PartitionRegistry::new(test_config());
        let first = reg.insert_partition(entry("ts-a", 100));
        let second = reg.insert_partition(entry("ts-b", 100));

        assert_ne!(first, second);
        assert_eq!(reg.partition_count(), 2);

        let found = reg.query_partitions(
            &TimeRange::new(0, 1000),
            nodedb_columnar::VersionCut::Current,
        );
        let mut dirs: Vec<&str> = found.iter().map(|e| e.dir_name.as_str()).collect();
        dirs.sort_unstable();
        assert_eq!(dirs, vec!["ts-a", "ts-b"]);

        // Re-registering the same directory is idempotent (restore / boot rescan).
        let again = reg.insert_partition(entry("ts-a", 100));
        assert_eq!(again, first);
        assert_eq!(reg.partition_count(), 2);
    }
}
