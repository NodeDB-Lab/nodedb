//! Per-group, per-partition offset tracking with redb persistence.
//!
//! Each consumer group maintains its own set of partition offsets,
//! independent of other groups on the same stream. Offsets are committed
//! explicitly (no auto-commit) to prevent lost events on consumer crash.

use std::collections::HashMap;
use std::path::Path;

use redb::{Database, TableDefinition};
use tracing::debug;

use super::types::PartitionOffset;

/// redb table: "{tenant}:{stream}:{group}:{partition}" → LE u64 (committed LSN).
const OFFSETS: TableDefinition<&str, &[u8]> = TableDefinition::new("consumer_offsets");

/// Cache key: (tenant_id, stream_name, group_name).
type GroupKey = (u32, String, String);

/// Manages offset state for all consumer groups across all streams.
///
/// Thread-safe: the in-memory cache uses `RwLock` for concurrent reads,
/// and redb commits serialize writes.
pub struct OffsetStore {
    db: Database,
    /// In-memory cache: GroupKey → { partition_id → committed_lsn }.
    cache: std::sync::RwLock<HashMap<GroupKey, HashMap<u16, u64>>>,
}

impl OffsetStore {
    /// Open or create the offset store at `{data_dir}/event_plane/consumer_offsets.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;

        let path = dir.join("consumer_offsets.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("open offset db {}: {e}", path.display()),
        })?;

        // Ensure table exists.
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_write: {e}"),
            })?;
            txn.open_table(OFFSETS).map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("open_table: {e}"),
            })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("commit: {e}"),
            })?;
        }

        // Load all offsets into cache.
        let mut cache: HashMap<(u32, String, String), HashMap<u16, u64>> = HashMap::new();
        {
            let txn = db.begin_read().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_read: {e}"),
            })?;
            let table = txn.open_table(OFFSETS).map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("open_table: {e}"),
            })?;
            let mut range = table.range::<&str>(..).map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("range: {e}"),
            })?;
            while let Some(Ok((key_guard, value_guard))) = range.next() {
                let key_str: &str = key_guard.value();
                let bytes: &[u8] = value_guard.value();
                if bytes.len() == 8 {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(bytes);
                    let lsn = u64::from_le_bytes(buf);
                    if let Some((tenant, stream, group, partition)) = parse_offset_key(key_str) {
                        cache
                            .entry((tenant, stream, group))
                            .or_default()
                            .insert(partition, lsn);
                    }
                }
            }
        }

        let total: usize = cache.values().map(|m| m.len()).sum();
        if total > 0 {
            debug!(offsets = total, "loaded consumer offsets from redb");
        }

        Ok(Self {
            db,
            cache: std::sync::RwLock::new(cache),
        })
    }

    /// Commit an offset for a specific partition.
    ///
    /// Rejects regressions: `lsn` must be >= the currently committed LSN
    /// for this `(tenant, stream, group, partition)`. A regressing commit
    /// would cause the next poll to redeliver already-acknowledged events
    /// and break exactly-once semantics. Re-committing the same LSN is
    /// accepted (idempotent retry).
    ///
    /// # Durability of the monotonicity check
    ///
    /// The check reads the in-memory `cache`, but the cache is authoritative
    /// because every successful `commit_offset` writes the new LSN to redb
    /// *before* updating the cache entry, and `OffsetStore::open` rebuilds
    /// the cache from redb on startup. So after a process restart the check
    /// still sees the last durably committed LSN and a regressing retry is
    /// still rejected — the guarantee holds across crashes, not only within
    /// a single process lifetime.
    pub fn commit_offset(
        &self,
        tenant_id: u32,
        stream: &str,
        group: &str,
        partition_id: u16,
        lsn: u64,
    ) -> crate::Result<()> {
        {
            let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
            if let Some(current) = cache
                .get(&(tenant_id, stream.to_string(), group.to_string()))
                .and_then(|m| m.get(&partition_id))
                .copied()
                && lsn < current
            {
                return Err(crate::Error::OffsetRegression {
                    stream: stream.to_string(),
                    group: group.to_string(),
                    partition_id,
                    current_lsn: current,
                    attempted_lsn: lsn,
                });
            }
        }

        let key = offset_key(tenant_id, stream, group, partition_id);
        let value = lsn.to_le_bytes();

        // Write to redb.
        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn.open_table(OFFSETS).map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("open_table: {e}"),
            })?;
            table
                .insert(key.as_str(), value.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("insert: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("commit: {e}"),
        })?;

        // Update cache.
        let mut cache = self.cache.write().unwrap_or_else(|p| p.into_inner());
        cache
            .entry((tenant_id, stream.to_string(), group.to_string()))
            .or_default()
            .insert(partition_id, lsn);

        Ok(())
    }

    /// Get the committed offset for a specific partition.
    /// Returns 0 if no offset has been committed.
    pub fn get_offset(&self, tenant_id: u32, stream: &str, group: &str, partition_id: u16) -> u64 {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache
            .get(&(tenant_id, stream.to_string(), group.to_string()))
            .and_then(|m| m.get(&partition_id))
            .copied()
            .unwrap_or(0)
    }

    /// Get all committed offsets for a group.
    pub fn get_all_offsets(
        &self,
        tenant_id: u32,
        stream: &str,
        group: &str,
    ) -> Vec<PartitionOffset> {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache
            .get(&(tenant_id, stream.to_string(), group.to_string()))
            .map(|m| {
                let mut offsets: Vec<PartitionOffset> = m
                    .iter()
                    .map(|(&pid, &lsn)| PartitionOffset::new(pid, lsn))
                    .collect();
                offsets.sort_by_key(|o| o.partition_id);
                offsets
            })
            .unwrap_or_default()
    }

    /// Delete all offsets for a group (on DROP CONSUMER GROUP).
    pub fn delete_group(&self, tenant_id: u32, stream: &str, group: &str) -> crate::Result<()> {
        // Remove from cache first to get partition IDs.
        let partitions: Vec<u16> = {
            let mut cache = self.cache.write().unwrap_or_else(|p| p.into_inner());
            let key = (tenant_id, stream.to_string(), group.to_string());
            cache
                .remove(&key)
                .map(|m| m.keys().copied().collect())
                .unwrap_or_default()
        };

        // Remove from redb.
        if !partitions.is_empty() {
            let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_write: {e}"),
            })?;
            {
                let mut table = txn.open_table(OFFSETS).map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
                for pid in partitions {
                    let key = offset_key(tenant_id, stream, group, pid);
                    let _ = table.remove(key.as_str());
                }
            }
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("commit: {e}"),
            })?;
        }

        Ok(())
    }
}

fn offset_key(tenant_id: u32, stream: &str, group: &str, partition_id: u16) -> String {
    format!("{tenant_id}:{stream}:{group}:{partition_id}")
}

fn parse_offset_key(key: &str) -> Option<(u32, String, String, u16)> {
    let parts: Vec<&str> = key.splitn(4, ':').collect();
    if parts.len() == 4 {
        let tenant = parts[0].parse().ok()?;
        let partition = parts[3].parse().ok()?;
        Some((
            tenant,
            parts[1].to_string(),
            parts[2].to_string(),
            partition,
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_and_read_offset() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        store
            .commit_offset(1, "orders_stream", "analytics", 0, 100)
            .unwrap();
        store
            .commit_offset(1, "orders_stream", "analytics", 1, 200)
            .unwrap();

        assert_eq!(store.get_offset(1, "orders_stream", "analytics", 0), 100);
        assert_eq!(store.get_offset(1, "orders_stream", "analytics", 1), 200);
        assert_eq!(store.get_offset(1, "orders_stream", "analytics", 99), 0); // No offset yet.
    }

    #[test]
    fn get_all_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        store.commit_offset(1, "s", "g", 2, 200).unwrap();
        store.commit_offset(1, "s", "g", 0, 100).unwrap();

        let offsets = store.get_all_offsets(1, "s", "g");
        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets[0].partition_id, 0); // Sorted.
        assert_eq!(offsets[0].committed_lsn, 100);
        assert_eq!(offsets[1].partition_id, 2);
    }

    #[test]
    fn delete_group_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        store.commit_offset(1, "s", "g", 0, 100).unwrap();
        store.commit_offset(1, "s", "g", 1, 200).unwrap();
        store.delete_group(1, "s", "g").unwrap();

        assert_eq!(store.get_offset(1, "s", "g", 0), 0);
        assert!(store.get_all_offsets(1, "s", "g").is_empty());
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = OffsetStore::open(dir.path()).unwrap();
            store.commit_offset(1, "s", "g", 5, 999).unwrap();
        }
        let store = OffsetStore::open(dir.path()).unwrap();
        assert_eq!(store.get_offset(1, "s", "g", 5), 999);
    }

    #[test]
    fn independent_groups() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        store.commit_offset(1, "s", "group_a", 0, 100).unwrap();
        store.commit_offset(1, "s", "group_b", 0, 500).unwrap();

        assert_eq!(store.get_offset(1, "s", "group_a", 0), 100);
        assert_eq!(store.get_offset(1, "s", "group_b", 0), 500);
    }

    /// A subsequent commit with an LSN strictly less than the currently
    /// committed LSN must be rejected — otherwise the next poll will
    /// redeliver already-acknowledged events and break exactly-once
    /// semantics downstream.
    #[test]
    fn commit_offset_rejects_regression() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        store.commit_offset(1, "s", "g", 0, 1_000_000).unwrap();

        let result = store.commit_offset(1, "s", "g", 0, 1);
        assert!(
            result.is_err(),
            "offset regression (1 after 1_000_000) must be rejected; got {result:?}"
        );
        // Committed value must not have been overwritten.
        assert_eq!(
            store.get_offset(1, "s", "g", 0),
            1_000_000,
            "rejected commit must not clobber the stored offset"
        );
    }

    /// Committing the same LSN that is already stored must succeed
    /// (idempotent retry) — only strict regressions are rejected.
    #[test]
    fn commit_offset_same_lsn_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        store.commit_offset(1, "s", "g", 0, 500).unwrap();
        store
            .commit_offset(1, "s", "g", 0, 500)
            .expect("re-committing the same LSN must be accepted");
        assert_eq!(store.get_offset(1, "s", "g", 0), 500);
    }
}
