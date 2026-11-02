// SPDX-License-Identifier: BUSL-1.1

//! Per-group, per-partition offset tracking with redb persistence.
//!
//! Each consumer group maintains its own set of partition offsets,
//! independent of other groups on the same stream. Offsets are committed
//! explicitly (no auto-commit) to prevent lost events on consumer crash.

use std::collections::HashMap;
use std::path::Path;

use redb::{Database, TableDefinition};

use super::codec::{encode_offset, offset_key};
use super::types::PartitionOffset;
use crate::event::cdc::offset::CdcOffset;
use crate::types::DatabaseId;

/// redb table: `v2:{database}:{tenant}:{len}:{stream}:{len}:{group}:{partition}`
/// → 16-byte LE composite offset.
const OFFSETS: TableDefinition<&str, &[u8]> = TableDefinition::new("consumer_offsets");

mod image;

pub use image::OffsetImageRead;

/// Cache key: (database_id, tenant_id, stream_name, group_name).
type GroupKey = (DatabaseId, u64, String, String);

/// Manages offset state for all consumer groups across all streams.
///
/// Thread-safe: the in-memory cache uses `RwLock` for concurrent reads,
/// and redb commits serialize writes.
pub struct OffsetStore {
    db: Database,
    /// In-memory cache: GroupKey → { partition_id → committed composite position }.
    cache: std::sync::RwLock<HashMap<GroupKey, HashMap<u32, CdcOffset>>>,
    /// Serializes offset mutations. In particular, it keeps the monotonicity
    /// check, durable redb commit, and cache update as one operation.
    mutation_lock: std::sync::Mutex<()>,
    /// Memory-only: last `StreamBuffer::total_evicted` snapshot seen per group.
    /// Used to compute `evicted_since_last_poll` in `PollResponse`.
    /// Not persisted — resets to 0 on restart, giving one cycle with delta=0.
    eviction_baselines: std::sync::RwLock<HashMap<GroupKey, u64>>,
}

impl crate::storage::RedbBacked for OffsetStore {
    fn redb_database(&self) -> &redb::Database {
        &self.db
    }
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

        let cache = image::load_cache(&db)?;

        Ok(Self {
            db,
            cache: std::sync::RwLock::new(cache),
            mutation_lock: std::sync::Mutex::new(()),
            eviction_baselines: std::sync::RwLock::new(HashMap::new()),
        })
    }

    /// Commit an offset for a specific partition.
    ///
    /// Rejects lexicographic regressions: the position must be >= the current
    /// `(index, sequence)` for this `(tenant, stream, group, partition)`. A
    /// regressing commit would redeliver acknowledged events. Re-committing
    /// the same position is accepted for idempotent retries.
    ///
    /// # Durability of the monotonicity check
    ///
    /// The check reads the in-memory `cache`, but the cache is authoritative
    /// because every successful `commit_offset` writes the new composite
    /// position to redb *before* updating the cache entry, and
    /// `OffsetStore::open` rebuilds the cache from redb on startup.
    pub fn commit_offset(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
        partition_id: u32,
        offset: impl Into<CdcOffset>,
    ) -> crate::Result<()> {
        let offset = offset.into();
        let _mutation_guard = self.mutation_lock.lock().unwrap_or_else(|p| p.into_inner());
        {
            let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
            if let Some(current) = cache
                .get(&(
                    database_id,
                    tenant_id,
                    stream.to_string(),
                    group.to_string(),
                ))
                .and_then(|m| m.get(&partition_id))
                .copied()
                && offset < current
            {
                return Err(crate::Error::OffsetRegression {
                    stream: stream.to_string(),
                    group: group.to_string(),
                    partition_id,
                    offsets: Box::new(crate::error::RegressedOffsets {
                        current,
                        attempted: offset,
                    }),
                });
            }
        }

        let key = offset_key(database_id, tenant_id, stream, group, partition_id);
        let value = encode_offset(offset);

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
            .entry((
                database_id,
                tenant_id,
                stream.to_string(),
                group.to_string(),
            ))
            .or_default()
            .insert(partition_id, offset);

        Ok(())
    }

    /// Raise committed offsets to `offsets` in one durable transaction.
    ///
    /// A partition already at or past its new position keeps its position,
    /// so a replayed or reordered commit never moves a cursor backward. The
    /// replicated `COMMIT OFFSET` applies through this on every node, where
    /// refusing a committed entry diverges the node from the quorum.
    pub fn advance_offsets(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
        offsets: &[PartitionOffset],
    ) -> crate::Result<()> {
        let _mutation_guard = self.mutation_lock.lock().unwrap_or_else(|p| p.into_inner());
        let group_key = (
            database_id,
            tenant_id,
            stream.to_string(),
            group.to_string(),
        );
        let raised: Vec<PartitionOffset> = {
            let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
            let current = cache.get(&group_key);
            offsets
                .iter()
                .filter(|offset| {
                    current
                        .and_then(|partitions| partitions.get(&offset.partition_id))
                        .is_none_or(|current| offset.committed_offset > *current)
                })
                .copied()
                .collect()
        };
        if raised.is_empty() {
            return Ok(());
        }

        let storage = |detail: String| crate::Error::Storage {
            engine: "event_plane".into(),
            detail,
        };
        let txn = self
            .db
            .begin_write()
            .map_err(|e| storage(format!("begin_write: {e}")))?;
        {
            let mut table = txn
                .open_table(OFFSETS)
                .map_err(|e| storage(format!("open_table: {e}")))?;
            for offset in &raised {
                let key = offset_key(database_id, tenant_id, stream, group, offset.partition_id);
                table
                    .insert(
                        key.as_str(),
                        encode_offset(offset.committed_offset).as_slice(),
                    )
                    .map_err(|e| storage(format!("insert: {e}")))?;
            }
        }
        txn.commit().map_err(|e| storage(format!("commit: {e}")))?;

        let mut cache = self.cache.write().unwrap_or_else(|p| p.into_inner());
        let partitions = cache.entry(group_key).or_default();
        for offset in raised {
            partitions.insert(offset.partition_id, offset.committed_offset);
        }
        Ok(())
    }

    /// Get the committed offset for a specific partition.
    /// Returns the initial position if no offset has been committed.
    pub fn get_offset(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
        partition_id: u32,
    ) -> CdcOffset {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache
            .get(&(
                database_id,
                tenant_id,
                stream.to_string(),
                group.to_string(),
            ))
            .and_then(|m| m.get(&partition_id))
            .copied()
            .unwrap_or(CdcOffset::ZERO)
    }

    /// Get all committed offsets for a group.
    pub fn get_all_offsets(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
    ) -> Vec<PartitionOffset> {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache
            .get(&(
                database_id,
                tenant_id,
                stream.to_string(),
                group.to_string(),
            ))
            .map(|m| {
                let mut offsets: Vec<PartitionOffset> = m
                    .iter()
                    .map(|(&pid, &offset)| PartitionOffset::new(pid, offset))
                    .collect();
                offsets.sort_by_key(|o| o.partition_id);
                offsets
            })
            .unwrap_or_default()
    }

    /// Delete offsets for several groups in one durable redb transaction.
    ///
    /// DROP TOPIC uses this to make cleanup all-or-nothing within the separate
    /// offset database before it commits the catalog deletion. The cache is
    /// updated only after that commit, so an I/O failure cannot leave memory
    /// claiming cleanup succeeded.
    pub fn delete_groups(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        groups: &[(String, String)],
    ) -> crate::Result<()> {
        let _mutation_guard = self.mutation_lock.lock().unwrap_or_else(|p| p.into_inner());
        let mut unique = std::collections::BTreeSet::new();
        unique.extend(groups.iter().cloned());
        let cache_keys: Vec<GroupKey> = unique
            .iter()
            .map(|(stream, group)| (database_id, tenant_id, stream.clone(), group.clone()))
            .collect();
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        let removals: Vec<(GroupKey, Vec<u32>)> = cache_keys
            .iter()
            .map(|key| {
                (
                    key.clone(),
                    cache
                        .get(key)
                        .map(|offsets| offsets.keys().copied().collect())
                        .unwrap_or_default(),
                )
            })
            .collect();
        drop(cache);

        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn.open_table(OFFSETS).map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("open_table: {e}"),
            })?;
            for (key, partitions) in &removals {
                for partition_id in partitions {
                    let durable = offset_key(database_id, tenant_id, &key.2, &key.3, *partition_id);
                    table
                        .remove(durable.as_str())
                        .map_err(|e| crate::Error::Storage {
                            engine: "event_plane".into(),
                            detail: format!("delete offset: {e}"),
                        })?;
                }
            }
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("commit offset cleanup: {e}"),
        })?;

        let mut cache = self.cache.write().unwrap_or_else(|p| p.into_inner());
        let mut baselines = self
            .eviction_baselines
            .write()
            .unwrap_or_else(|p| p.into_inner());
        for key in cache_keys {
            cache.remove(&key);
            baselines.remove(&key);
        }
        Ok(())
    }

    /// Delete all offsets for a group (on DROP CONSUMER GROUP).
    pub fn delete_group(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
    ) -> crate::Result<()> {
        self.delete_groups(
            database_id,
            tenant_id,
            &[(stream.to_owned(), group.to_owned())],
        )
    }

    // ── Eviction baseline (memory-only, for PollResponse delta tracking) ──

    /// Return the last `total_evicted` snapshot recorded for this group, and
    /// atomically replace it with `current_total`. Returns `(baseline, delta)`:
    /// - `baseline` is the previous snapshot (0 if this is the group's first poll).
    /// - `delta = current_total - baseline` is the drop count since the last poll.
    ///
    /// Called at the start of each HTTP poll so `evicted_since_last_poll` in
    /// `PollResponse` reflects exactly the events that fell out of the buffer
    /// between this poll and the previous one for this group.
    pub fn swap_eviction_baseline(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
        current_total: u64,
    ) -> u64 {
        let key = (
            database_id,
            tenant_id,
            stream.to_string(),
            group.to_string(),
        );
        let mut baselines = self
            .eviction_baselines
            .write()
            .unwrap_or_else(|p| p.into_inner());
        let baseline = baselines.insert(key, current_total).unwrap_or(0);
        current_total.saturating_sub(baseline)
    }
}

#[cfg(test)]
mod tests {
    use redb::{ReadableDatabase, ReadableTable};

    use super::*;

    #[test]
    fn commit_and_read_offset() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        let database_id = DatabaseId::new(7);
        store
            .commit_offset(database_id, 1, "orders_stream", "analytics", 0, 100)
            .unwrap();
        store
            .commit_offset(database_id, 1, "orders_stream", "analytics", 1, 200)
            .unwrap();

        assert_eq!(
            store.get_offset(database_id, 1, "orders_stream", "analytics", 0),
            100
        );
        assert_eq!(
            store.get_offset(database_id, 1, "orders_stream", "analytics", 1),
            200
        );
        assert_eq!(
            store.get_offset(database_id, 1, "orders_stream", "analytics", 99),
            0
        ); // No offset yet.
    }

    #[test]
    fn get_all_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        let database_id = DatabaseId::new(7);
        store
            .commit_offset(database_id, 1, "s", "g", 2, 200)
            .unwrap();
        store
            .commit_offset(database_id, 1, "s", "g", 0, 100)
            .unwrap();

        let offsets = store.get_all_offsets(database_id, 1, "s", "g");
        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets[0].partition_id, 0); // Sorted.
        assert_eq!(offsets[0].committed_offset, CdcOffset::whole_index(100));
        assert_eq!(offsets[1].partition_id, 2);
        assert_eq!(offsets[1].committed_offset, CdcOffset::whole_index(200));
    }

    #[test]
    fn delete_group_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        let database_id = DatabaseId::new(7);
        store
            .commit_offset(database_id, 1, "s", "g", 0, 100)
            .unwrap();
        store
            .commit_offset(database_id, 1, "s", "g", 1, 200)
            .unwrap();
        store.delete_group(database_id, 1, "s", "g").unwrap();

        assert_eq!(store.get_offset(database_id, 1, "s", "g", 0), 0);
        assert!(store.get_all_offsets(database_id, 1, "s", "g").is_empty());
    }

    #[test]
    fn topic_offset_batch_cleanup_survives_restart_and_recreate() {
        let dir = tempfile::tempdir().unwrap();
        let database_id = DatabaseId::new(7);
        {
            let store = OffsetStore::open(dir.path()).unwrap();
            for stream in ["topic:events", "events"] {
                store
                    .commit_offset(database_id, 1, stream, "analytics", 0, 100)
                    .unwrap();
            }
            store
                .delete_groups(
                    database_id,
                    1,
                    &[
                        ("topic:events".into(), "analytics".into()),
                        ("events".into(), "analytics".into()),
                    ],
                )
                .unwrap();
        }
        let recreated = OffsetStore::open(dir.path()).unwrap();
        assert_eq!(
            recreated.get_offset(database_id, 1, "topic:events", "analytics", 0),
            0
        );
        // A recreated topic/group uses the same identity but cannot inherit a
        // cursor from the removed lifecycle.
        recreated
            .commit_offset(database_id, 1, "topic:events", "analytics", 0, 1)
            .unwrap();
        assert_eq!(
            recreated.get_offset(database_id, 1, "topic:events", "analytics", 0),
            1
        );
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let database_id = DatabaseId::new(7);
        {
            let store = OffsetStore::open(dir.path()).unwrap();
            store
                .commit_offset(database_id, 1, "s", "g", 5, 999)
                .unwrap();
        }
        let store = OffsetStore::open(dir.path()).unwrap();
        assert_eq!(store.get_offset(database_id, 1, "s", "g", 5), 999);
    }

    #[test]
    fn independent_groups() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        let database_id = DatabaseId::new(7);
        store
            .commit_offset(database_id, 1, "s", "group_a", 0, 100)
            .unwrap();
        store
            .commit_offset(database_id, 1, "s", "group_b", 0, 500)
            .unwrap();

        assert_eq!(store.get_offset(database_id, 1, "s", "group_a", 0), 100);
        assert_eq!(store.get_offset(database_id, 1, "s", "group_b", 0), 500);
    }

    #[test]
    fn offsets_are_isolated_by_database() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();
        let first_db = DatabaseId::new(7);
        let second_db = DatabaseId::new(8);

        store
            .commit_offset(first_db, 1, "orders", "analytics", 0, 100)
            .unwrap();
        store
            .commit_offset(second_db, 1, "orders", "analytics", 0, 200)
            .unwrap();

        assert_eq!(store.get_offset(first_db, 1, "orders", "analytics", 0), 100);
        assert_eq!(
            store.get_offset(second_db, 1, "orders", "analytics", 0),
            200
        );
    }

    /// A subsequent commit with a position strictly less than the currently
    /// committed position must be rejected — otherwise the next poll will
    /// redeliver already-acknowledged events and break exactly-once
    /// semantics downstream.
    #[test]
    fn concurrent_commits_never_regress_durable_offset() {
        use std::sync::{Arc, Barrier};

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(OffsetStore::open(dir.path()).unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let high_store = Arc::clone(&store);
        let high_barrier = Arc::clone(&barrier);
        let high = std::thread::spawn(move || {
            high_barrier.wait();
            high_store.commit_offset(DatabaseId::new(7), 1, "s", "g", 0, 100)
        });
        let low_store = Arc::clone(&store);
        let low_barrier = Arc::clone(&barrier);
        let low = std::thread::spawn(move || {
            low_barrier.wait();
            low_store.commit_offset(DatabaseId::new(7), 1, "s", "g", 0, 1)
        });
        barrier.wait();
        let _ = high.join().unwrap();
        let _ = low.join().unwrap();

        assert_eq!(store.get_offset(DatabaseId::new(7), 1, "s", "g", 0), 100);
        drop(store);
        let reopened = OffsetStore::open(dir.path()).unwrap();
        assert_eq!(reopened.get_offset(DatabaseId::new(7), 1, "s", "g", 0), 100);
    }

    #[test]
    fn commit_offset_rejects_regression() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        let database_id = DatabaseId::new(7);
        store
            .commit_offset(database_id, 1, "s", "g", 0, 1_000_000)
            .unwrap();

        let result = store.commit_offset(database_id, 1, "s", "g", 0, 1);
        assert!(
            result.is_err(),
            "offset regression (1 after 1_000_000) must be rejected; got {result:?}"
        );
        // Committed value must not have been overwritten.
        assert_eq!(
            store.get_offset(database_id, 1, "s", "g", 0),
            1_000_000,
            "rejected commit must not clobber the stored offset"
        );
    }

    #[test]
    fn composite_offset_persists_and_rejects_sibling_regression() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();
        let database_id = DatabaseId::new(7);
        let offset = CdcOffset::new(100, 2);
        store
            .commit_offset(database_id, 1, "s", "g", 0, offset)
            .unwrap();
        let key = offset_key(database_id, 1, "s", "g", 0);
        let txn = store.db.begin_read().unwrap();
        let table = txn.open_table(OFFSETS).unwrap();
        assert_eq!(table.get(key.as_str()).unwrap().unwrap().value().len(), 24);
        assert!(
            store
                .commit_offset(database_id, 1, "s", "g", 0, CdcOffset::new(100, 1))
                .is_err()
        );
    }

    /// Committing the position already stored succeeds (idempotent retry).
    /// Only strict regressions are rejected.
    #[test]
    fn commit_offset_same_index_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();

        let database_id = DatabaseId::new(7);
        store
            .commit_offset(database_id, 1, "s", "g", 0, 500)
            .unwrap();
        store
            .commit_offset(database_id, 1, "s", "g", 0, 500)
            .expect("re-committing the same index must be accepted");
        assert_eq!(store.get_offset(database_id, 1, "s", "g", 0), 500);
    }

    #[test]
    fn advance_offsets_raises_and_never_regresses() {
        let dir = tempfile::tempdir().unwrap();
        let store = OffsetStore::open(dir.path()).unwrap();
        let database_id = DatabaseId::new(7);
        store
            .advance_offsets(
                database_id,
                1,
                "s",
                "g",
                &[
                    PartitionOffset::new(0, CdcOffset::new(10, 4)),
                    PartitionOffset::new(1, CdcOffset::new(3, 2)),
                ],
            )
            .unwrap();
        // A replayed older commit leaves partition 0 alone and raises 1.
        store
            .advance_offsets(
                database_id,
                1,
                "s",
                "g",
                &[
                    PartitionOffset::new(0, CdcOffset::new(9, 8)),
                    PartitionOffset::new(1, CdcOffset::new(5, 2)),
                ],
            )
            .unwrap();
        drop(store);

        let store = OffsetStore::open(dir.path()).unwrap();
        assert_eq!(
            store.get_offset(database_id, 1, "s", "g", 0),
            CdcOffset::new(10, 4)
        );
        assert_eq!(
            store.get_offset(database_id, 1, "s", "g", 1),
            CdcOffset::new(5, 2)
        );
    }
}
