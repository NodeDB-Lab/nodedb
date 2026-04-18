//! Cross-shard Dead Letter Queue.
//!
//! Failed cross-shard writes (after max retries) are persisted here.
//! Queryable via `SELECT * FROM _system.dead_letter_queue` and replayable
//! via `CALL replay_dead_letters(collection, since)`.

use std::collections::VecDeque;
use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use tracing::warn;

/// redb table: entry_id (u64) → MessagePack-serialized DlqEntry.
const CROSS_SHARD_DLQ: TableDefinition<u64, &[u8]> = TableDefinition::new("cross_shard_dlq");

/// Maximum DLQ entries per node.
const DEFAULT_MAX_ENTRIES: usize = 100_000;

/// A cross-shard write that exhausted all retries.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct CrossShardDlqEntry {
    /// Monotonic entry identifier.
    pub entry_id: u64,
    /// Tenant that owns the source data.
    pub tenant_id: u32,
    /// Collection that triggered this cross-shard write.
    pub source_collection: String,
    /// SQL statement that failed to execute on the target.
    pub sql: String,
    /// Source vShard ID.
    pub source_vshard: u16,
    /// Target vShard ID.
    pub target_vshard: u16,
    /// Target node ID.
    pub target_node: u64,
    /// Source LSN for idempotency correlation.
    pub source_lsn: u64,
    /// Source sequence number.
    pub source_sequence: u64,
    /// Last error message from the target.
    pub error: String,
    /// Total retry attempts before DLQ.
    pub retry_count: u32,
    /// Epoch ms when the entry was created.
    pub created_at: u64,
    /// Whether this entry has been resolved (replayed successfully).
    pub resolved: bool,
}

/// Parameters for enqueuing a new DLQ entry.
pub struct DlqEnqueueParams {
    pub tenant_id: u32,
    pub source_collection: String,
    pub sql: String,
    pub source_vshard: u16,
    pub target_vshard: u16,
    pub target_node: u64,
    pub source_lsn: u64,
    pub source_sequence: u64,
    pub error: String,
    pub retry_count: u32,
}

/// Cross-shard Dead Letter Queue backed by redb.
pub struct CrossShardDlq {
    db: Database,
    /// In-memory index for fast listing.
    entries: VecDeque<CrossShardDlqEntry>,
    next_entry_id: u64,
    max_entries: usize,
}

impl CrossShardDlq {
    /// Open or create the cross-shard DLQ store.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;

        let path = dir.join("cross_shard_dlq.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("open cross_shard_dlq db {}: {e}", path.display()),
        })?;

        // Ensure table exists and load existing entries.
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_write: {e}"),
            })?;
            txn.open_table(CROSS_SHARD_DLQ)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("commit: {e}"),
            })?;
        }

        let entries = Self::load_entries(&db)?;
        let next_entry_id = entries.back().map_or(1, |e| e.entry_id + 1);

        Ok(Self {
            db,
            entries,
            next_entry_id,
            max_entries: DEFAULT_MAX_ENTRIES,
        })
    }

    /// Enqueue a failed cross-shard write.
    pub fn enqueue(&mut self, params: DlqEnqueueParams) -> crate::Result<u64> {
        let entry_id = self.next_entry_id;
        self.next_entry_id += 1;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = CrossShardDlqEntry {
            entry_id,
            tenant_id: params.tenant_id,
            source_collection: params.source_collection,
            sql: params.sql,
            source_vshard: params.source_vshard,
            target_vshard: params.target_vshard,
            target_node: params.target_node,
            source_lsn: params.source_lsn,
            source_sequence: params.source_sequence,
            error: params.error,
            retry_count: params.retry_count,
            created_at: now,
            resolved: false,
        };

        self.write_to_redb(&entry)?;
        self.entries.push_back(entry);

        // Evict oldest if over capacity.
        while self.entries.len() > self.max_entries {
            if let Some(evicted) = self.entries.pop_front() {
                warn!(
                    entry_id = evicted.entry_id,
                    collection = %evicted.source_collection,
                    "cross-shard DLQ capacity exceeded, evicting oldest entry"
                );
                self.delete_from_redb(evicted.entry_id);
            }
        }

        Ok(entry_id)
    }

    /// List all unresolved DLQ entries.
    pub fn list_unresolved(&self) -> Vec<&CrossShardDlqEntry> {
        self.entries.iter().filter(|e| !e.resolved).collect()
    }

    /// List unresolved entries for a specific collection, optionally since a timestamp.
    pub fn list_for_collection(
        &self,
        collection: &str,
        since_epoch_ms: Option<u64>,
    ) -> Vec<&CrossShardDlqEntry> {
        self.entries
            .iter()
            .filter(|e| {
                !e.resolved
                    && e.source_collection == collection
                    && since_epoch_ms.is_none_or(|since| e.created_at >= since)
            })
            .collect()
    }

    /// Mark an entry as resolved (successfully replayed).
    pub fn resolve(&mut self, entry_id: u64) -> bool {
        let Some(entry) = self.entries.iter_mut().find(|e| e.entry_id == entry_id) else {
            return false;
        };
        entry.resolved = true;
        let cloned = entry.clone();
        let _ = self.write_to_redb(&cloned);
        true
    }

    /// Number of entries (including resolved).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Number of unresolved entries.
    pub fn unresolved_count(&self) -> usize {
        self.entries.iter().filter(|e| !e.resolved).count()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get entries ready for replay (unresolved, for a collection, since a time).
    /// Returns owned clones suitable for async replay.
    pub fn replay_candidates(
        &self,
        collection: &str,
        since_epoch_ms: u64,
    ) -> Vec<CrossShardDlqEntry> {
        self.entries
            .iter()
            .filter(|e| {
                !e.resolved && e.source_collection == collection && e.created_at >= since_epoch_ms
            })
            .cloned()
            .collect()
    }

    fn write_to_redb(&self, entry: &CrossShardDlqEntry) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(entry).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("cross_shard_dlq: {e}"),
        })?;

        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(CROSS_SHARD_DLQ)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            table
                .insert(entry.entry_id, bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("insert: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(())
    }

    fn delete_from_redb(&self, entry_id: u64) {
        if let Ok(txn) = self.db.begin_write() {
            if let Ok(mut table) = txn.open_table(CROSS_SHARD_DLQ) {
                let _ = table.remove(entry_id);
            }
            let _ = txn.commit();
        }
    }

    fn load_entries(db: &Database) -> crate::Result<VecDeque<CrossShardDlqEntry>> {
        let txn = db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_read: {e}"),
        })?;
        let table = txn
            .open_table(CROSS_SHARD_DLQ)
            .map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("open_table: {e}"),
            })?;

        let mut entries = VecDeque::new();
        let iter = table.iter().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("iter: {e}"),
        })?;
        for result in iter {
            let guard = result.map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("entry: {e}"),
            })?;
            let value_bytes: &[u8] = guard.1.value();
            match zerompk::from_msgpack::<CrossShardDlqEntry>(value_bytes) {
                Ok(entry) => entries.push_back(entry),
                Err(e) => {
                    warn!(error = %e, "skipping corrupt cross-shard DLQ entry");
                }
            }
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_params(collection: &str, lsn: u64) -> DlqEnqueueParams {
        DlqEnqueueParams {
            tenant_id: 1,
            source_collection: collection.into(),
            sql: format!("INSERT INTO audit (lsn) VALUES ({lsn})"),
            source_vshard: 3,
            target_vshard: 7,
            target_node: 2,
            source_lsn: lsn,
            source_sequence: lsn,
            error: "shard unavailable".into(),
            retry_count: 5,
        }
    }

    #[test]
    fn enqueue_and_list() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();

        let id = dlq.enqueue(make_params("orders", 100)).unwrap();
        assert_eq!(id, 1);
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq.unresolved_count(), 1);

        let unresolved = dlq.list_unresolved();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].source_lsn, 100);
    }

    #[test]
    fn resolve_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();

        let id = dlq.enqueue(make_params("orders", 100)).unwrap();
        assert!(dlq.resolve(id));
        assert_eq!(dlq.unresolved_count(), 0);
        assert_eq!(dlq.len(), 1); // Still present, just resolved.
    }

    #[test]
    fn list_for_collection() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();

        dlq.enqueue(make_params("orders", 100)).unwrap();
        dlq.enqueue(make_params("users", 200)).unwrap();
        dlq.enqueue(make_params("orders", 300)).unwrap();

        let orders = dlq.list_for_collection("orders", None);
        assert_eq!(orders.len(), 2);
        let users = dlq.list_for_collection("users", None);
        assert_eq!(users.len(), 1);
    }

    #[test]
    fn replay_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();

        dlq.enqueue(make_params("orders", 100)).unwrap();
        dlq.enqueue(make_params("orders", 200)).unwrap();

        // All candidates (since epoch 0).
        let candidates = dlq.replay_candidates("orders", 0);
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn persistence_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = CrossShardDlq::open(dir.path()).unwrap();
            dlq.enqueue(make_params("orders", 100)).unwrap();
            dlq.enqueue(make_params("orders", 200)).unwrap();
        }
        let dlq = CrossShardDlq::open(dir.path()).unwrap();
        assert_eq!(dlq.len(), 2);
        assert_eq!(dlq.unresolved_count(), 2);
    }

    #[test]
    fn eviction_on_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();
        dlq.max_entries = 3;

        for i in 1..=5 {
            dlq.enqueue(make_params("orders", i * 100)).unwrap();
        }
        assert_eq!(dlq.len(), 3);
        // Oldest entries evicted.
        let entries: Vec<u64> = dlq.list_unresolved().iter().map(|e| e.source_lsn).collect();
        assert_eq!(entries, vec![300, 400, 500]);
    }
}
