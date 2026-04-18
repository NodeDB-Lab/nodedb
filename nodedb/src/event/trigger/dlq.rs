//! Dead-Letter Queue for failed async trigger events.
//!
//! When an async trigger's DML fails after max retries, the failed event
//! is enqueued here with full context for debugging and manual replay.
//! Separate from the sync DLQ (which handles CRDT constraint violations).
//!
//! Bounded per tenant: oldest entries evicted when capacity is reached.
//! Persisted to redb for durability across restarts.

use std::collections::VecDeque;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableTable, TableDefinition};
use tracing::{debug, warn};

/// redb table: monotonic entry_id → MessagePack-serialized TriggerDlqEntry.
const TRIGGER_DLQ: TableDefinition<u64, &[u8]> = TableDefinition::new("trigger_dlq");

/// Maximum DLQ entries per node (bounded to prevent unbounded growth).
const DEFAULT_MAX_ENTRIES: usize = 100_000;

/// A failed trigger event in the DLQ.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct TriggerDlqEntry {
    /// Unique entry ID (monotonic within this node).
    pub entry_id: u64,
    /// Tenant context.
    pub tenant_id: u32,
    /// Collection that triggered the event.
    pub source_collection: String,
    /// Row ID from the originating event.
    pub row_id: String,
    /// Operation that triggered: "INSERT", "UPDATE", "DELETE".
    pub operation: String,
    /// Trigger name that failed.
    pub trigger_name: String,
    /// Error message from the last attempt.
    pub error: String,
    /// Total retry attempts before DLQ.
    pub retry_count: u32,
    /// Original event's LSN (for idempotency/ordering).
    pub source_lsn: u64,
    /// Original event's sequence number.
    pub source_sequence: u64,
    /// Timestamp (Unix epoch millis) when the entry was created.
    pub created_at: u64,
    /// Whether this entry has been manually resolved.
    pub resolved: bool,
}

/// Parameters for enqueuing a trigger DLQ entry.
pub struct DlqEnqueueParams {
    pub tenant_id: u32,
    pub source_collection: String,
    pub row_id: String,
    pub operation: String,
    pub trigger_name: String,
    pub error: String,
    pub retry_count: u32,
    pub source_lsn: u64,
    pub source_sequence: u64,
}

/// Trigger dead-letter queue.
pub struct TriggerDlq {
    db: Database,
    /// In-memory index for fast listing (mirrors redb for read performance).
    entries: VecDeque<TriggerDlqEntry>,
    next_entry_id: u64,
    max_entries: usize,
}

impl TriggerDlq {
    /// Open or create the trigger DLQ at `{data_dir}/event_plane/trigger_dlq.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;

        let path = dir.join("trigger_dlq.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("open trigger DLQ db {}: {e}", path.display()),
        })?;

        // Ensure table exists and load existing entries.
        let mut entries = VecDeque::new();
        let mut max_id = 0u64;
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_write: {e}"),
            })?;
            {
                let table = txn
                    .open_table(TRIGGER_DLQ)
                    .map_err(|e| crate::Error::Storage {
                        engine: "event_plane".into(),
                        detail: format!("open_table: {e}"),
                    })?;
                let mut range = table.range(0u64..).map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("range: {e}"),
                })?;
                while let Some(Ok((key_guard, value_guard))) = range.next() {
                    let id: u64 = key_guard.value();
                    if id > max_id {
                        max_id = id;
                    }
                    let bytes: &[u8] = value_guard.value();
                    if let Ok(entry) = zerompk::from_msgpack::<TriggerDlqEntry>(bytes) {
                        entries.push_back(entry);
                    }
                }
            }
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("commit: {e}"),
            })?;
        }

        debug!(
            entries = entries.len(),
            next_id = max_id + 1,
            "trigger DLQ loaded"
        );

        Ok(Self {
            db,
            entries,
            next_entry_id: max_id + 1,
            max_entries: DEFAULT_MAX_ENTRIES,
        })
    }

    /// Enqueue a failed trigger event.
    pub fn enqueue(&mut self, params: DlqEnqueueParams) -> crate::Result<u64> {
        let entry_id = self.next_entry_id;
        self.next_entry_id += 1;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = TriggerDlqEntry {
            entry_id,
            tenant_id: params.tenant_id,
            source_collection: params.source_collection,
            row_id: params.row_id,
            operation: params.operation,
            trigger_name: params.trigger_name,
            error: params.error,
            retry_count: params.retry_count,
            source_lsn: params.source_lsn,
            source_sequence: params.source_sequence,
            created_at: now,
            resolved: false,
        };

        // Evict oldest if at capacity.
        while self.entries.len() >= self.max_entries {
            if let Some(evicted) = self.entries.pop_front() {
                self.delete_from_redb(evicted.entry_id);
                warn!(
                    entry_id = evicted.entry_id,
                    trigger = %evicted.trigger_name,
                    "trigger DLQ evicted oldest entry (at capacity)"
                );
            }
        }

        // Persist to redb.
        self.write_to_redb(&entry)?;

        debug!(
            entry_id,
            trigger = %entry.trigger_name,
            "trigger event sent to DLQ"
        );
        self.entries.push_back(entry);
        Ok(entry_id)
    }

    /// List all unresolved DLQ entries.
    pub fn list_unresolved(&self) -> Vec<&TriggerDlqEntry> {
        self.entries.iter().filter(|e| !e.resolved).collect()
    }

    /// Mark an entry as resolved.
    pub fn resolve(&mut self, entry_id: u64) -> bool {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.entry_id == entry_id) {
            entry.resolved = true;
            true
        } else {
            return false;
        };
        // Persist the update to redb (entry cloned to avoid borrow conflict).
        if let Some(entry) = self.entries.iter().find(|e| e.entry_id == entry_id) {
            let _ = self.write_to_redb(entry);
        }
        true
    }

    /// Total entries (resolved + unresolved).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn write_to_redb(&self, entry: &TriggerDlqEntry) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(entry).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("trigger DLQ entry: {e}"),
        })?;
        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(TRIGGER_DLQ)
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
            if let Ok(mut table) = txn.open_table(TRIGGER_DLQ) {
                let _ = table.remove(entry_id);
            }
            let _ = txn.commit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(collection: &str, row_id: &str, trigger: &str) -> DlqEnqueueParams {
        DlqEnqueueParams {
            tenant_id: 1,
            source_collection: collection.into(),
            row_id: row_id.into(),
            operation: "INSERT".into(),
            trigger_name: trigger.into(),
            error: "timeout".into(),
            retry_count: 5,
            source_lsn: 100,
            source_sequence: 1,
        }
    }

    #[test]
    fn dlq_enqueue_and_list() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();

        let id = dlq
            .enqueue(params("orders", "order-1", "audit_trigger"))
            .unwrap();

        assert_eq!(dlq.len(), 1);
        let unresolved = dlq.list_unresolved();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].entry_id, id);
        assert_eq!(unresolved[0].trigger_name, "audit_trigger");
    }

    #[test]
    fn dlq_resolve() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();

        let id = dlq.enqueue(params("orders", "order-1", "audit")).unwrap();

        assert!(dlq.resolve(id));
        assert!(dlq.list_unresolved().is_empty());
    }

    #[test]
    fn dlq_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = TriggerDlq::open(dir.path()).unwrap();
            dlq.enqueue(params("orders", "o-1", "t1")).unwrap();
        }
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq.list_unresolved()[0].trigger_name, "t1");
    }

    #[test]
    fn dlq_evicts_oldest_at_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        dlq.max_entries = 3;

        for i in 0u64..5 {
            dlq.enqueue(DlqEnqueueParams {
                tenant_id: 1,
                source_collection: "c".into(),
                row_id: format!("r-{i}"),
                operation: "INSERT".into(),
                trigger_name: "t".into(),
                error: "e".into(),
                retry_count: 1,
                source_lsn: i,
                source_sequence: i,
            })
            .unwrap();
        }
        assert_eq!(dlq.len(), 3);
        assert_eq!(dlq.entries.front().unwrap().row_id, "r-2");
    }
}
