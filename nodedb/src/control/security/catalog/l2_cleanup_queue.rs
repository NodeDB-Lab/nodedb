// SPDX-License-Identifier: BUSL-1.1

//! L2 (object-store) cleanup queue — post-purge delete backlog.
//!
//! When a collection hard-delete commits, any columnar/vector/FTS
//! segments that lived on L2 (S3/GCS/Azure) must be deleted from the
//! object store. That delete call is not part of the
//! `PurgeCollection` commit path (it can take seconds, it can fail
//! transiently, and the StoredCollection row must be reaped
//! regardless), so each node persists one entry per
//! `(tenant_id, collection)` into this queue. A Tokio worker drains
//! it: on success the entry is removed; on failure `last_error` and
//! `attempts` are updated so operators can see via
//! `_system.l2_cleanup_queue` why an entry is stuck.
//!
//! Surface: `SystemCatalog::{enqueue,list,update_attempt,remove}_l2_cleanup`.

use redb::ReadableTable;

use super::types::{L2_CLEANUP_QUEUE, SystemCatalog, catalog_err};

/// One queue entry: "L2 delete for this collection is still owed".
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map, allow_unknown_fields)]
pub struct StoredL2CleanupEntry {
    pub database_id: u64,
    pub tenant_id: u64,
    pub name: String,
    /// WAL LSN at which the hard-delete committed. Used for ordering +
    /// operator diagnostics.
    pub purge_lsn: u64,
    /// Unix-epoch nanoseconds when this entry was created.
    pub enqueued_at_ns: u64,
    /// Best-effort estimate of L2 bytes still to delete. 0 if unknown.
    #[msgpack(default)]
    pub bytes_pending: u64,
    /// Last error the worker observed, if any. Empty = healthy.
    #[msgpack(default)]
    pub last_error: String,
    /// Number of delete attempts this entry has survived.
    #[msgpack(default)]
    pub attempts: u32,
}

impl SystemCatalog {
    /// Add or refresh a queue entry. Idempotent: re-enqueuing the same
    /// `(tenant_id, name)` replaces the previous entry (so retries
    /// after a restart don't pile up duplicate backlog rows).
    pub fn enqueue_l2_cleanup(&self, entry: &StoredL2CleanupEntry) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(entry)
            .map_err(|e| catalog_err("encode l2_cleanup entry", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("enqueue_l2_cleanup txn", e))?;
        {
            let mut table = txn
                .open_table(L2_CLEANUP_QUEUE)
                .map_err(|e| catalog_err("open l2_cleanup_queue", e))?;
            table
                .insert(
                    (entry.database_id, entry.tenant_id, entry.name.as_str()),
                    bytes.as_slice(),
                )
                .map_err(|e| catalog_err("insert l2_cleanup entry", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit l2_cleanup enqueue", e))
    }

    /// Load every queued entry. Callers that only need per-tenant
    /// views should filter the result (the queue is small enough that
    /// full scan is fine).
    pub fn load_l2_cleanup_queue(&self) -> crate::Result<Vec<StoredL2CleanupEntry>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_l2_cleanup read txn", e))?;
        let table = txn
            .open_table(L2_CLEANUP_QUEUE)
            .map_err(|e| catalog_err("open l2_cleanup_queue", e))?;
        let mut out = Vec::new();
        for item in table
            .range::<(u64, u64, &str)>(..)
            .map_err(|e| catalog_err("range l2_cleanup", e))?
        {
            let (_, v) = item.map_err(|e| catalog_err("read l2_cleanup", e))?;
            let entry: StoredL2CleanupEntry = zerompk::from_msgpack(v.value())
                .map_err(|e| catalog_err("decode l2_cleanup entry", e))?;
            out.push(entry);
        }
        Ok(out)
    }

    /// Record a failed delete attempt: bump `attempts`, store the error
    /// text. No-op if the entry has already been removed.
    pub fn record_l2_cleanup_attempt(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        last_error: &str,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("record_l2_cleanup_attempt txn", e))?;
        {
            let mut table = txn
                .open_table(L2_CLEANUP_QUEUE)
                .map_err(|e| catalog_err("open l2_cleanup_queue", e))?;
            let existing = table
                .get((database_id, tenant_id, name))
                .map_err(|e| catalog_err("get l2_cleanup entry", e))?
                .map(|g| g.value().to_vec());
            let Some(raw) = existing else { return Ok(()) };
            let mut entry: StoredL2CleanupEntry = zerompk::from_msgpack(&raw)
                .map_err(|e| catalog_err("decode l2_cleanup entry", e))?;
            entry.attempts = entry.attempts.saturating_add(1);
            entry.last_error = last_error.to_string();
            let bytes = zerompk::to_msgpack_vec(&entry)
                .map_err(|e| catalog_err("encode l2_cleanup entry", e))?;
            table
                .insert((database_id, tenant_id, name), bytes.as_slice())
                .map_err(|e| catalog_err("update l2_cleanup entry", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit l2_cleanup attempt", e))
    }

    /// Remove a successfully-drained entry. Idempotent.
    pub fn remove_l2_cleanup(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("remove_l2_cleanup txn", e))?;
        {
            let mut table = txn
                .open_table(L2_CLEANUP_QUEUE)
                .map_err(|e| catalog_err("open l2_cleanup_queue", e))?;
            table
                .remove((database_id, tenant_id, name))
                .map_err(|e| catalog_err("remove l2_cleanup entry", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit l2_cleanup remove", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const DB: u64 = 0;

    fn cat() -> (SystemCatalog, TempDir) {
        let tmp = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&tmp.path().join("system.redb")).unwrap();
        (cat, tmp)
    }

    fn entry(tenant: u64, name: &str, lsn: u64, bytes: u64) -> StoredL2CleanupEntry {
        StoredL2CleanupEntry {
            database_id: DB,
            tenant_id: tenant,
            name: name.to_string(),
            purge_lsn: lsn,
            enqueued_at_ns: 100,
            bytes_pending: bytes,
            last_error: String::new(),
            attempts: 0,
        }
    }

    #[test]
    fn enqueue_then_load_roundtrip() {
        let (c, _t) = cat();
        c.enqueue_l2_cleanup(&entry(1, "events", 500, 2_000))
            .unwrap();
        c.enqueue_l2_cleanup(&entry(2, "logs", 600, 10_000))
            .unwrap();
        let mut all = c.load_l2_cleanup_queue().unwrap();
        all.sort_by_key(|e| (e.tenant_id, e.name.clone()));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].tenant_id, 1);
        assert_eq!(all[0].bytes_pending, 2_000);
        assert_eq!(all[1].name, "logs");
        assert_eq!(all[1].purge_lsn, 600);
    }

    #[test]
    fn enqueue_is_idempotent_per_database_scoped_key() {
        let (c, _t) = cat();
        c.enqueue_l2_cleanup(&entry(1, "events", 500, 2_000))
            .unwrap();
        c.enqueue_l2_cleanup(&entry(1, "events", 700, 9_000))
            .unwrap();
        let mut other_database = entry(1, "events", 800, 4_000);
        other_database.database_id = 1;
        c.enqueue_l2_cleanup(&other_database).unwrap();
        let all = c.load_l2_cleanup_queue().unwrap();
        assert_eq!(all.len(), 2);
        let default_entry = all.iter().find(|entry| entry.database_id == DB).unwrap();
        assert_eq!(default_entry.purge_lsn, 700);
        assert_eq!(default_entry.bytes_pending, 9_000);
        let other_entry = all.iter().find(|entry| entry.database_id == 1).unwrap();
        assert_eq!(other_entry.purge_lsn, 800);
    }

    #[test]
    fn record_attempt_updates_in_place() {
        let (c, _t) = cat();
        c.enqueue_l2_cleanup(&entry(1, "events", 500, 2_000))
            .unwrap();
        c.record_l2_cleanup_attempt(DB, 1, "events", "s3: 503")
            .unwrap();
        c.record_l2_cleanup_attempt(DB, 1, "events", "s3: 503")
            .unwrap();
        let all = c.load_l2_cleanup_queue().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].attempts, 2);
        assert_eq!(all[0].last_error, "s3: 503");
    }

    #[test]
    fn record_attempt_on_missing_is_noop() {
        let (c, _t) = cat();
        c.record_l2_cleanup_attempt(DB, 1, "missing", "err")
            .unwrap();
        assert_eq!(c.load_l2_cleanup_queue().unwrap().len(), 0);
    }

    #[test]
    fn remove_drops_entry() {
        let (c, _t) = cat();
        c.enqueue_l2_cleanup(&entry(1, "events", 500, 0)).unwrap();
        c.remove_l2_cleanup(DB, 1, "events").unwrap();
        assert_eq!(c.load_l2_cleanup_queue().unwrap().len(), 0);
        // Idempotent.
        c.remove_l2_cleanup(DB, 1, "events").unwrap();
    }
}
