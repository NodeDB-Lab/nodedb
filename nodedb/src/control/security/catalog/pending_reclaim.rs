// SPDX-License-Identifier: BUSL-1.1

//! Pending engine-reclaim queue — post-drop storage-purge backlog.
//!
//! When a collection DROP commits, its catalog row is removed at apply
//! and every node then runs the redb + versioned engine purge
//! (`clear_collection_all_engines`, dispatched via
//! `MetaOp::UnregisterCollection`). That purge is correctness-critical:
//! if it fails on a node while the catalog row is already gone, engine
//! rows survive behind a gone catalog row → permanent divergence that
//! resurrects the dropped collection's history when the name is
//! re-CREATEd over the same key prefix.
//!
//! The engine purge is therefore run as a RESULT-CHECKED step. On
//! failure the `(tenant_id, collection)` is persisted here instead of
//! being warn-logged and forgotten. A Tokio worker
//! (`event::collection_gc::pending_reclaim`) drains it: it re-runs the
//! engine purge and, on success, removes the entry; on failure it bumps
//! `attempts` and stores `last_error` so operators can see via
//! `_system.pending_reclaim` why an entry is stuck. A boot-time drain
//! completes any purge left outstanding by a crash.
//!
//! Surface: `SystemCatalog::{enqueue,load,record_attempt,remove}_pending_reclaim`.
//! Structure mirrors `l2_cleanup_queue.rs`.

use redb::ReadableTable;

use super::types::{PENDING_RECLAIM, SystemCatalog, catalog_err};

/// One queue entry: "engine storage purge for this collection is still owed".
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map, allow_unknown_fields)]
pub struct StoredPendingReclaim {
    /// Database the collection lived in. The purge dispatch keys the
    /// Data-Plane vShard by this id.
    #[msgpack(default)]
    pub database_id: u64,
    pub tenant_id: u64,
    pub name: String,
    /// WAL LSN at which the DROP's `PurgeCollection` committed. Passed
    /// back to the engine purge so replay-shadowing stays consistent.
    pub purge_lsn: u64,
    /// Unix-epoch nanoseconds when this entry was first recorded.
    pub enqueued_at_ns: u64,
    /// Last error the worker observed, if any. Empty = not yet retried.
    #[msgpack(default)]
    pub last_error: String,
    /// Number of purge attempts this entry has survived (post-first-failure).
    #[msgpack(default)]
    pub attempts: u32,
}

fn pending_key(database_id: u64, tenant_id: u64, name: &str) -> String {
    format!("{database_id}:{tenant_id}:{name}")
}

impl SystemCatalog {
    /// Add or refresh a queue entry. Idempotent: re-recording the same
    /// `(tenant_id, name)` replaces the previous entry so repeated
    /// failures for one collection don't pile up duplicate backlog rows.
    pub fn enqueue_pending_reclaim(&self, entry: &StoredPendingReclaim) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(entry)
            .map_err(|e| catalog_err("encode pending_reclaim entry", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("enqueue_pending_reclaim txn", e))?;
        {
            let mut table = txn
                .open_table(PENDING_RECLAIM)
                .map_err(|e| catalog_err("open pending_reclaim", e))?;
            let key = pending_key(entry.database_id, entry.tenant_id, &entry.name);
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert pending_reclaim entry", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit pending_reclaim enqueue", e))
    }

    /// Load every queued entry. The queue is small (one row per node per
    /// failed drop) so a full scan is fine.
    pub fn load_pending_reclaim_queue(&self) -> crate::Result<Vec<StoredPendingReclaim>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_pending_reclaim read txn", e))?;
        let table = txn
            .open_table(PENDING_RECLAIM)
            .map_err(|e| catalog_err("open pending_reclaim", e))?;
        let mut out = Vec::new();
        for item in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range pending_reclaim", e))?
        {
            let (_, v) = item.map_err(|e| catalog_err("read pending_reclaim", e))?;
            let entry: StoredPendingReclaim = zerompk::from_msgpack(v.value())
                .map_err(|e| catalog_err("decode pending_reclaim entry", e))?;
            out.push(entry);
        }
        Ok(out)
    }

    /// Record a failed purge attempt: bump `attempts`, store the error
    /// text. No-op if the entry has already been removed.
    pub fn record_pending_reclaim_attempt(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        last_error: &str,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("record_pending_reclaim_attempt txn", e))?;
        {
            let mut table = txn
                .open_table(PENDING_RECLAIM)
                .map_err(|e| catalog_err("open pending_reclaim", e))?;
            let key = pending_key(database_id, tenant_id, name);
            let existing = table
                .get(key.as_str())
                .map_err(|e| catalog_err("get pending_reclaim entry", e))?
                .map(|g| g.value().to_vec());
            let Some(raw) = existing else { return Ok(()) };
            let mut entry: StoredPendingReclaim = zerompk::from_msgpack(&raw)
                .map_err(|e| catalog_err("decode pending_reclaim entry", e))?;
            entry.attempts = entry.attempts.saturating_add(1);
            entry.last_error = last_error.to_string();
            let bytes = zerompk::to_msgpack_vec(&entry)
                .map_err(|e| catalog_err("encode pending_reclaim entry", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("update pending_reclaim entry", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit pending_reclaim attempt", e))
    }

    /// Remove a successfully-drained entry. Idempotent.
    pub fn remove_pending_reclaim(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("remove_pending_reclaim txn", e))?;
        {
            let mut table = txn
                .open_table(PENDING_RECLAIM)
                .map_err(|e| catalog_err("open pending_reclaim", e))?;
            let key = pending_key(database_id, tenant_id, name);
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove pending_reclaim entry", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit pending_reclaim remove", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn cat() -> (SystemCatalog, TempDir) {
        let tmp = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&tmp.path().join("system.redb")).unwrap();
        (cat, tmp)
    }

    fn entry(tenant: u64, name: &str, lsn: u64) -> StoredPendingReclaim {
        StoredPendingReclaim {
            database_id: 0,
            tenant_id: tenant,
            name: name.to_string(),
            purge_lsn: lsn,
            enqueued_at_ns: 100,
            last_error: String::new(),
            attempts: 0,
        }
    }

    #[test]
    fn enqueue_then_load_roundtrip() {
        let (c, _t) = cat();
        c.enqueue_pending_reclaim(&entry(1, "events", 500)).unwrap();
        c.enqueue_pending_reclaim(&entry(2, "logs", 600)).unwrap();
        let mut all = c.load_pending_reclaim_queue().unwrap();
        all.sort_by_key(|e| (e.tenant_id, e.name.clone()));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].tenant_id, 1);
        assert_eq!(all[0].purge_lsn, 500);
        assert_eq!(all[1].name, "logs");
    }

    #[test]
    fn same_collection_name_in_two_databases_has_distinct_entries() {
        let (c, _t) = cat();
        let default = entry(1, "events", 500);
        let mut other = entry(1, "events", 600);
        other.database_id = 9;
        c.enqueue_pending_reclaim(&default).unwrap();
        c.enqueue_pending_reclaim(&other).unwrap();
        let all = c.load_pending_reclaim_queue().unwrap();
        assert_eq!(all.len(), 2);
        assert!(all.iter().any(|entry| entry.database_id == 0));
        assert!(all.iter().any(|entry| entry.database_id == 9));
    }

    #[test]
    fn enqueue_is_idempotent_per_key() {
        let (c, _t) = cat();
        c.enqueue_pending_reclaim(&entry(1, "events", 500)).unwrap();
        c.enqueue_pending_reclaim(&entry(1, "events", 700)).unwrap();
        let all = c.load_pending_reclaim_queue().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].purge_lsn, 700);
    }

    #[test]
    fn record_attempt_updates_in_place() {
        let (c, _t) = cat();
        c.enqueue_pending_reclaim(&entry(1, "events", 500)).unwrap();
        c.record_pending_reclaim_attempt(0, 1, "events", "dp: timeout")
            .unwrap();
        c.record_pending_reclaim_attempt(0, 1, "events", "dp: timeout")
            .unwrap();
        let all = c.load_pending_reclaim_queue().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].attempts, 2);
        assert_eq!(all[0].last_error, "dp: timeout");
    }

    #[test]
    fn record_attempt_on_missing_is_noop() {
        let (c, _t) = cat();
        c.record_pending_reclaim_attempt(0, 1, "missing", "err")
            .unwrap();
        assert_eq!(c.load_pending_reclaim_queue().unwrap().len(), 0);
    }

    #[test]
    fn remove_drops_entry() {
        let (c, _t) = cat();
        c.enqueue_pending_reclaim(&entry(1, "events", 500)).unwrap();
        c.remove_pending_reclaim(0, 1, "events").unwrap();
        assert_eq!(c.load_pending_reclaim_queue().unwrap().len(), 0);
        // Idempotent.
        c.remove_pending_reclaim(0, 1, "events").unwrap();
    }
}
