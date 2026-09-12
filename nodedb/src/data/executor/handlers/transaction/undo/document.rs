// SPDX-License-Identifier: BUSL-1.1

//! Document-engine undo entry application logic.
//!
//! `apply_undo_document` handles document-engine undo entries. All methods
//! return `Err((entry_index, detail))` on fatal failure so the caller can
//! escalate to a typed `RollbackFailed` response.

use nodedb_types::StorageKey;
use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::sparse::btree_versioned::VersionedIndexEntry;

use super::UndoEntry;

#[derive(Clone, Copy)]
pub(super) struct UndoDocumentContext<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub entry_index: usize,
    pub collection: &'a str,
    pub document_id: &'a StorageKey,
}

impl CoreLoop {
    // ── Document ────────────────────────────────────────────────────────────

    pub(super) fn apply_undo_document(
        &mut self,
        database_id: u64,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::PutDocument {
                collection,
                document_id,
                // Rollback restores prior storage state; it emits no event, so
                // the row's client identity has no reader here.
                identity: _,
                old_value,
                bitemporal_sys_from_ms,
                bitemporal_index_tuples,
                secondary_index_added,
                secondary_index_removed,
                chain_hash_prior,
            } => {
                let ctx = UndoDocumentContext {
                    database_id,
                    tid,
                    entry_index,
                    collection: &collection,
                    document_id: &document_id,
                };
                let storage_key = document_id;
                let surrogate = document_id.surrogate();
                if let Some(sys_from_ms) = bitemporal_sys_from_ms {
                    // Bitemporal op: never wrote the non-versioned table, so
                    // physically remove the appended version row (+ its index
                    // entries) instead of a plain put/delete. `versioned_get_current`
                    // recomputes from the remaining rows, so removing the newest
                    // version restores the prior one automatically.
                    self.undo_bitemporal_write(ctx, sys_from_ms, &bitemporal_index_tuples)?;
                } else {
                    let result = if let Some(old) = old_value {
                        self.sparse
                            .put(database_id, tid, &collection, &storage_key, &old)
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    } else {
                        self.sparse
                            .delete(database_id, tid, &collection, &storage_key)
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    };
                    result.map_err(|e| {
                        error!(
                            core = self.core_id,
                            entry_index,
                            collection = %collection,
                            document_id = %document_id,
                            error = %e,
                            "transaction undo: document restore failed; shard state unknown"
                        );
                        (
                            entry_index,
                            format!("document restore on {collection}/{document_id}: {e}"),
                        )
                    })?;
                }
                // Reverse plain secondary-index mutations: undo the inserts and
                // restore the stale entries this put removed. Empty on the
                // bitemporal path (its index reversal happened in
                // `undo_bitemporal_write` above), so this is a no-op there.
                self.undo_secondary_index(ctx, &secondary_index_added, &secondary_index_removed)?;
                // Revert inverted index: remove the postings this rolled-back
                // put wrote. FATAL on failure — a rollback that leaves stale FTS
                // postings behind is the same silent-partial-success corruption
                // the primary-store restore guards against.
                self.inverted
                    .remove_document(
                        database_id,
                        crate::types::TenantId::new(tid),
                        &collection,
                        surrogate,
                    )
                    .map_err(|e| {
                        error!(
                            core = self.core_id,
                            entry_index,
                            collection = %collection,
                            document_id = %document_id,
                            error = %e,
                            "transaction undo: FTS posting removal failed; shard state unknown"
                        );
                        (
                            entry_index,
                            format!("fts posting removal on {collection}/{document_id}: {e}"),
                        )
                    })?;
                // Evict any cached copy of the reversed document. Always safe:
                // a stale hit would otherwise resurrect a rolled-back put; the
                // worst case here is a cache miss.
                self.doc_cache
                    .invalidate(database_id, tid, &collection, &storage_key);
                self.undo_chain_hash(database_id, tid, &collection, entry_index, chain_hash_prior)?;
                Ok(())
            }
            UndoEntry::DeleteDocument {
                collection,
                document_id,
                // Rollback restores prior storage state; it emits no event, so
                // the row's client identity has no reader here.
                identity: _,
                old_value,
                bitemporal_sys_from_ms,
                bitemporal_index_tuples,
                secondary_index_tuples,
                chain_hash_prior,
            } => {
                let ctx = UndoDocumentContext {
                    database_id,
                    tid,
                    entry_index,
                    collection: &collection,
                    document_id: &document_id,
                };
                let storage_key = document_id;
                let surrogate = document_id.surrogate();
                if let Some(sys_from_ms) = bitemporal_sys_from_ms {
                    self.undo_bitemporal_write(ctx, sys_from_ms, &bitemporal_index_tuples)?;
                } else {
                    self.sparse
                        .put(database_id, tid, &collection, &storage_key, &old_value)
                        .map(|_| ())
                        .map_err(|e| {
                            error!(
                                core = self.core_id,
                                entry_index,
                                collection = %collection,
                                document_id = %document_id,
                                error = %e,
                                "transaction undo: document re-insert failed; shard state unknown"
                            );
                            (
                                entry_index,
                                format!("document re-insert on {collection}/{document_id}: {e}"),
                            )
                        })?;
                }
                // Restore the plain secondary-index entries the forward delete
                // cascade removed. Empty on the bitemporal path (no plain
                // INDEXES entries there), so this is a no-op for it.
                self.undo_secondary_index(ctx, &[], &secondary_index_tuples)?;
                // Re-index the restored document into the full-text inverted
                // index. The forward delete cascade removed its postings
                // unconditionally, so a rollback that restored the row but not
                // its postings would leave it restored-but-unsearchable. FATAL
                // on failure — a half-restored FTS index is corruption.
                self.reindex_restored_document_fts(ctx, surrogate, &old_value)?;
                // Evict any cached copy of the reversed document (see the
                // PutDocument branch): reversing a delete restores the row, so a
                // stale post-delete cache entry must not linger.
                self.doc_cache
                    .invalidate(database_id, tid, &collection, &storage_key);
                self.undo_chain_hash(database_id, tid, &collection, entry_index, chain_hash_prior)?;
                Ok(())
            }
            _ => unreachable!("apply_undo_document called with non-document entry"),
        }
    }

    /// Physically reverse a bitemporal versioned write inside a single
    /// caller-owned redb write transaction: remove the version/tombstone row
    /// appended at `sys_from_ms`, plus every versioned index entry written at
    /// the same system time. redb is single-writer, so all removals share one
    /// transaction.
    fn undo_bitemporal_write(
        &self,
        ctx: UndoDocumentContext<'_>,
        sys_from_ms: i64,
        index_tuples: &[(String, String)],
    ) -> Result<(), (usize, String)> {
        let UndoDocumentContext {
            database_id,
            tid,
            entry_index,
            collection,
            document_id,
        } = ctx;
        let map_err = |stage: &str, e: String| {
            error!(
                core = self.core_id,
                entry_index,
                collection = %collection,
                document_id = %document_id,
                error = %e,
                "transaction undo: bitemporal version removal failed; shard state unknown"
            );
            (
                entry_index,
                format!("bitemporal {stage} on {collection}/{document_id}: {e}"),
            )
        };
        let txn = self
            .sparse
            .db()
            .begin_write()
            .map_err(|e| map_err("begin_write", e.to_string()))?;
        self.sparse
            .versioned_remove_in_txn(&txn, database_id, tid, collection, document_id, sys_from_ms)
            .map_err(|e| map_err("version remove", e.to_string()))?;
        for (field, value) in index_tuples {
            self.sparse
                .versioned_index_remove_in_txn(
                    &txn,
                    VersionedIndexEntry {
                        database_id,
                        tenant: tid,
                        coll: collection,
                        field,
                        value,
                        doc_id: document_id,
                        sys_from_ms,
                    },
                )
                .map_err(|e| map_err("index remove", e.to_string()))?;
        }
        txn.commit().map_err(|e| map_err("commit", e.to_string()))?;
        Ok(())
    }

    /// Reverse plain (non-bitemporal) secondary-index mutations from a
    /// rolled-back document write.
    ///
    /// `to_remove` were INSERTED on the forward path → delete them; `to_restore`
    /// were REMOVED on the forward path (stale UPDATE entries, or a DELETE's
    /// cascade) → re-insert them. Fatal on failure like the primary-store
    /// restore, so a partial index rollback surfaces as `RollbackFailed` rather
    /// than silently diverging the secondary index from the primary store.
    fn undo_secondary_index(
        &self,
        ctx: UndoDocumentContext<'_>,
        to_remove: &[(String, String)],
        to_restore: &[(String, String)],
    ) -> Result<(), (usize, String)> {
        let UndoDocumentContext {
            database_id,
            tid,
            entry_index,
            collection,
            document_id,
        } = ctx;
        let map_err = |stage: &str, e: String| {
            error!(
                core = self.core_id,
                entry_index,
                collection = %collection,
                document_id = %document_id,
                error = %e,
                "transaction undo: secondary-index reversal failed; shard state unknown"
            );
            (
                entry_index,
                format!("secondary-index {stage} on {collection}/{document_id}: {e}"),
            )
        };
        for (field, value) in to_remove {
            self.sparse
                .index_remove(database_id, tid, collection, field, value, document_id)
                .map_err(|e| map_err("remove", e.to_string()))?;
        }
        for (field, value) in to_restore {
            self.sparse
                .index_put(database_id, tid, collection, field, value, document_id)
                .map_err(|e| map_err("restore", e.to_string()))?;
        }
        Ok(())
    }

    /// Reverse a hash-chain mutation performed by a document write. `None` =
    /// the op never touched the chain; `Some(None)` = remove the key (genesis
    /// insert); `Some(Some(prev))` = restore the key to its pre-image.
    ///
    /// Reverses the DURABLE head as well as the in-memory one. The forward
    /// sub-plan committed its transaction, so the advanced head is already on
    /// disk; restoring only the map would let the next restart rehydrate the
    /// head of a rolled-back row. FATAL on failure, like the primary-store
    /// restore: a rollback that leaves the persisted head ahead of the rows is
    /// a chain that verifies as broken forever after.
    fn undo_chain_hash(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        entry_index: usize,
        chain_hash_prior: Option<Option<String>>,
    ) -> Result<(), (usize, String)> {
        let key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let persisted = match chain_hash_prior {
            None => return Ok(()),
            Some(None) => {
                self.chain_hashes.remove(&key);
                self.sparse.delete_chain_head(database_id, tid, collection)
            }
            Some(Some(prev)) => {
                self.chain_hashes.insert(key, prev.clone());
                self.sparse
                    .put_chain_head(database_id, tid, collection, &prev)
            }
        };
        persisted.map_err(|e| {
            error!(
                core = self.core_id,
                entry_index,
                collection = %collection,
                error = %e,
                "transaction undo: hash-chain head restore failed; shard state unknown"
            );
            (
                entry_index,
                format!("hash-chain head restore on {collection}: {e}"),
            )
        })
    }
}

/// Unit tests for document undo — bitemporal version reversal, hash-chain
/// reversal, and backward-compatibility of the plain (non-versioned) path.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::sparse::btree_versioned::VersionedPut;
    use crate::types::TenantId;

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn storage_key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(nodedb_types::Surrogate::new(surrogate))
    }

    fn seed_version(
        core: &crate::data::executor::core_loop::CoreLoop,
        doc: u32,
        t: i64,
        body: &[u8],
    ) {
        core.sparse
            .versioned_put(VersionedPut {
                database_id: DB,
                tenant: TID,
                coll: "c",
                doc_id: &storage_key(doc),
                sys_from_ms: t,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
                body,
            })
            .unwrap();
    }

    fn seed_index(core: &crate::data::executor::core_loop::CoreLoop, doc: u32, t: i64) {
        core.sparse
            .versioned_index_put(VersionedIndexEntry {
                database_id: DB,
                tenant: TID,
                coll: "c",
                field: "status",
                value: "active",
                doc_id: &storage_key(doc),
                sys_from_ms: t,
            })
            .unwrap();
    }

    fn index_lookup(core: &crate::data::executor::core_loop::CoreLoop) -> Vec<StorageKey> {
        core.sparse
            .versioned_index_lookup_as_of(DB, TID, "c", "status", "active", None)
            .unwrap()
    }

    #[test]
    fn bitemporal_put_undo_removes_version_and_index() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let t = 1_000;
        let d1 = storage_key(1);
        seed_version(&core, 1, t, b"v1");
        seed_index(&core, 1, t);

        assert!(
            core.sparse
                .versioned_get_current(DB, TID, "c", &d1)
                .unwrap()
                .is_some()
        );
        assert_eq!(index_lookup(&core), vec![d1]);

        let entry = UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: d1,
            identity: d1.to_identity(),
            old_value: None,
            bitemporal_sys_from_ms: Some(t),
            bitemporal_index_tuples: vec![("status".into(), "active".into())],
            secondary_index_added: Vec::new(),
            secondary_index_removed: Vec::new(),
            chain_hash_prior: None,
        };
        core.apply_undo_document(DB, TID, 0, entry).unwrap();

        assert!(
            core.sparse
                .versioned_get_current(DB, TID, "c", &d1)
                .unwrap()
                .is_none(),
            "version row must be physically gone"
        );
        assert!(index_lookup(&core).is_empty(), "index entry must be gone");
    }

    #[test]
    fn bitemporal_delete_undo_restores_prior_live_version() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        // Live version at T1, then a tombstone at T2 (plus an index tombstone).
        let d1 = storage_key(1);
        seed_version(&core, 1, 1_000, b"v1");
        seed_index(&core, 1, 1_000);
        core.sparse
            .versioned_tombstone(DB, TID, "c", &d1, 2_000)
            .unwrap();
        core.sparse
            .versioned_index_tombstone(VersionedIndexEntry {
                database_id: DB,
                tenant: TID,
                coll: "c",
                field: "status",
                value: "active",
                doc_id: &d1,
                sys_from_ms: 2_000,
            })
            .unwrap();

        // Tombstone hides the row.
        assert!(
            core.sparse
                .versioned_get_current(DB, TID, "c", &d1)
                .unwrap()
                .is_none()
        );

        let entry = UndoEntry::DeleteDocument {
            collection: "c".into(),
            document_id: d1,
            identity: d1.to_identity(),
            old_value: b"v1".to_vec(),
            bitemporal_sys_from_ms: Some(2_000),
            bitemporal_index_tuples: vec![("status".into(), "active".into())],
            secondary_index_tuples: Vec::new(),
            chain_hash_prior: None,
        };
        core.apply_undo_document(DB, TID, 0, entry).unwrap();

        // Removing the tombstone restores the prior live version as current.
        assert_eq!(
            core.sparse
                .versioned_get_current(DB, TID, "c", &d1)
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(index_lookup(&core), vec![d1]);
    }

    #[test]
    fn chain_hash_undo_restores_prior_and_removes_genesis() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = || {
            (
                crate::types::DatabaseId::new(DB),
                TenantId::new(TID),
                "c".to_string(),
            )
        };

        // Restore-to-prior case: map holds "h1", undo restores "h0".
        core.chain_hashes.insert(key(), "h1".into());
        let restore = UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: storage_key(0),
            identity: storage_key(0).to_identity(),
            old_value: None,
            bitemporal_sys_from_ms: None,
            bitemporal_index_tuples: Vec::new(),
            secondary_index_added: Vec::new(),
            secondary_index_removed: Vec::new(),
            chain_hash_prior: Some(Some("h0".into())),
        };
        core.apply_undo_document(DB, TID, 0, restore).unwrap();
        assert_eq!(
            core.chain_hashes.get(&key()).map(String::as_str),
            Some("h0")
        );

        // Genesis case: undo removes the key entirely.
        let genesis = UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: storage_key(0),
            identity: storage_key(0).to_identity(),
            old_value: None,
            bitemporal_sys_from_ms: None,
            bitemporal_index_tuples: Vec::new(),
            secondary_index_added: Vec::new(),
            secondary_index_removed: Vec::new(),
            chain_hash_prior: Some(None),
        };
        core.apply_undo_document(DB, TID, 0, genesis).unwrap();
        assert!(!core.chain_hashes.contains_key(&key()));
    }

    #[test]
    fn plain_put_undo_restores_or_removes_the_surrogate_row() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        // Overwrite case: current holds "new", undo restores "old". `undo`
        // reads the row through the entry's storage key, so the seeded row
        // and the entry share one surrogate.
        let key1 = storage_key(1);
        core.sparse.put(DB, TID, "c", &key1, b"new").unwrap();
        let overwrite = UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: key1,
            identity: key1.to_identity(),
            old_value: Some(b"old".to_vec()),
            bitemporal_sys_from_ms: None,
            bitemporal_index_tuples: Vec::new(),
            secondary_index_added: Vec::new(),
            secondary_index_removed: Vec::new(),
            chain_hash_prior: None,
        };
        core.apply_undo_document(DB, TID, 0, overwrite).unwrap();
        assert_eq!(
            core.sparse.get(DB, TID, "c", &key1).unwrap(),
            Some(b"old".to_vec())
        );

        // Insert case: undo deletes the row.
        let key2 = storage_key(2);
        core.sparse.put(DB, TID, "c", &key2, b"inserted").unwrap();
        let insert = UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: key2,
            identity: key2.to_identity(),
            old_value: None,
            bitemporal_sys_from_ms: None,
            bitemporal_index_tuples: Vec::new(),
            secondary_index_added: Vec::new(),
            secondary_index_removed: Vec::new(),
            chain_hash_prior: None,
        };
        core.apply_undo_document(DB, TID, 0, insert).unwrap();
        assert!(core.sparse.get(DB, TID, "c", &key2).unwrap().is_none());
    }

    #[test]
    fn plain_delete_undo_reinserts_the_surrogate_row() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        // Row was deleted by the forward op; undo re-inserts its prior value.
        let key1 = storage_key(1);
        let entry = UndoEntry::DeleteDocument {
            collection: "c".into(),
            document_id: key1,
            identity: key1.to_identity(),
            old_value: b"prior".to_vec(),
            bitemporal_sys_from_ms: None,
            bitemporal_index_tuples: Vec::new(),
            secondary_index_tuples: Vec::new(),
            chain_hash_prior: None,
        };
        core.apply_undo_document(DB, TID, 0, entry).unwrap();
        assert_eq!(
            core.sparse.get(DB, TID, "c", &key1).unwrap(),
            Some(b"prior".to_vec())
        );
    }
}
