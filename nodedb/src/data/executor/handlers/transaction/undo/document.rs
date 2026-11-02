// SPDX-License-Identifier: BUSL-1.1

//! Document-engine undo entry application logic.
//!
//! `apply_undo_document` handles document-engine undo entries. All methods
//! return an [`UndoError`] on fatal failure, and the caller escalates it to
//! a typed `RollbackFailed` response.

use nodedb_types::StorageKey;
use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::sparse::btree_versioned::VersionedIndexEntry;

use super::{UndoEntry, UndoError};

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
    ) -> Result<(), UndoError> {
        match entry {
            UndoEntry::PutDocument {
                collection,
                document_id,
                version_sys_from,
                index_tuples,
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
                // Physically remove the appended version and its index
                // entries. The row's current version is the newest remaining
                // one, so removing the put's version restores the prior one.
                self.undo_version_write(ctx, version_sys_from, &index_tuples)?;
                // Revert inverted index: the text version this rolled-back put
                // started leaves, and the version it ended is live again.
                // FATAL on failure — a rollback that leaves stale FTS postings
                // behind is the same silent-partial-success corruption the
                // primary-store restore guards against.
                self.inverted
                    .undo_text_version(
                        database_id,
                        crate::types::TenantId::new(tid),
                        &collection,
                        surrogate,
                        version_sys_from,
                    )
                    .map_err(|e| {
                        let err = UndoError::failed(
                            entry_index,
                            format!("fts posting removal on {collection}/{document_id}"),
                            e,
                        );
                        error!(
                            core = self.core_id,
                            entry_index,
                            collection = %collection,
                            document_id = %document_id,
                            error = %err,
                            "transaction undo: FTS posting removal failed; shard state unknown"
                        );
                        err
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
                old_value,
                version_sys_from,
                index_tuples,
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
                // Removing the tombstone makes the deleted version current
                // again, and removing the index tombstones makes its values
                // findable again.
                self.undo_version_write(ctx, version_sys_from, &index_tuples)?;
                // Restore the text version the forward delete ended, so the
                // restored row is searchable and its history is whole again.
                // FATAL on failure — a half-restored FTS index is corruption.
                self.restore_deleted_document_fts(ctx, surrogate, &old_value, version_sys_from)?;
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

    /// Physically reverse a versioned write in one redb write transaction:
    /// remove the version or tombstone appended at `sys_from`, plus every
    /// index entry written at the same system time. redb is single-writer, so
    /// all removals share one transaction.
    pub(super) fn undo_version_write(
        &self,
        ctx: UndoDocumentContext<'_>,
        sys_from: i64,
        index_tuples: &[(String, String)],
    ) -> Result<(), UndoError> {
        let UndoDocumentContext {
            database_id,
            tid,
            entry_index,
            collection,
            document_id,
        } = ctx;
        let map_err = |stage: &str, e: crate::Error| {
            let err = UndoError::failed(
                entry_index,
                format!("version {stage} on {collection}/{document_id}"),
                e,
            );
            error!(
                core = self.core_id,
                entry_index,
                collection = %collection,
                document_id = %document_id,
                error = %err,
                "transaction undo: version removal failed; shard state unknown"
            );
            err
        };
        let txn = self
            .sparse
            .begin_write()
            .map_err(|e| map_err("begin_write", e))?;
        self.sparse
            .versioned_remove_in_txn(&txn, database_id, tid, collection, document_id, sys_from)
            .map_err(|e| map_err("version remove", e))?;
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
                        sys_from,
                    },
                )
                .map_err(|e| map_err("index remove", e))?;
        }
        txn.commit().map_err(|e| {
            map_err(
                "commit",
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: e.to_string(),
                },
            )
        })?;
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
    pub(super) fn undo_chain_hash(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        entry_index: usize,
        chain_hash_prior: Option<Option<crate::types::hash_chain::ChainHead>>,
    ) -> Result<(), UndoError> {
        let key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let persisted = match chain_hash_prior {
            None => return Ok(()),
            Some(None) => {
                self.chain.heads.remove(&key);
                self.sparse.delete_chain_head(database_id, tid, collection)
            }
            Some(Some(prev)) => {
                self.chain.heads.insert(key, prev.clone());
                self.sparse
                    .put_chain_head(database_id, tid, collection, &prev)
            }
        };
        persisted.map_err(|e| {
            let err = UndoError::failed(
                entry_index,
                format!("hash-chain head restore on {collection}"),
                e,
            );
            error!(
                core = self.core_id,
                entry_index,
                collection = %collection,
                error = %err,
                "transaction undo: hash-chain head restore failed; shard state unknown"
            );
            err
        })
    }
}

/// Unit tests for document undo: version reversal and hash-chain reversal.
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
                sys_from: t,
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
                sys_from: t,
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
            version_sys_from: t,
            index_tuples: vec![("status".into(), "active".into())],
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
                sys_from: 2_000,
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
            old_value: b"v1".to_vec(),
            version_sys_from: 2_000,
            index_tuples: vec![("status".into(), "active".into())],
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

        let head = |seq: u64, hash: &str| crate::types::hash_chain::ChainHead {
            seq,
            hash: hash.to_string(),
        };
        // Restore-to-prior case: map holds "h1", undo restores "h0".
        core.chain.heads.insert(key(), head(2, "h1"));
        let restore = UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: storage_key(0),
            version_sys_from: 1,
            index_tuples: Vec::new(),
            chain_hash_prior: Some(Some(head(1, "h0"))),
        };
        core.apply_undo_document(DB, TID, 0, restore).unwrap();
        assert_eq!(core.chain.heads.get(&key()), Some(&head(1, "h0")));

        // Genesis case: undo removes the key entirely.
        let genesis = UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: storage_key(0),
            version_sys_from: 1,
            index_tuples: Vec::new(),
            chain_hash_prior: Some(None),
        };
        core.apply_undo_document(DB, TID, 0, genesis).unwrap();
        assert!(!core.chain.heads.contains_key(&key()));
    }

    #[test]
    fn put_undo_restores_the_prior_version_or_removes_the_row() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        // Overwrite case: "old" at 10, the put's "new" at 20. Undo removes
        // the version at 20, so "old" is current again. `undo` reads the row
        // through the entry's storage key, so the seeded row and the entry
        // share one surrogate.
        let key1 = storage_key(1);
        core.sparse.put(DB, TID, "c", &key1, b"old", 10).unwrap();
        core.sparse.put(DB, TID, "c", &key1, b"new", 20).unwrap();
        let overwrite = UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: key1,
            version_sys_from: 20,
            index_tuples: Vec::new(),
            chain_hash_prior: None,
        };
        core.apply_undo_document(DB, TID, 0, overwrite).unwrap();
        assert_eq!(
            core.sparse.get(DB, TID, "c", &key1).unwrap(),
            Some(b"old".to_vec())
        );
        assert_eq!(
            core.sparse
                .versioned_latest_sys_from(DB, TID, "c", &key1)
                .unwrap(),
            Some(10),
            "the rolled-back version leaves no trace in history"
        );

        // Insert case: undo removes the only version.
        let key2 = storage_key(2);
        core.sparse
            .put(DB, TID, "c", &key2, b"inserted", 30)
            .unwrap();
        let insert = UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: key2,
            version_sys_from: 30,
            index_tuples: Vec::new(),
            chain_hash_prior: None,
        };
        core.apply_undo_document(DB, TID, 0, insert).unwrap();
        assert!(core.sparse.get(DB, TID, "c", &key2).unwrap().is_none());
        assert_eq!(
            core.sparse
                .versioned_latest_sys_from(DB, TID, "c", &key2)
                .unwrap(),
            None
        );
    }

    #[test]
    fn delete_undo_removes_the_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let key1 = storage_key(1);
        core.sparse.put(DB, TID, "c", &key1, b"prior", 10).unwrap();
        core.sparse.delete(DB, TID, "c", &key1, 20).unwrap();
        let entry = UndoEntry::DeleteDocument {
            collection: "c".into(),
            document_id: key1,
            old_value: b"prior".to_vec(),
            version_sys_from: 20,
            index_tuples: Vec::new(),
            chain_hash_prior: None,
        };
        core.apply_undo_document(DB, TID, 0, entry).unwrap();
        assert_eq!(
            core.sparse.get(DB, TID, "c", &key1).unwrap(),
            Some(b"prior".to_vec())
        );
    }
}
