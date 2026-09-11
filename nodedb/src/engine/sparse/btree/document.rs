// SPDX-License-Identifier: BUSL-1.1

//! Primary-document CRUD on the non-versioned `DOCUMENTS` table.
//!
//! Owns point reads and writes in both flavours — autocommit (`put`, `get`,
//! `delete`, `batch_put`) and transaction-scoped (`put_in_txn`, `get_in_txn`,
//! `delete_in_txn`, `exists_in_txn`), where the caller owns the redb write
//! transaction — plus the collection-wide byte-size estimate.

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable, WriteTransaction};
use tracing::debug;

use super::engine::SparseEngine;
use super::keys::{coll_prefix, with_tenant_key};
use super::tables::{DOCUMENTS, redb_err};

impl SparseEngine {
    /// Insert or update a document (tenant-scoped).
    ///
    /// Returns the prior bytes when this write replaced an existing document,
    /// or `None` when it was a fresh insert. Callers thread the prior value
    /// into Event Plane emission so the `WriteOp` tag (Insert vs Update)
    /// reflects the actual mutation — there is no separate probe.
    pub fn put(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
        value: &[u8],
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let write_txn = self
                .db
                .begin_write()
                .map_err(|e| redb_err("write txn", e))?;
            let prior = {
                let mut table = write_txn
                    .open_table(DOCUMENTS)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .insert(key, value)
                    .map_err(|e| redb_err("insert", e))?
                    .map(|g| g.value().to_vec())
            };
            write_txn.commit().map_err(|e| redb_err("commit", e))?;

            debug!(collection, %document_id, len = value.len(), "document put");
            Ok(prior)
        })
    }

    /// Insert or update a document within an externally-owned write transaction.
    /// Same prior-bytes semantics as [`SparseEngine::put`].
    pub fn put_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
        value: &[u8],
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let mut table = txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;
            let prior = table
                .insert(key, value)
                .map_err(|e| redb_err("insert", e))?
                .map(|g| g.value().to_vec());
            Ok(prior)
        })
    }

    /// Check whether a document exists within an externally-owned write
    /// transaction — the probe used by INSERT-with-unique-PK semantics.
    ///
    /// Uses the caller's write txn so the check is linearizable with the
    /// subsequent `put_in_txn`: no other writer can slip a row in between
    /// the "does it exist" read and the insert commit. Returns `Ok(true)`
    /// if a document with this (tenant, collection, document_id) is
    /// already present.
    pub fn exists_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> crate::Result<bool> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let table = txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;
            match table.get(key) {
                Ok(Some(_)) => Ok(true),
                Ok(None) => Ok(false),
                Err(e) => Err(redb_err("exists_in_txn", e)),
            }
        })
    }

    /// Point lookup inside a caller-owned write transaction.
    ///
    /// Unlike [`SparseEngine::get`], which opens its own read transaction and
    /// therefore observes the last COMMITTED state, this reads through the
    /// caller's write transaction and so sees that transaction's own uncommitted
    /// writes. A read-modify-write that runs more than once against the same row
    /// inside one transaction — a derived running total accumulating two deltas,
    /// for instance — must read this way or the second pass reads the pre-write
    /// value and overwrites the first pass's result.
    pub fn get_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let table = txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;
            match table.get(key) {
                Ok(Some(value)) => Ok(Some(value.value().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(redb_err("get_in_txn", e)),
            }
        })
    }

    /// Batch insert or update multiple documents in a single redb transaction.
    pub fn batch_put(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        documents: &[(StorageKey, &[u8])],
    ) -> crate::Result<()> {
        if documents.is_empty() {
            return Ok(());
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("batch write txn", e))?;
        {
            let mut table = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;

            for (document_id, value) in documents {
                with_tenant_key(
                    database_id,
                    tenant_id,
                    collection,
                    document_id,
                    |key| -> crate::Result<()> {
                        table
                            .insert(key, *value)
                            .map_err(|e| redb_err("batch insert", e))?;
                        Ok(())
                    },
                )?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("batch commit", e))?;

        debug!(collection, count = documents.len(), "batch document put");
        Ok(())
    }

    /// Point lookup: retrieve a document by collection + document_id (tenant-scoped).
    pub fn get(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
            let table = read_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;

            match table.get(key) {
                Ok(Some(value)) => Ok(Some(value.value().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(redb_err("get", e)),
            }
        })
    }

    /// Approximate byte count for all documents in a single
    /// `(tenant_id, collection)` pair. Sums the raw value sizes via a
    /// redb range scan — O(N) in row count for a single read
    /// transaction. Best-effort: redb key overhead + secondary-index
    /// bytes are not counted. Used by the
    /// `_system.dropped_collections.size_bytes_estimate` column.
    pub fn approx_bytes_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> u64 {
        let prefix = coll_prefix(database_id, tenant_id, collection);
        let end = format!("{prefix}\u{ffff}");
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let table = match read_txn.open_table(DOCUMENTS) {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let mut total: u64 = 0;
        let range = match table.range(prefix.as_str()..end.as_str()) {
            Ok(r) => r,
            Err(_) => return 0,
        };
        for entry in range {
            let Ok((_k, v)) = entry else { continue };
            total = total.saturating_add(v.value().len() as u64);
        }
        total
    }

    /// Delete a document (tenant-scoped).
    ///
    /// Returns the prior bytes when a row was actually removed, or `None`
    /// when nothing matched. The Event Plane needs the prior bytes as the
    /// `old_value` for CDC/trigger delete events; returning them here
    /// avoids a second read pass in the handler.
    pub fn delete(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let write_txn = self
                .db
                .begin_write()
                .map_err(|e| redb_err("write txn", e))?;
            let prior = {
                let mut table = write_txn
                    .open_table(DOCUMENTS)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .remove(key)
                    .map_err(|e| redb_err("remove", e))?
                    .map(|g| g.value().to_vec())
            };
            write_txn.commit().map_err(|e| redb_err("commit", e))?;

            debug!(
                collection,
                %document_id,
                removed = prior.is_some(),
                "document delete"
            );
            Ok(prior)
        })
    }

    /// Delete a document within an externally-owned write transaction.
    ///
    /// Same prior-bytes semantics as [`SparseEngine::delete`]: the removed
    /// value when a row actually went away, `None` when nothing matched. Does
    /// NOT commit — the caller owns the transaction, so the row removal lands
    /// (or is rolled back) together with every other write it carries.
    pub fn delete_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(database_id, tenant_id, collection, document_id, |key| {
            let mut table = txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;
            let prior = table
                .remove(key)
                .map_err(|e| redb_err("remove", e))?
                .map(|g| g.value().to_vec());
            Ok(prior)
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
        (engine, dir)
    }

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    #[test]
    fn put_and_get() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice").unwrap();
        engine.put(0, 1, "users", &key(2), b"bob").unwrap();
        assert_eq!(
            engine.get(0, 1, "users", &key(1)).unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get(0, 1, "users", &key(2)).unwrap(),
            Some(b"bob".to_vec())
        );
        assert_eq!(engine.get(0, 1, "users", &key(3)).unwrap(), None);
    }

    #[test]
    fn databases_are_isolated() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice").unwrap();
        engine.put(7, 1, "users", &key(1), b"alice-db7").unwrap();
        assert_eq!(
            engine.get(0, 1, "users", &key(1)).unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get(7, 1, "users", &key(1)).unwrap(),
            Some(b"alice-db7".to_vec())
        );
    }

    #[test]
    fn put_overwrites() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice").unwrap();
        engine.put(0, 1, "users", &key(1), b"ALICE").unwrap();
        assert_eq!(
            engine.get(0, 1, "users", &key(1)).unwrap(),
            Some(b"ALICE".to_vec())
        );
    }

    #[test]
    fn delete_removes() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice").unwrap();
        assert_eq!(
            engine.delete(0, 1, "users", &key(1)).unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(engine.get(0, 1, "users", &key(1)).unwrap(), None);
        assert_eq!(engine.delete(0, 1, "users", &key(1)).unwrap(), None);
    }

    #[test]
    fn collections_are_isolated() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice").unwrap();
        engine.put(0, 1, "orders", &key(1), b"order-1").unwrap();
        assert_eq!(
            engine.get(0, 1, "users", &key(1)).unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get(0, 1, "orders", &key(1)).unwrap(),
            Some(b"order-1".to_vec())
        );
    }
}
