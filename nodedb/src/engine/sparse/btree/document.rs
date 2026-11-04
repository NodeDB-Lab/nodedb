// SPDX-License-Identifier: BUSL-1.1

//! Current-state document reads and writes over the versioned document
//! table.
//!
//! The versioned table is the only document store. A write appends a
//! version at the system-time ordinal the caller stamps, with an unbounded
//! valid interval. A delete appends a tombstone. A read sees the newest
//! version. Superseded versions stay until purge.

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable, WriteTransaction};
use tracing::debug;

use super::engine::SparseEngine;
use super::tables::redb_err;
use crate::engine::sparse::btree_versioned::doc::DOCUMENTS_VERSIONED;
use crate::engine::sparse::btree_versioned::history_bytes::{
    apply_versioned_delta, insert_counted,
};
use crate::engine::sparse::btree_versioned::key::{
    coll_prefix, coll_prefix_end, versioned_doc_key,
};
use crate::engine::sparse::btree_versioned::value::{
    TAG_LIVE, TAG_TOMBSTONE, decode_value, encode_value,
};
use crate::engine::sparse::btree_versioned::visible::{RowRef, visible_body, visible_version};
use crate::engine::storage_bytes::StorageDelta;

impl SparseEngine {
    /// Append a live version of `document_id` at `sys_from`, in its own
    /// write transaction.
    ///
    /// Returns the prior current body, or `None` when the row had none.
    /// Callers thread the prior value into Event Plane emission so the
    /// `WriteOp` tag (Insert vs Update) reflects the actual mutation.
    pub fn put(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
        value: &[u8],
        sys_from: i64,
    ) -> crate::Result<Option<Vec<u8>>> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        let mut delta = StorageDelta::default();
        let prior = {
            let mut table = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open table", e))?;
            let row = RowRef {
                database_id,
                tenant: tenant_id,
                coll: collection,
                doc_id: document_id,
            };
            let prior = visible_body(&table, row, None)?.map(|(_, body)| body);
            let key = versioned_doc_key(database_id, tenant_id, collection, document_id, sys_from);
            let val = encode_value(TAG_LIVE, i64::MIN, i64::MAX, value);
            insert_counted(&mut table, &key, &val, &mut delta)?;
            prior
        };
        apply_versioned_delta(&txn, delta)?;
        txn.commit().map_err(|e| redb_err("commit", e))?;
        debug!(collection, %document_id, sys_from, len = value.len(), "document put");
        Ok(prior)
    }

    /// Whether `document_id` has a live current version, read through the
    /// caller's write transaction so the probe is linearizable with the
    /// version the caller appends next. INSERT-with-unique-PK uses it.
    pub fn exists_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> crate::Result<bool> {
        self.versioned_exists_current_in_txn(txn, database_id, tenant_id, collection, document_id)
    }

    /// The current body of `document_id`, read through the caller's write
    /// transaction so it sees that transaction's uncommitted versions.
    ///
    /// A read-modify-write that runs more than once against the same row
    /// inside one transaction (a derived running total accumulating two
    /// deltas, for instance) reads this way. A committed read would hand the
    /// second pass the pre-write value.
    pub fn get_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        self.versioned_get_current_in_txn(txn, database_id, tenant_id, collection, document_id)
    }

    /// The current body of `document_id`: one reverse seek.
    pub fn get(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        self.versioned_get_current(database_id, tenant_id, collection, document_id)
    }

    /// Bytes every stored version of `(tenant_id, collection)` holds,
    /// superseded versions and tombstones included: the history the
    /// collection keeps on disk. O(N) in stored versions. redb key overhead
    /// and secondary-index bytes are not counted. Feeds the
    /// `_system.dropped_collections.size_bytes_estimate` column.
    ///
    /// `Err` when the table does not read.
    pub fn approx_bytes_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<u64> {
        let lo = coll_prefix(database_id, tenant_id, collection);
        let hi = coll_prefix_end(database_id, tenant_id, collection);
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let mut total: u64 = 0;
        for entry in table
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?
        {
            let (key, value) = entry.map_err(|e| redb_err("entry", e))?;
            total = total
                .saturating_add(key.value().len() as u64)
                .saturating_add(value.value().len() as u64);
        }
        Ok(total)
    }

    /// Append a tombstone for `document_id` at `sys_from`, in its own write
    /// transaction, when the row has a live current version.
    ///
    /// Returns the prior current body when the row was live, or `None` when
    /// nothing matched (no tombstone is written then). The Event Plane takes
    /// the prior bytes as the delete event's `old_value`.
    pub fn delete(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
        sys_from: i64,
    ) -> crate::Result<Option<Vec<u8>>> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        let prior = self.delete_in_txn(
            &txn,
            database_id,
            tenant_id,
            collection,
            document_id,
            sys_from,
        )?;
        txn.commit().map_err(|e| redb_err("commit", e))?;
        debug!(collection, %document_id, sys_from, removed = prior.is_some(), "document delete");
        Ok(prior)
    }

    /// [`Self::delete`] inside a caller-owned write transaction. Does NOT
    /// commit: the tombstone lands, or rolls back, with every other write
    /// the transaction carries.
    pub fn delete_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
        sys_from: i64,
    ) -> crate::Result<Option<Vec<u8>>> {
        let mut table = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let row = RowRef {
            database_id,
            tenant: tenant_id,
            coll: collection,
            doc_id: document_id,
        };
        let prior = match visible_version(&table, row, None)? {
            Some((_, guard)) => {
                let decoded = decode_value(guard.value())?;
                decoded.is_live().then(|| decoded.body.to_vec())
            }
            None => None,
        };
        if prior.is_some() {
            let key = versioned_doc_key(database_id, tenant_id, collection, document_id, sys_from);
            let val = encode_value(TAG_TOMBSTONE, 0, 0, &[]);
            let mut delta = StorageDelta::default();
            insert_counted(&mut table, &key, &val, &mut delta)?;
            apply_versioned_delta(txn, delta)?;
        }
        Ok(prior)
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
        engine.put(0, 1, "users", &key(1), b"alice", 10).unwrap();
        engine.put(0, 1, "users", &key(2), b"bob", 11).unwrap();
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
        engine.put(0, 1, "users", &key(1), b"alice", 10).unwrap();
        engine
            .put(7, 1, "users", &key(1), b"alice-db7", 11)
            .unwrap();
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
    fn put_overwrites_current_and_keeps_history() {
        let (engine, _dir) = open_temp();
        assert_eq!(
            engine.put(0, 1, "users", &key(1), b"alice", 10).unwrap(),
            None
        );
        assert_eq!(
            engine.put(0, 1, "users", &key(1), b"ALICE", 20).unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get(0, 1, "users", &key(1)).unwrap(),
            Some(b"ALICE".to_vec())
        );
        assert_eq!(
            engine
                .versioned_get_as_of(0, 1, "users", &key(1), Some(15), None)
                .unwrap(),
            Some(b"alice".to_vec()),
            "the superseded version stays visible at a cut below its successor"
        );
    }

    #[test]
    fn delete_tombstones_and_reinsert_reads_back() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice", 10).unwrap();
        assert_eq!(
            engine.delete(0, 1, "users", &key(1), 20).unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(engine.get(0, 1, "users", &key(1)).unwrap(), None);
        assert_eq!(
            engine.delete(0, 1, "users", &key(1), 30).unwrap(),
            None,
            "a delete of a deleted row writes nothing"
        );
        assert_eq!(
            engine
                .versioned_latest_sys_from(0, 1, "users", &key(1))
                .unwrap(),
            Some(20)
        );
        engine.put(0, 1, "users", &key(1), b"again", 40).unwrap();
        let at = |cut: i64| {
            engine
                .versioned_get_as_of(0, 1, "users", &key(1), Some(cut), None)
                .unwrap()
        };
        assert_eq!(at(5), None);
        assert_eq!(at(10), Some(b"alice".to_vec()));
        assert_eq!(at(25), None);
        assert_eq!(at(40), Some(b"again".to_vec()));
        assert_eq!(
            engine.get(0, 1, "users", &key(1)).unwrap(),
            Some(b"again".to_vec())
        );
    }

    #[test]
    fn collections_are_isolated() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice", 10).unwrap();
        engine.put(0, 1, "orders", &key(1), b"order-1", 11).unwrap();
        assert_eq!(
            engine.get(0, 1, "users", &key(1)).unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get(0, 1, "orders", &key(1)).unwrap(),
            Some(b"order-1".to_vec())
        );
    }

    #[test]
    fn approx_bytes_counts_history() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice", 10).unwrap();
        let one = engine.approx_bytes_for_collection(0, 1, "users").unwrap();
        engine.put(0, 1, "users", &key(1), b"alice", 20).unwrap();
        let two = engine.approx_bytes_for_collection(0, 1, "users").unwrap();
        assert_eq!(two, one * 2, "a superseded version keeps its bytes");
    }

    #[test]
    fn history_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse.redb");
        {
            let engine = SparseEngine::open(&path).unwrap();
            engine.put(0, 1, "users", &key(1), b"v1", 10).unwrap();
            engine.put(0, 1, "users", &key(1), b"v2", 20).unwrap();
            engine.delete(0, 1, "users", &key(1), 30).unwrap();
        }
        let engine = SparseEngine::open(&path).unwrap();
        let at = |cut: i64| {
            engine
                .versioned_get_as_of(0, 1, "users", &key(1), Some(cut), None)
                .unwrap()
        };
        assert_eq!(at(15), Some(b"v1".to_vec()));
        assert_eq!(at(25), Some(b"v2".to_vec()));
        assert_eq!(at(35), None);
        assert_eq!(engine.get(0, 1, "users", &key(1)).unwrap(), None);
    }
}
