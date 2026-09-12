// SPDX-License-Identifier: BUSL-1.1

//! Collection re-keying: moving every non-versioned row of one
//! `(database_id, tenant_id, collection)` to another in a single transaction.
//!
//! Covers in-place renames and cross-database moves of the document table, the
//! secondary-index table, and the collection's persisted hash-chain head.

use redb::{ReadableDatabase, ReadableTable};
use tracing::debug;

use super::engine::SparseEngine;
use super::keys::coll_prefix;
use super::tables::{DOCUMENTS, INDEXES, redb_err};

impl SparseEngine {
    /// Re-key all documents and secondary indexes for
    /// `(old_database_id, tenant_id, old_collection)` to
    /// `(new_database_id, tenant_id, new_collection)` in a single write
    /// transaction. Supports both in-place renames and cross-database moves.
    ///
    /// Reads every row with the old prefix from both the DOCUMENTS and INDEXES
    /// tables, writes them under the new prefix, and deletes the old rows —
    /// all inside one transaction so the rename is atomic. The collection's
    /// persisted hash-chain head moves in the same transaction: leaving it
    /// behind would restart the renamed collection's chain at genesis while
    /// its already-chained rows travelled to the new name.
    ///
    /// Returns the count of document rows that were moved.
    pub fn rename_collection(
        &self,
        old_database_id: u64,
        new_database_id: u64,
        tenant_id: u64,
        old_collection: &str,
        new_collection: &str,
    ) -> crate::Result<usize> {
        let old_prefix = coll_prefix(old_database_id, tenant_id, old_collection);
        let old_end = format!("{old_prefix}\u{ffff}");
        let new_prefix = coll_prefix(new_database_id, tenant_id, new_collection);
        let new_prefix_len = new_prefix.len();

        // Collect document rows.
        let doc_rows: Vec<(String, Vec<u8>)> = {
            let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
            let table = read_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open docs", e))?;
            let mut out = Vec::new();
            for entry in table
                .range(old_prefix.as_str()..old_end.as_str())
                .map_err(|e| redb_err("range docs", e))?
            {
                let (k, v) = entry.map_err(|e| redb_err("scan doc row", e))?;
                if let Some(suffix) = k.value().strip_prefix(&old_prefix) {
                    out.push((format!("{new_prefix}{suffix}"), v.value().to_vec()));
                }
            }
            out
        };

        // Collect index rows.
        let idx_rows: Vec<String> = {
            let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
            let table = read_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes", e))?;
            let mut out = Vec::new();
            for entry in table
                .range(old_prefix.as_str()..old_end.as_str())
                .map_err(|e| redb_err("range indexes", e))?
            {
                let (k, _) = entry.map_err(|e| redb_err("scan idx row", e))?;
                if let Some(suffix) = k.value().strip_prefix(&old_prefix) {
                    out.push(suffix.to_string());
                }
            }
            out
        };

        let chain_head = self.get_chain_head(old_database_id, tenant_id, old_collection)?;

        if doc_rows.is_empty() && idx_rows.is_empty() && chain_head.is_none() {
            return Ok(0);
        }
        let doc_count = doc_rows.len();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut docs = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open docs write", e))?;
            for (new_key, value) in &doc_rows {
                docs.insert(new_key.as_str(), value.as_slice())
                    .map_err(|e| redb_err("insert doc renamed", e))?;
                let old_key = format!("{old_prefix}{}", &new_key[new_prefix_len..]);
                docs.remove(old_key.as_str())
                    .map_err(|e| redb_err("remove old doc", e))?;
            }

            let mut idxs = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes write", e))?;
            for suffix in &idx_rows {
                let new_key = format!("{new_prefix}{suffix}");
                let old_key = format!("{old_prefix}{suffix}");
                idxs.insert(new_key.as_str(), &[] as &[u8])
                    .map_err(|e| redb_err("insert idx renamed", e))?;
                idxs.remove(old_key.as_str())
                    .map_err(|e| redb_err("remove old idx", e))?;
            }
        }
        if let Some(head) = &chain_head {
            // Remove-then-write, never the reverse: a rename whose source and
            // target keys are identical must end with the head still present.
            self.delete_chain_head_in_txn(&write_txn, old_database_id, tenant_id, old_collection)?;
            self.put_chain_head_in_txn(
                &write_txn,
                new_database_id,
                tenant_id,
                new_collection,
                head,
            )?;
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit rename", e))?;

        debug!(
            tenant_id,
            old_collection, new_collection, doc_count, "sparse: rename_collection complete"
        );
        Ok(doc_count)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{StorageKey, Surrogate};

    use super::*;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
        (engine, dir)
    }

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    /// Renaming a collection moves its chain head with its rows. Leaving the head
    /// under the old name would restart the renamed collection at genesis while
    /// its already-chained rows travelled to the new name.
    #[test]
    fn rename_moves_the_chain_head() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "ledger", &key(42), b"row").unwrap();
        engine.put_chain_head(0, 1, "ledger", "h1").unwrap();

        engine
            .rename_collection(0, 0, 1, "ledger", "journal")
            .unwrap();

        assert_eq!(engine.get_chain_head(0, 1, "ledger").unwrap(), None);
        assert_eq!(
            engine.get_chain_head(0, 1, "journal").unwrap(),
            Some("h1".to_string())
        );
    }
}
