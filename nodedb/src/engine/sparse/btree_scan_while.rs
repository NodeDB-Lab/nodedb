// SPDX-License-Identifier: BUSL-1.1

//! A streaming document scan its visitor can stop.

use std::ops::ControlFlow;

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable};

use super::btree::{
    DOCUMENTS, KeyedTable, SparseEngine, coll_prefix, invalid_storage_key_err, redb_err,
};

impl SparseEngine {
    /// Visit the documents of a collection in key order, one row in memory
    /// at a time, until `f` returns `ControlFlow::Break`. Every redb error
    /// and every `f` error is propagated.
    pub fn scan_documents_while<F>(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        mut f: F,
    ) -> crate::Result<()>
    where
        F: FnMut(&StorageKey, &[u8]) -> crate::Result<ControlFlow<()>>,
    {
        let prefix = coll_prefix(database_id, tenant_id, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(DOCUMENTS)
            .map_err(|e| redb_err("open table", e))?;
        let range = table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| redb_err("doc range", e))?;

        for entry in range {
            let entry = entry.map_err(|e| redb_err("doc entry", e))?;
            let key = entry.0.value();
            let doc_id = key.strip_prefix(&prefix).unwrap_or(key);
            let storage_key = StorageKey::parse(doc_id).ok_or_else(|| {
                invalid_storage_key_err(KeyedTable::Documents, collection, doc_id)
            })?;
            if f(&storage_key, entry.1.value())?.is_break() {
                break;
            }
        }
        Ok(())
    }
}
