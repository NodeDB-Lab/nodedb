// SPDX-License-Identifier: BUSL-1.1

//! Secondary index operations for the sparse engine.
//!
//! Index key format: `"{database_id}:{tenant_id}:{collection}:{field}:{value}:{document_id}"`.
//! Extracted from `btree.rs` — document CRUD stays there, index ops live here.

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable, WriteTransaction};
use tracing::debug;

use super::btree::{
    DOCUMENTS, INDEXES, KeyedTable, SparseEngine, coll_prefix, invalid_storage_key_err, redb_err,
};

/// Identifies a single secondary-index entry for an in-txn mutation.
///
/// Grouping the tenant-scoped key components into one struct keeps
/// [`SparseEngine::index_remove_in_txn`] to two arguments rather than the
/// seven positional args its `index_put_in_txn` sibling carries.
pub struct IndexEntryTxn<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub value: &'a str,
    pub document_id: &'a StorageKey,
}

/// Parameters for [`SparseEngine::range_scan`].
#[derive(Debug, Clone, Copy)]
pub struct RangeScanParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub lower: Option<&'a [u8]>,
    pub upper: Option<&'a [u8]>,
    pub limit: usize,
}

impl SparseEngine {
    /// Delete all secondary index entries for a document.
    ///
    /// Scans the INDEXES table for entries ending with `:{document_id}` and
    /// removes them. Called during document deletion cascade.
    pub fn delete_indexes_for_document(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        self.delete_indexes_for_document_in_txn(
            &write_txn,
            database_id,
            tenant_id,
            collection,
            document_id,
        )?;
        write_txn
            .commit()
            .map_err(|e| redb_err("commit index cascade", e))?;

        Ok(())
    }

    /// Delete all secondary index entries for a document within an
    /// externally-owned write transaction.
    ///
    /// The transactional form of [`SparseEngine::delete_indexes_for_document`],
    /// used by the document-delete cascade so the index removals land in the
    /// same redb transaction as the row removal that caused them. Does NOT
    /// commit.
    pub fn delete_indexes_for_document_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> crate::Result<()> {
        let prefix = coll_prefix(database_id, tenant_id, collection);
        let end = format!("{prefix}\u{ffff}");
        let suffix = format!(":{document_id}");

        let mut table = txn
            .open_table(INDEXES)
            .map_err(|e| redb_err("open indexes", e))?;

        let keys_to_remove: Vec<String> = table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| redb_err("index range", e))?
            .filter_map(|r| {
                r.ok().and_then(|(k, _)| {
                    let key = k.value().to_string();
                    if key.ends_with(&suffix) {
                        Some(key)
                    } else {
                        None
                    }
                })
            })
            .collect();

        for key in &keys_to_remove {
            table
                .remove(key.as_str())
                .map_err(|e| redb_err("remove index", e))?;
        }

        Ok(())
    }

    /// Delete all secondary index entries for a specific field in a collection.
    pub fn delete_index_entries_for_field(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
    ) -> crate::Result<usize> {
        let prefix = format!(
            "{}{field}:",
            coll_prefix(database_id, tenant_id, collection)
        );
        let end = format!("{prefix}\u{ffff}");

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;

        let removed;
        {
            let mut table = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes", e))?;

            let keys_to_remove: Vec<String> = table
                .range(prefix.as_str()..end.as_str())
                .map_err(|e| redb_err("index range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();

            removed = keys_to_remove.len();
            for key in &keys_to_remove {
                table
                    .remove(key.as_str())
                    .map_err(|e| redb_err("remove index entry", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit index delete", e))?;

        if removed > 0 {
            debug!(
                collection,
                field, removed, "index entries deleted for field"
            );
        }

        Ok(removed)
    }

    /// Range scan on secondary index entries.
    pub fn range_scan(&self, params: RangeScanParams<'_>) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let RangeScanParams {
            database_id,
            tenant_id,
            collection,
            field,
            lower,
            upper,
            limit,
        } = params;
        let prefix = format!(
            "{}{field}:",
            coll_prefix(database_id, tenant_id, collection)
        );

        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEXES)
            .map_err(|e| redb_err("open table", e))?;

        let start = match lower {
            Some(l) => format!("{prefix}{}", String::from_utf8_lossy(l)),
            None => prefix.clone(),
        };
        let end = match upper {
            Some(u) => format!("{prefix}{}", String::from_utf8_lossy(u)),
            None => {
                let mut end = prefix.clone();
                end.push('\u{ffff}');
                end
            }
        };

        let mut results = Vec::with_capacity(limit.min(256));
        let range = table
            .range(start.as_str()..end.as_str())
            .map_err(|e| redb_err("range", e))?;

        for entry in range {
            if results.len() >= limit {
                break;
            }
            let entry = entry.map_err(|e| redb_err("range entry", e))?;
            let key = entry.0.value().to_string();
            let value = entry.1.value().to_vec();
            results.push((key, value));
        }

        debug!(collection, field, count = results.len(), "range scan");
        Ok(results)
    }

    /// Insert a secondary index entry (tenant-scoped).
    ///
    /// Opens its own write transaction. Callers already inside a write
    /// transaction — the PointPut / BatchInsert apply path — MUST use
    /// [`SparseEngine::index_put_in_txn`] instead; redb allows only one
    /// active writer so a nested `begin_write` here deadlocks.
    pub fn index_put(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        value: &str,
        document_id: &StorageKey,
    ) -> crate::Result<()> {
        super::btree::with_tenant_key4(
            database_id,
            tenant_id,
            collection,
            field,
            value,
            document_id,
            |key| {
                let write_txn = self
                    .db
                    .begin_write()
                    .map_err(|e| redb_err("write txn", e))?;
                {
                    let mut table = write_txn
                        .open_table(INDEXES)
                        .map_err(|e| redb_err("open table", e))?;
                    table
                        .insert(key, [].as_slice())
                        .map_err(|e| redb_err("index insert", e))?;
                }
                write_txn.commit().map_err(|e| redb_err("commit", e))?;
                Ok(())
            },
        )
    }

    /// Insert a secondary index entry into an already-open write txn.
    ///
    /// Use from the PointPut / BatchInsert apply path so document + index
    /// writes commit atomically in the same redb transaction.
    pub fn index_put_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        entry: IndexEntryTxn<'_>,
    ) -> crate::Result<()> {
        super::btree::with_tenant_key4(
            entry.database_id,
            entry.tenant_id,
            entry.collection,
            entry.field,
            entry.value,
            entry.document_id,
            |key| {
                let mut table = txn
                    .open_table(INDEXES)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .insert(key, [].as_slice())
                    .map_err(|e| redb_err("index insert", e))?;
                Ok(())
            },
        )
    }

    /// Insert many secondary-index entries in one pass.
    ///
    /// `keys` are the fully-built index keys; they are sorted and deduplicated
    /// here, then written in that order into a single opened table. This is
    /// what a backfill wants: the scan yields documents in id order while the
    /// index is ordered by value, so writing as it scans re-enters the tree at
    /// an unrelated position for every key. Writing in key order keeps each
    /// insert near the last, and the table is opened once rather than once per
    /// entry as [`Self::index_put_in_txn`] does.
    ///
    /// Deduplication is not optional: an array field can yield the same value
    /// twice for one document (`tags: ["a", "a"]`), producing the same key
    /// twice.
    ///
    /// A cursor would remove the per-insert descent entirely, but cannot be
    /// used here: its `insert_after` requires each key to sort strictly between
    /// the entries adjacent to it, so a key already present — which `insert`
    /// simply overwrites, and which a re-run of a backfill produces — is an
    /// error rather than an update.
    pub fn index_put_sorted_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        keys: &mut Vec<String>,
    ) -> crate::Result<()> {
        keys.sort_unstable();
        keys.dedup();
        if keys.is_empty() {
            return Ok(());
        }
        let mut table = txn
            .open_table(INDEXES)
            .map_err(|e| redb_err("open table", e))?;
        for key in keys.iter() {
            table
                .insert(key.as_str(), [].as_slice())
                .map_err(|e| redb_err("index insert", e))?;
        }
        Ok(())
    }

    /// Remove a secondary index entry from an already-open write txn.
    ///
    /// Mirror of [`Self::index_put_in_txn`] using the same tenant-scoped key
    /// format (`with_tenant_key4`), but `remove`s the entry instead of
    /// inserting it. Used by the UPDATE stale-entry diff so a changed field
    /// value drops its old index row inside the caller's write transaction.
    pub fn index_remove_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        entry: IndexEntryTxn<'_>,
    ) -> crate::Result<()> {
        super::btree::with_tenant_key4(
            entry.database_id,
            entry.tenant_id,
            entry.collection,
            entry.field,
            entry.value,
            entry.document_id,
            |key| {
                let mut table = txn
                    .open_table(INDEXES)
                    .map_err(|e| redb_err("open table", e))?;
                table.remove(key).map_err(|e| redb_err("index remove", e))?;
                Ok(())
            },
        )
    }

    /// Remove a secondary index entry (tenant-scoped), opening its own write
    /// transaction.
    ///
    /// Mirror of [`Self::index_put`]. Used by the transaction rollback undo
    /// path, which runs outside any caller-owned write txn, to reverse a
    /// forward index write.
    pub fn index_remove(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        value: &str,
        document_id: &StorageKey,
    ) -> crate::Result<()> {
        super::btree::with_tenant_key4(
            database_id,
            tenant_id,
            collection,
            field,
            value,
            document_id,
            |key| {
                let write_txn = self
                    .db
                    .begin_write()
                    .map_err(|e| redb_err("write txn", e))?;
                {
                    let mut table = write_txn
                        .open_table(INDEXES)
                        .map_err(|e| redb_err("open table", e))?;
                    table.remove(key).map_err(|e| redb_err("index remove", e))?;
                }
                write_txn.commit().map_err(|e| redb_err("commit", e))?;
                Ok(())
            },
        )
    }

    /// Index-only scan: return `(doc_id, field_value)` pairs without touching DOCUMENTS.
    pub fn scan_index_values(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        limit: usize,
    ) -> crate::Result<Vec<(StorageKey, String)>> {
        let prefix = format!(
            "{}{field}:",
            coll_prefix(database_id, tenant_id, collection)
        );
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEXES)
            .map_err(|e| redb_err("open table", e))?;

        let range = table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| redb_err("index range", e))?;

        let mut results = Vec::with_capacity(limit.min(256));
        for entry in range {
            if results.len() >= limit {
                break;
            }
            let entry = entry.map_err(|e| redb_err("index entry", e))?;
            let key = entry.0.value().to_string();
            if let Some(rest) = key.strip_prefix(&prefix)
                && let Some(colon_pos) = rest.rfind(':')
            {
                let value = &rest[..colon_pos];
                let doc_id = &rest[colon_pos + 1..];
                let doc_id = StorageKey::parse(doc_id).ok_or_else(|| {
                    invalid_storage_key_err(KeyedTable::Indexes, collection, doc_id)
                })?;
                results.push((doc_id, value.to_string()));
            }
        }

        debug!(collection, field, count = results.len(), "index-only scan");
        Ok(results)
    }

    /// Insert a document by raw pre-formed key (snapshot restore).
    pub fn put_raw(&self, key: &str, value: &[u8]) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("raw write txn", e))?;
        {
            let mut table = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;
            table
                .insert(key, value)
                .map_err(|e| redb_err("raw insert", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Point lookup by raw pre-formed key (snapshot restore verification).
    pub fn get_raw(&self, key: &str) -> crate::Result<Option<Vec<u8>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("raw read txn", e))?;
        let table = read_txn
            .open_table(DOCUMENTS)
            .map_err(|e| redb_err("open table", e))?;
        match table.get(key) {
            Ok(Some(v)) => Ok(Some(v.value().to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(redb_err("raw get", e)),
        }
    }
}

/// Build the index key for `entry` as an owned `String`.
///
/// The same composite the per-entry write path builds, materialized rather than
/// borrowed, so a caller can collect many keys and write them in sorted order.
/// Ordering on this string IS the table's ordering — the key is
/// `database:tenant:collection:field:value:document` — so sorting the strings
/// sorts the rows.
pub fn index_key_for(entry: IndexEntryTxn<'_>) -> String {
    super::btree::with_tenant_key4(
        entry.database_id,
        entry.tenant_id,
        entry.collection,
        entry.field,
        entry.value,
        entry.document_id,
        |key| key.to_string(),
    )
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
    fn range_scan_with_index() {
        let (engine, _dir) = open_temp();
        engine
            .index_put(0, 1, "users", "age", "025", &key(1))
            .unwrap();
        engine
            .index_put(0, 1, "users", "age", "030", &key(2))
            .unwrap();
        engine
            .index_put(0, 1, "users", "age", "035", &key(3))
            .unwrap();
        engine
            .index_put(0, 1, "users", "age", "040", &key(4))
            .unwrap();
        let results = engine
            .range_scan(RangeScanParams {
                database_id: 0,
                tenant_id: 1,
                collection: "users",
                field: "age",
                lower: Some(b"025"),
                upper: Some(b"036"),
                limit: 10,
            })
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn delete_index_entries_for_field() {
        let (engine, _dir) = open_temp();
        engine
            .index_put(0, 1, "users", "email", "alice@example.com", &key(1))
            .unwrap();
        engine
            .index_put(0, 1, "users", "email", "bob@example.com", &key(2))
            .unwrap();
        engine
            .index_put(0, 1, "users", "age", "30", &key(1))
            .unwrap();
        engine
            .index_put(0, 1, "users", "age", "25", &key(2))
            .unwrap();
        let removed = engine
            .delete_index_entries_for_field(0, 1, "users", "email")
            .unwrap();
        assert_eq!(removed, 2);
        let age_entries = engine.scan_index_groups(0, 1, "users", "age").unwrap();
        assert_eq!(age_entries.len(), 2);
        let email_entries = engine.scan_index_groups(0, 1, "users", "email").unwrap();
        assert!(email_entries.is_empty());
    }
}
