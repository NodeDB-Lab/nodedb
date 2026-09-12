// SPDX-License-Identifier: BUSL-1.1

//! Table scanning and import/export methods for `SparseEngine`.

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable};
use tracing::debug;

use super::btree::{
    DOCUMENTS, INDEXES, KeyedTable, SparseEngine, coll_prefix, invalid_storage_key_err, redb_err,
    tenant_prefix,
};

impl SparseEngine {
    /// Scan documents in a collection (reads DOCUMENTS table, not INDEXES).
    ///
    /// Returns `(document_id, document_bytes)` pairs for all documents in the
    /// collection, up to `limit`. Use for full table scans and post-scan filtering.
    pub fn scan_documents(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        let prefix = coll_prefix(database_id, tenant_id, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(DOCUMENTS)
            .map_err(|e| redb_err("open table", e))?;

        let range = table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| redb_err("doc range", e))?;

        let mut results = Vec::with_capacity(limit.min(256));
        for entry in range {
            if results.len() >= limit {
                break;
            }
            let entry = entry.map_err(|e| redb_err("doc entry", e))?;
            let key = entry.0.value();
            // Extract document_id from key format "{database_id}:{tenant}:{collection}:{doc_id}"
            let doc_id = key.strip_prefix(&prefix).unwrap_or(key);
            let storage_key = StorageKey::parse(doc_id).ok_or_else(|| {
                invalid_storage_key_err(KeyedTable::Documents, collection, doc_id)
            })?;
            let value = entry.1.value().to_vec();
            results.push((storage_key, value));
        }

        debug!(collection, count = results.len(), "document scan");
        Ok(results)
    }

    /// Streaming variant of `scan_documents`: yields each `(document_id,
    /// document_bytes)` row to `f` as it is read, holding the redb read
    /// transaction open across callback invocations.
    ///
    /// Unlike `scan_documents`, no result `Vec` is materialized — peak memory
    /// is a single row, not the whole scan. The visited rows (and their order)
    /// are byte-identical to `scan_documents` for the same arguments: same
    /// table, same key range, same prefix stripping, same value extraction,
    /// same `limit` cutoff.
    ///
    /// If `f` returns `Err`, iteration stops immediately and the error is
    /// propagated. Every redb iteration error is propagated via `?` — rows are
    /// never silently dropped.
    // consumed by `scan_collection_for_each` streaming routing
    pub fn scan_documents_for_each<F>(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        limit: usize,
        mut f: F,
    ) -> crate::Result<()>
    where
        F: FnMut(&StorageKey, &[u8]) -> crate::Result<()>,
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

        let mut count = 0usize;
        for entry in range {
            if count >= limit {
                break;
            }
            let entry = entry.map_err(|e| redb_err("doc entry", e))?;
            let key = entry.0.value();
            // Extract document_id from key format "{database_id}:{tenant}:{collection}:{doc_id}"
            let doc_id = key.strip_prefix(&prefix).unwrap_or(key);
            let storage_key = StorageKey::parse(doc_id).ok_or_else(|| {
                invalid_storage_key_err(KeyedTable::Documents, collection, doc_id)
            })?;
            let value = entry.1.value();
            f(&storage_key, value)?;
            count += 1;
        }

        debug!(collection, count, "streaming document scan");
        Ok(())
    }

    /// Scan documents in chunks, calling `handler` for each chunk.
    ///
    /// Processes up to `total_limit` documents in chunks of `chunk_size`.
    /// The handler receives each chunk and can accumulate results without
    /// holding all documents in memory simultaneously.
    ///
    /// Returns the total number of documents processed.
    pub fn scan_documents_chunked<F>(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        total_limit: usize,
        chunk_size: usize,
        mut handler: F,
    ) -> crate::Result<usize>
    where
        F: FnMut(&[(StorageKey, Vec<u8>)]),
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

        let mut chunk = Vec::with_capacity(chunk_size);
        let mut total = 0usize;

        for entry in range {
            if total >= total_limit {
                break;
            }
            let entry = entry.map_err(|e| redb_err("doc entry", e))?;
            let key = entry.0.value();
            let doc_id = key.strip_prefix(&prefix).unwrap_or(key);
            let storage_key = StorageKey::parse(doc_id).ok_or_else(|| {
                invalid_storage_key_err(KeyedTable::Documents, collection, doc_id)
            })?;
            let value = entry.1.value().to_vec();
            chunk.push((storage_key, value));
            total += 1;

            if chunk.len() >= chunk_size {
                handler(&chunk);
                chunk.clear();
            }
        }

        // Process remaining partial chunk.
        if !chunk.is_empty() {
            handler(&chunk);
        }

        debug!(collection, total, chunk_size, "chunked document scan");
        Ok(total)
    }

    /// Scan index entries grouped by value for a field.
    ///
    /// Returns `(value, count)` pairs by scanning the INDEXES table for
    /// `{database_id}:{tenant}:{collection}:{field}:*` entries and counting documents
    /// per value. Used for index-backed `GROUP BY field` + `COUNT(*)`
    /// queries — no document table access needed at all.
    pub fn scan_index_groups(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
    ) -> crate::Result<Vec<(String, usize)>> {
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

        // Index keys have format: {tenant}:{collection}:{field}:{value}:{doc_id}
        // Group by {value} (4th component) and count occurrences.
        let mut groups: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for entry in range {
            let entry = entry.map_err(|e| redb_err("index entry", e))?;
            let key = entry.0.value().to_string();
            // Extract value from key: skip prefix, split on ':', take first part.
            // rest = "{value}:{doc_id}" — split at last ':' to get value.
            if let Some(rest) = key.strip_prefix(&prefix)
                && let Some(colon_pos) = rest.rfind(':')
            {
                let value = &rest[..colon_pos];
                *groups.entry(value.to_string()).or_default() += 1;
            }
        }

        let mut result: Vec<(String, usize)> = groups.into_iter().collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        debug!(collection, field, groups = result.len(), "index group scan");
        Ok(result)
    }

    /// Scan index entries grouped by value, filtered to a set of document IDs.
    ///
    /// Like `scan_index_groups`, but only counts entries whose document ID
    /// is in the provided `doc_ids` set. This enables shared-filter-bitmap
    /// facet counting: evaluate the WHERE predicate once to get matching doc IDs,
    /// then count per-facet-field using this method.
    pub fn scan_index_groups_filtered(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        doc_ids: &std::collections::HashSet<String>,
    ) -> crate::Result<Vec<(String, usize)>> {
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

        let mut groups: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for entry in range {
            let entry = entry.map_err(|e| redb_err("index entry", e))?;
            let key = entry.0.value().to_string();
            if let Some(rest) = key.strip_prefix(&prefix)
                && let Some(colon_pos) = rest.rfind(':')
            {
                let value = &rest[..colon_pos];
                let doc_id = &rest[colon_pos + 1..];
                if doc_ids.contains(doc_id) {
                    *groups.entry(value.to_string()).or_default() += 1;
                }
            }
        }

        let mut result: Vec<(String, usize)> = groups.into_iter().collect();
        result.sort_by_key(|r| std::cmp::Reverse(r.1)); // Sort by count descending (most popular first).
        debug!(
            collection,
            field,
            groups = result.len(),
            "filtered index group scan"
        );
        Ok(result)
    }

    /// Scan documents with predicate evaluation at the storage layer.
    ///
    /// Unlike `scan_documents` which returns all documents and filters
    /// post-fetch, this method evaluates predicates during the scan.
    /// Non-matching documents are never allocated or returned — only their
    /// raw bytes are decoded for predicate evaluation, then dropped if they
    /// don't match. This avoids O(N) allocation for large collections when
    /// only a small fraction matches the predicate.
    ///
    /// `predicate` receives the row's doc-id key and raw document bytes, and
    /// returns true if the document should be included in results. The key
    /// carries a schemaless row's identity when its body has no `id` field,
    /// so a predicate that checks `id` needs it.
    ///
    /// `stop` is consulted once per scanned row, before the predicate. It ends
    /// the scan where it stands, so the caller that owns the signal — a
    /// statement deadline, today — decides whether the short result is an
    /// answer or an error. Pass
    /// [`never_stop`](crate::engine::sparse::scan_stop::never_stop) when
    /// nothing can cut the scan short.
    pub fn scan_documents_filtered(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        limit: usize,
        predicate: &dyn Fn(&StorageKey, &[u8]) -> bool,
        stop: &dyn Fn() -> bool,
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        let prefix = coll_prefix(database_id, tenant_id, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(DOCUMENTS)
            .map_err(|e| redb_err("open table", e))?;

        let range = table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| redb_err("doc range", e))?;

        let mut results = Vec::with_capacity(limit.min(256));
        for entry in range {
            if results.len() >= limit {
                break;
            }
            if stop() {
                break;
            }
            let entry = entry.map_err(|e| redb_err("doc entry", e))?;
            let value_bytes = entry.1.value();
            let key = entry.0.value();
            let doc_id = key.strip_prefix(&prefix).unwrap_or(key);
            let storage_key = StorageKey::parse(doc_id).ok_or_else(|| {
                invalid_storage_key_err(KeyedTable::Documents, collection, doc_id)
            })?;

            // Evaluate predicate on raw bytes — skip allocation if no match.
            if !predicate(&storage_key, value_bytes) {
                continue;
            }

            results.push((storage_key, value_bytes.to_vec()));
        }

        debug!(collection, count = results.len(), "filtered document scan");
        Ok(results)
    }

    /// Export all documents as key-value pairs (for snapshot transfer).
    pub fn export_documents(&self) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(DOCUMENTS)
            .map_err(|e| redb_err("open docs", e))?;
        let mut pairs = Vec::new();
        let iter = table.range(..).map_err(|e| redb_err("iter docs", e))?;
        for entry in iter {
            let entry = entry.map_err(|e| redb_err("read doc entry", e))?;
            pairs.push((entry.0.value().to_string(), entry.1.value().to_vec()));
        }
        Ok(pairs)
    }

    /// Export all index entries as key-value pairs (for snapshot transfer).
    pub fn export_indexes(&self) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(INDEXES)
            .map_err(|e| redb_err("open indexes", e))?;
        let mut pairs = Vec::new();
        let iter = table.range(..).map_err(|e| redb_err("iter indexes", e))?;
        for entry in iter {
            let entry = entry.map_err(|e| redb_err("read index entry", e))?;
            pairs.push((entry.0.value().to_string(), entry.1.value().to_vec()));
        }
        Ok(pairs)
    }

    /// Import documents from a snapshot (overwrites existing data).
    pub fn import_documents(&self, pairs: &[(String, Vec<u8>)]) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open docs", e))?;
            for (key, value) in pairs {
                table
                    .insert(key.as_str(), value.as_slice())
                    .map_err(|e| redb_err("insert doc", e))?;
            }
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Import index entries from a snapshot.
    pub fn import_indexes(&self, pairs: &[(String, Vec<u8>)]) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes", e))?;
            for (key, value) in pairs {
                table
                    .insert(key.as_str(), value.as_slice())
                    .map_err(|e| redb_err("insert idx", e))?;
            }
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    // ── Tenant-wide scan (for BACKUP/RESTORE) ──────────────────────

    /// Scan ALL documents for a tenant across all collections in a database.
    ///
    /// Returns `(full_key, value_bytes)` pairs. The key includes the
    /// database/tenant prefix: `"{database_id}:{tenant_id}:{collection}:{doc_id}"`.
    pub fn scan_all_for_tenant(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        self.scan_table_for_tenant(DOCUMENTS, database_id, tenant_id, "tenant scan")
    }

    /// Scan ALL index entries for a tenant in a database.
    ///
    /// Returns `(full_key, value_bytes)` pairs from the INDEXES table.
    pub fn scan_indexes_for_tenant(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        self.scan_table_for_tenant(INDEXES, database_id, tenant_id, "index scan")
    }

    /// Shared scan logic for any redb table with database/tenant-prefixed keys.
    fn scan_table_for_tenant(
        &self,
        table_def: redb::TableDefinition<&str, &[u8]>,
        database_id: u64,
        tenant_id: u64,
        label: &str,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let prefix = tenant_prefix(database_id, tenant_id);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(table_def)
            .map_err(|e| redb_err("open table", e))?;

        let range = table
            .range(prefix.as_str()..end.as_str())
            .map_err(|e| redb_err(label, e))?;

        let mut results = Vec::new();
        for entry in range {
            let entry = entry.map_err(|e| redb_err("entry", e))?;
            results.push((entry.0.value().to_string(), entry.1.value().to_vec()));
        }
        Ok(results)
    }

    /// Insert an index entry by raw pre-formed key (snapshot restore).
    pub fn put_index_raw(&self, key: &str, value: &[u8]) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open index table", e))?;
            table
                .insert(key, value)
                .map_err(|e| redb_err("index insert", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
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
    fn for_each_matches_scan_documents() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice").unwrap();
        engine.put(0, 1, "users", &key(2), b"bob").unwrap();
        engine.put(0, 1, "users", &key(3), b"carol").unwrap();

        let materialized = engine.scan_documents(0, 1, "users", usize::MAX).unwrap();

        let mut streamed: Vec<(StorageKey, Vec<u8>)> = Vec::new();
        engine
            .scan_documents_for_each(0, 1, "users", usize::MAX, |doc_id, bytes| {
                streamed.push((*doc_id, bytes.to_vec()));
                Ok(())
            })
            .unwrap();

        // Same order, same ids, same bytes.
        assert_eq!(materialized, streamed);
    }

    #[test]
    fn for_each_respects_limit() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice").unwrap();
        engine.put(0, 1, "users", &key(2), b"bob").unwrap();
        engine.put(0, 1, "users", &key(3), b"carol").unwrap();

        let materialized = engine.scan_documents(0, 1, "users", 2).unwrap();

        let mut streamed: Vec<(StorageKey, Vec<u8>)> = Vec::new();
        engine
            .scan_documents_for_each(0, 1, "users", 2, |doc_id, bytes| {
                streamed.push((*doc_id, bytes.to_vec()));
                Ok(())
            })
            .unwrap();

        assert_eq!(materialized.len(), 2);
        assert_eq!(materialized, streamed);
    }

    /// The stop signal ends the row loop where it stands. A caller that owns
    /// the signal — a statement deadline — turns the short result into an error
    /// rather than reporting it as the answer.
    #[test]
    fn filtered_scan_stops_on_the_stop_signal() {
        let (engine, _dir) = open_temp();
        for i in 0..8 {
            engine.put(0, 1, "users", &key(i), b"body").unwrap();
        }

        // Stop after the third row is visited.
        let visited = std::cell::Cell::new(0usize);
        let rows = engine
            .scan_documents_filtered(
                0,
                1,
                "users",
                usize::MAX,
                &|_: &StorageKey, _: &[u8]| {
                    visited.set(visited.get() + 1);
                    true
                },
                &|| visited.get() >= 3,
            )
            .unwrap();

        assert_eq!(rows.len(), 3, "the scan ends at the stop signal");
        assert_eq!(visited.get(), 3, "no row past the signal is evaluated");
    }

    #[test]
    fn filtered_scan_runs_to_completion_without_a_stop() {
        let (engine, _dir) = open_temp();
        for i in 0..8 {
            engine.put(0, 1, "users", &key(i), b"body").unwrap();
        }

        let rows = engine
            .scan_documents_filtered(
                0,
                1,
                "users",
                usize::MAX,
                &|_: &StorageKey, _: &[u8]| true,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();

        assert_eq!(rows.len(), 8, "never_stop must not cut the scan short");
    }

    #[test]
    fn for_each_propagates_callback_error() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice").unwrap();
        engine.put(0, 1, "users", &key(2), b"bob").unwrap();

        let mut seen = 0usize;
        let result =
            engine.scan_documents_for_each(0, 1, "users", usize::MAX, |_doc_id, _bytes| {
                seen += 1;
                Err(crate::Error::Internal {
                    detail: "stop".to_string(),
                })
            });

        assert!(result.is_err());
        // Stops at the first row — does not visit every row.
        assert_eq!(seen, 1);
    }
}
