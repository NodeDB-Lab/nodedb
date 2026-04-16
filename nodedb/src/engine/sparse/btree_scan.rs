//! Table scanning and import/export methods for `SparseEngine`.

use tracing::debug;

use super::btree::{DOCUMENTS, INDEXES, SparseEngine, redb_err};

impl SparseEngine {
    /// Scan documents in a collection (reads DOCUMENTS table, not INDEXES).
    ///
    /// Returns `(document_id, document_bytes)` pairs for all documents in the
    /// collection, up to `limit`. Use for full table scans and post-scan filtering.
    pub fn scan_documents(
        &self,
        tenant_id: u32,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let prefix = format!("{tenant_id}:{collection}:");
        let end = format!("{tenant_id}:{collection}:\u{ffff}");

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
            let key = entry.0.value().to_string();
            // Extract document_id from key format "{tenant}:{collection}:{doc_id}"
            let doc_id = key.strip_prefix(&prefix).unwrap_or(&key).to_string();
            let value = entry.1.value().to_vec();
            results.push((doc_id, value));
        }

        debug!(collection, count = results.len(), "document scan");
        Ok(results)
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
        tenant_id: u32,
        collection: &str,
        total_limit: usize,
        chunk_size: usize,
        mut handler: F,
    ) -> crate::Result<usize>
    where
        F: FnMut(&[(String, Vec<u8>)]),
    {
        let prefix = format!("{tenant_id}:{collection}:");
        let end = format!("{tenant_id}:{collection}:\u{ffff}");

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
            let key = entry.0.value().to_string();
            let doc_id = key.strip_prefix(&prefix).unwrap_or(&key).to_string();
            let value = entry.1.value().to_vec();
            chunk.push((doc_id, value));
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
    /// `{tenant}:{collection}:{field}:*` entries and counting documents
    /// per value. Used for index-backed `GROUP BY field` + `COUNT(*)`
    /// queries — no document table access needed at all.
    pub fn scan_index_groups(
        &self,
        tenant_id: u32,
        collection: &str,
        field: &str,
    ) -> crate::Result<Vec<(String, usize)>> {
        let prefix = format!("{tenant_id}:{collection}:{field}:");
        let end = format!("{tenant_id}:{collection}:{field}:\u{ffff}");

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
        tenant_id: u32,
        collection: &str,
        field: &str,
        doc_ids: &std::collections::HashSet<String>,
    ) -> crate::Result<Vec<(String, usize)>> {
        let prefix = format!("{tenant_id}:{collection}:{field}:");
        let end = format!("{tenant_id}:{collection}:{field}:\u{ffff}");

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
    /// `predicate` receives the raw document bytes and returns true if the
    /// document should be included in results.
    pub fn scan_documents_filtered(
        &self,
        tenant_id: u32,
        collection: &str,
        limit: usize,
        predicate: &dyn Fn(&[u8]) -> bool,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let prefix = format!("{tenant_id}:{collection}:");
        let end = format!("{tenant_id}:{collection}:\u{ffff}");

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
            let value_bytes = entry.1.value();

            // Evaluate predicate on raw bytes — skip allocation if no match.
            if !predicate(value_bytes) {
                continue;
            }

            let key = entry.0.value().to_string();
            let doc_id = key.strip_prefix(&prefix).unwrap_or(&key).to_string();
            results.push((doc_id, value_bytes.to_vec()));
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
        let iter = table
            .range::<&str>(..)
            .map_err(|e| redb_err("iter docs", e))?;
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
        let iter = table
            .range::<&str>(..)
            .map_err(|e| redb_err("iter indexes", e))?;
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

    /// Scan ALL documents for a tenant across all collections.
    ///
    /// Returns `(full_key, value_bytes)` pairs. The key includes the tenant
    /// prefix: `"{tenant_id}:{collection}:{doc_id}"`.
    pub fn scan_all_for_tenant(&self, tenant_id: u32) -> crate::Result<Vec<(String, Vec<u8>)>> {
        self.scan_table_for_tenant(DOCUMENTS, tenant_id, "tenant scan")
    }

    /// Scan ALL index entries for a tenant.
    ///
    /// Returns `(full_key, value_bytes)` pairs from the INDEXES table.
    pub fn scan_indexes_for_tenant(&self, tenant_id: u32) -> crate::Result<Vec<(String, Vec<u8>)>> {
        self.scan_table_for_tenant(INDEXES, tenant_id, "index scan")
    }

    /// Shared scan logic for any redb table with tenant-prefixed keys.
    fn scan_table_for_tenant(
        &self,
        table_def: redb::TableDefinition<&str, &[u8]>,
        tenant_id: u32,
        label: &str,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let prefix = format!("{tenant_id}:");
        let end = format!("{tenant_id}:\u{ffff}");

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
