use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition};
use tracing::{debug, info};

/// Table definition for the primary document store.
/// Key: "{tenant_id}:{collection}:{document_id}" → Value: document bytes.
pub(super) const DOCUMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");

/// Table definition for secondary indexes.
/// Key: "{tenant_id}:{collection}:{field}:{value}:{document_id}" → Value: empty (existence index).
pub(super) const INDEXES: TableDefinition<&str, &[u8]> = TableDefinition::new("indexes");

/// Map a redb error into our crate error with context.
pub(super) fn redb_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("{ctx}: {e}"),
    }
}

// Thread-local reusable buffer for building composite keys without heap allocation.
// Most composite keys are short (collection + doc_id < 256 bytes), so this avoids
// a `format!()` heap allocation on every get/put/delete in the hot path.
std::thread_local! {
    static KEY_BUF: std::cell::RefCell<String> = std::cell::RefCell::new(String::with_capacity(256));
}

/// Build a tenant-scoped composite key `"{tenant}:{a}:{b}"` using thread-local buffer.
fn with_tenant_key<R>(tenant_id: u32, a: &str, b: &str, f: impl FnOnce(&str) -> R) -> R {
    KEY_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        // Use itoa-style formatting to avoid heap allocation for the u32.
        use std::fmt::Write;
        let _ = write!(buf, "{tenant_id}");
        buf.push(':');
        buf.push_str(a);
        buf.push(':');
        buf.push_str(b);
        f(&buf)
    })
}

/// Build a tenant-scoped index key `"{tenant}:{a}:{b}:{c}:{d}"`.
fn with_tenant_key4<R>(
    tenant_id: u32,
    a: &str,
    b: &str,
    c: &str,
    d: &str,
    f: impl FnOnce(&str) -> R,
) -> R {
    KEY_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        use std::fmt::Write;
        let _ = write!(buf, "{tenant_id}");
        buf.push(':');
        buf.push_str(a);
        buf.push(':');
        buf.push_str(b);
        buf.push(':');
        buf.push_str(c);
        buf.push(':');
        buf.push_str(d);
        f(&buf)
    })
}

/// redb-backed B-Tree storage engine for sparse/metadata queries.
///
/// Provides ACID point lookups, range scans, and secondary index support
/// via redb's embedded B-Tree. This is the Sparse & Metadata engine.
///
/// Thread-safety: redb `Database` is `Send + Sync` — safe for Control Plane.
/// For Data Plane (single-core), each core gets its own `SparseEngine` instance
/// or accesses the shared `Database` via read transactions (lock-free readers).
pub struct SparseEngine {
    pub(super) db: Arc<Database>,
}

impl SparseEngine {
    /// Open or create the sparse engine database at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| redb_err("open", e))?;

        // Ensure tables exist.
        let write_txn = db.begin_write().map_err(|e| redb_err("write txn", e))?;
        {
            let _ = write_txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open documents table", e))?;
            let _ = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes table", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;

        info!(path = %path.display(), "sparse engine opened");

        Ok(Self { db: Arc::new(db) })
    }

    /// Insert or update a document (tenant-scoped).
    pub fn put(
        &self,
        tenant_id: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> crate::Result<()> {
        with_tenant_key(tenant_id, collection, document_id, |key| {
            let write_txn = self
                .db
                .begin_write()
                .map_err(|e| redb_err("write txn", e))?;
            {
                let mut table = write_txn
                    .open_table(DOCUMENTS)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .insert(key, value)
                    .map_err(|e| redb_err("insert", e))?;
            }
            write_txn.commit().map_err(|e| redb_err("commit", e))?;

            debug!(collection, document_id, len = value.len(), "document put");
            Ok(())
        })
    }

    /// Batch insert or update multiple documents in a single redb transaction.
    ///
    /// Amortizes the write transaction overhead: one `begin_write()` + one
    /// `commit()` for all documents, instead of one per document. Critical
    /// for bulk ingestion and sync endpoint delta application.
    pub fn batch_put(
        &self,
        tenant_id: u32,
        collection: &str,
        documents: &[(&str, &[u8])],
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
        tenant_id: u32,
        collection: &str,
        document_id: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        with_tenant_key(tenant_id, collection, document_id, |key| {
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

    /// Delete a document (tenant-scoped).
    pub fn delete(
        &self,
        tenant_id: u32,
        collection: &str,
        document_id: &str,
    ) -> crate::Result<bool> {
        with_tenant_key(tenant_id, collection, document_id, |key| {
            let write_txn = self
                .db
                .begin_write()
                .map_err(|e| redb_err("write txn", e))?;
            let removed = {
                let mut table = write_txn
                    .open_table(DOCUMENTS)
                    .map_err(|e| redb_err("open table", e))?;
                table
                    .remove(key)
                    .map_err(|e| redb_err("remove", e))?
                    .is_some()
            };
            write_txn.commit().map_err(|e| redb_err("commit", e))?;

            debug!(collection, document_id, removed, "document delete");
            Ok(removed)
        })
    }

    /// Delete all secondary index entries for a document.
    ///
    /// Scans the INDEXES table for entries ending with `:{document_id}` and
    /// removes them. Called during document deletion cascade.
    pub fn delete_indexes_for_document(
        &self,
        tenant_id: u32,
        collection: &str,
        document_id: &str,
    ) -> crate::Result<()> {
        let prefix = format!("{tenant_id}:{collection}:");
        let end = format!("{tenant_id}:{collection}:\u{ffff}");
        let suffix = format!(":{document_id}");

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(INDEXES)
                .map_err(|e| redb_err("open indexes", e))?;

            // Collect matching keys first (can't mutate during iteration).
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
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit index cascade", e))?;

        Ok(())
    }

    /// Range scan: retrieve documents in a collection with keys in [lower, upper).
    ///
    /// The `field` parameter scopes the scan prefix. Returns up to `limit` results.
    pub fn range_scan(
        &self,
        tenant_id: u32,
        collection: &str,
        field: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let prefix = format!("{tenant_id}:{collection}:{field}:");

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
                // Increment last byte for exclusive upper bound.
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

    /// Insert a secondary index entry for range scan support (tenant-scoped).
    pub fn index_put(
        &self,
        tenant_id: u32,
        collection: &str,
        field: &str,
        value: &str,
        document_id: &str,
    ) -> crate::Result<()> {
        with_tenant_key4(tenant_id, collection, field, value, document_id, |key| {
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
        })
    }

    /// Index-only scan: return `(doc_id, field_value)` pairs from the
    /// INDEXES table without touching the DOCUMENTS table.
    ///
    /// Used when the query projection only needs the indexed field and doc_id.
    /// Avoids document deserialization entirely — O(index_entries) with zero
    /// allocation per document.
    pub fn scan_index_values(
        &self,
        tenant_id: u32,
        collection: &str,
        field: &str,
        limit: usize,
    ) -> crate::Result<Vec<(String, String)>> {
        let prefix = format!("{tenant_id}:{collection}:{field}:");
        let end = format!("{tenant_id}:{collection}:{field}:\u{ffff}");

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
            // Key format: "{tenant}:{collection}:{field}:{value}:{doc_id}"
            if let Some(rest) = key.strip_prefix(&prefix)
                && let Some(colon_pos) = rest.rfind(':')
            {
                let value = &rest[..colon_pos];
                let doc_id = &rest[colon_pos + 1..];
                results.push((doc_id.to_string(), value.to_string()));
            }
        }

        debug!(collection, field, count = results.len(), "index-only scan");
        Ok(results)
    }

    /// Insert a document by raw pre-formed key (snapshot restore).
    ///
    /// The key already includes the `{tenant_id}:{collection}:{doc_id}` prefix.
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

    /// Get the underlying database handle (for advanced use / shared access).
    pub fn db(&self) -> &Arc<Database> {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
        (engine, dir)
    }

    #[test]
    fn put_and_get() {
        let (engine, _dir) = open_temp();

        engine.put(1, "users", "u1", b"alice").unwrap();
        engine.put(1, "users", "u2", b"bob").unwrap();

        assert_eq!(
            engine.get(1, "users", "u1").unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(engine.get(1, "users", "u2").unwrap(), Some(b"bob".to_vec()));
        assert_eq!(engine.get(1, "users", "u3").unwrap(), None);
    }

    #[test]
    fn put_overwrites() {
        let (engine, _dir) = open_temp();

        engine.put(1, "users", "u1", b"alice").unwrap();
        engine.put(1, "users", "u1", b"ALICE").unwrap();

        assert_eq!(
            engine.get(1, "users", "u1").unwrap(),
            Some(b"ALICE".to_vec())
        );
    }

    #[test]
    fn delete_removes() {
        let (engine, _dir) = open_temp();

        engine.put(1, "users", "u1", b"alice").unwrap();
        assert!(engine.delete(1, "users", "u1").unwrap());
        assert_eq!(engine.get(1, "users", "u1").unwrap(), None);
        // Double delete returns false.
        assert!(!engine.delete(1, "users", "u1").unwrap());
    }

    #[test]
    fn range_scan_with_index() {
        let (engine, _dir) = open_temp();

        // Insert index entries: users by age.
        engine.index_put(1, "users", "age", "025", "u1").unwrap();
        engine.index_put(1, "users", "age", "030", "u2").unwrap();
        engine.index_put(1, "users", "age", "035", "u3").unwrap();
        engine.index_put(1, "users", "age", "040", "u4").unwrap();

        // Scan age [025, 036).
        let results = engine
            .range_scan(1, "users", "age", Some(b"025"), Some(b"036"), 10)
            .unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].0.contains("025"));
        assert!(results[2].0.contains("035"));
    }

    #[test]
    fn range_scan_respects_limit() {
        let (engine, _dir) = open_temp();

        for i in 0..20 {
            engine
                .index_put(1, "logs", "ts", &format!("{i:04}"), &format!("doc{i}"))
                .unwrap();
        }

        let results = engine.range_scan(1, "logs", "ts", None, None, 5).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn collections_are_isolated() {
        let (engine, _dir) = open_temp();

        engine.put(1, "users", "u1", b"alice").unwrap();
        engine.put(1, "orders", "u1", b"order-1").unwrap();

        assert_eq!(
            engine.get(1, "users", "u1").unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get(1, "orders", "u1").unwrap(),
            Some(b"order-1".to_vec())
        );
    }
}
