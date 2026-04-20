use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};
use tracing::{debug, info};

/// Table definition for the primary document store.
/// Key: "{tenant_id}:{collection}:{document_id}" → Value: document bytes.
pub(crate) const DOCUMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");

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

std::thread_local! {
    static KEY_BUF: std::cell::RefCell<String> = std::cell::RefCell::new(String::with_capacity(256));
}

/// Build a tenant-scoped composite key `"{tenant}:{a}:{b}"` using thread-local buffer.
fn with_tenant_key<R>(tenant_id: u32, a: &str, b: &str, f: impl FnOnce(&str) -> R) -> R {
    KEY_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
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
pub(super) fn with_tenant_key4<R>(
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

    /// Insert or update a document within an externally-owned write transaction.
    pub fn put_in_txn(
        &self,
        txn: &WriteTransaction,
        tenant_id: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> crate::Result<()> {
        with_tenant_key(tenant_id, collection, document_id, |key| {
            let mut table = txn
                .open_table(DOCUMENTS)
                .map_err(|e| redb_err("open table", e))?;
            table
                .insert(key, value)
                .map_err(|e| redb_err("insert", e))?;
            Ok(())
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
        tenant_id: u32,
        collection: &str,
        document_id: &str,
    ) -> crate::Result<bool> {
        with_tenant_key(tenant_id, collection, document_id, |key| {
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

    /// Batch insert or update multiple documents in a single redb transaction.
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

    /// Approximate byte count for all documents in a single
    /// `(tenant_id, collection)` pair. Sums the raw value sizes via a
    /// redb range scan — O(N) in row count for a single read
    /// transaction. Best-effort: redb key overhead + secondary-index
    /// bytes are not counted. Used by the
    /// `_system.dropped_collections.size_bytes_estimate` column.
    pub fn approx_bytes_for_collection(&self, tenant_id: u32, collection: &str) -> u64 {
        let prefix = format!("{tenant_id}:{collection}:");
        let end = format!("{tenant_id}:{collection}:\u{ffff}");
        let read_txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let table = match read_txn.open_table(DOCUMENTS) {
            Ok(t) => t,
            Err(_) => return 0,
        };
        let mut total: u64 = 0;
        let range = match table.range::<&str>(prefix.as_str()..end.as_str()) {
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

    /// Begin a write transaction on the underlying database.
    pub fn begin_write(&self) -> crate::Result<WriteTransaction> {
        self.db
            .begin_write()
            .map_err(|e| redb_err("begin write txn", e))
    }

    /// Get the underlying database handle.
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
        assert!(!engine.delete(1, "users", "u1").unwrap());
    }

    #[test]
    fn range_scan_with_index() {
        let (engine, _dir) = open_temp();
        engine.index_put(1, "users", "age", "025", "u1").unwrap();
        engine.index_put(1, "users", "age", "030", "u2").unwrap();
        engine.index_put(1, "users", "age", "035", "u3").unwrap();
        engine.index_put(1, "users", "age", "040", "u4").unwrap();
        let results = engine
            .range_scan(1, "users", "age", Some(b"025"), Some(b"036"), 10)
            .unwrap();
        assert_eq!(results.len(), 3);
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

    #[test]
    fn delete_index_entries_for_field() {
        let (engine, _dir) = open_temp();
        engine
            .index_put(1, "users", "email", "alice@example.com", "u1")
            .unwrap();
        engine
            .index_put(1, "users", "email", "bob@example.com", "u2")
            .unwrap();
        engine.index_put(1, "users", "age", "30", "u1").unwrap();
        engine.index_put(1, "users", "age", "25", "u2").unwrap();
        let removed = engine
            .delete_index_entries_for_field(1, "users", "email")
            .unwrap();
        assert_eq!(removed, 2);
        let age_entries = engine.scan_index_groups(1, "users", "age").unwrap();
        assert_eq!(age_entries.len(), 2);
        let email_entries = engine.scan_index_groups(1, "users", "email").unwrap();
        assert!(email_entries.is_empty());
    }
}
