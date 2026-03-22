//! redb-backed `StorageEngine` implementation.
//!
//! Pure Rust, ACID, no C dependencies. Works on native, WASM, and WASI.
//! Uses a single redb table with `(namespace, key)` → `value` mapping.
//!
//! redb is a B-tree KV store designed for embedded use — exactly what
//! Lite needs. No SQL parsing, no WAL journal mode, no spawn_blocking.

use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use redb::{Database, TableDefinition};

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};
use nodedb_types::Namespace;

/// Table definition: `(namespace_u8, key_bytes)` → `value_bytes`.
///
/// redb requires a fixed table name at compile time. We use a single table
/// and encode the namespace as the first byte of the key.
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

/// redb-backed KV storage.
///
/// Thread-safe via `Mutex<Database>`. On WASM (single-threaded) the mutex
/// is uncontended. On native, redb handles internal locking but we serialize
/// access to avoid holding multiple write transactions.
pub struct RedbStorage {
    db: Mutex<Database>,
}

impl RedbStorage {
    /// Open or create a database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LiteError> {
        let db = Database::create(path).map_err(|e| LiteError::Storage {
            detail: format!("redb open failed: {e}"),
        })?;
        Ok(Self { db: Mutex::new(db) })
    }

    /// Create an in-memory database (for testing and WASM without persistence).
    pub fn open_in_memory() -> Result<Self, LiteError> {
        let backend = redb::backends::InMemoryBackend::new();
        let db = Database::builder()
            .create_with_backend(backend)
            .map_err(|e| LiteError::Storage {
                detail: format!("redb in-memory create failed: {e}"),
            })?;
        Ok(Self { db: Mutex::new(db) })
    }

    /// Build the composite key: `[namespace_u8, ...key_bytes]`.
    fn make_key(ns: Namespace, key: &[u8]) -> Vec<u8> {
        let mut k = Vec::with_capacity(1 + key.len());
        k.push(ns as u8);
        k.extend_from_slice(key);
        k
    }

    /// Extract the user key from a composite key (strip the namespace byte).
    fn strip_ns(composite: &[u8]) -> &[u8] {
        if composite.len() > 1 {
            &composite[1..]
        } else {
            &[]
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl StorageEngine for RedbStorage {
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        let composite = Self::make_key(ns, key);
        let db = self.db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_read().map_err(|e| LiteError::Storage {
            detail: format!("read txn failed: {e}"),
        })?;
        let table = match txn.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => {
                return Err(LiteError::Storage {
                    detail: format!("open table failed: {e}"),
                });
            }
        };

        match table.get(composite.as_slice()) {
            Ok(Some(val)) => Ok(Some(val.value().to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(LiteError::Storage {
                detail: format!("get failed: {e}"),
            }),
        }
    }

    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        let composite = Self::make_key(ns, key);
        let db = self.db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_write().map_err(|e| LiteError::Storage {
            detail: format!("write txn failed: {e}"),
        })?;
        {
            let mut table = txn.open_table(TABLE).map_err(|e| LiteError::Storage {
                detail: format!("open table failed: {e}"),
            })?;
            table
                .insert(composite.as_slice(), value)
                .map_err(|e| LiteError::Storage {
                    detail: format!("insert failed: {e}"),
                })?;
        }
        txn.commit().map_err(|e| LiteError::Storage {
            detail: format!("commit failed: {e}"),
        })?;
        Ok(())
    }

    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        let composite = Self::make_key(ns, key);
        let db = self.db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_write().map_err(|e| LiteError::Storage {
            detail: format!("write txn failed: {e}"),
        })?;
        {
            let mut table = txn.open_table(TABLE).map_err(|e| LiteError::Storage {
                detail: format!("open table failed: {e}"),
            })?;
            table
                .remove(composite.as_slice())
                .map_err(|e| LiteError::Storage {
                    detail: format!("remove failed: {e}"),
                })?;
        }
        txn.commit().map_err(|e| LiteError::Storage {
            detail: format!("commit failed: {e}"),
        })?;
        Ok(())
    }

    async fn scan_prefix(
        &self,
        ns: Namespace,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, LiteError> {
        let ns_byte = ns as u8;
        let mut start_key = Vec::with_capacity(1 + prefix.len());
        start_key.push(ns_byte);
        start_key.extend_from_slice(prefix);

        let db = self.db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_read().map_err(|e| LiteError::Storage {
            detail: format!("read txn failed: {e}"),
        })?;
        let table = match txn.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => {
                return Err(LiteError::Storage {
                    detail: format!("open table failed: {e}"),
                });
            }
        };

        let mut results = Vec::new();

        // redb range scan: from start_key to end of namespace.
        let range = table
            .range(start_key.as_slice()..)
            .map_err(|e| LiteError::Storage {
                detail: format!("range scan failed: {e}"),
            })?;

        for entry in range {
            let entry = entry.map_err(|e| LiteError::Storage {
                detail: format!("range iteration failed: {e}"),
            })?;
            let k = entry.0.value();

            // Stop if we've left this namespace.
            if k[0] != ns_byte {
                break;
            }

            let user_key = Self::strip_ns(k);

            // If prefix is non-empty, check it matches.
            if !prefix.is_empty() && !user_key.starts_with(prefix) {
                break;
            }

            results.push((user_key.to_vec(), entry.1.value().to_vec()));
        }

        Ok(results)
    }

    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
        if ops.is_empty() {
            return Ok(());
        }

        let db = self.db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_write().map_err(|e| LiteError::Storage {
            detail: format!("write txn failed: {e}"),
        })?;
        {
            let mut table = txn.open_table(TABLE).map_err(|e| LiteError::Storage {
                detail: format!("open table failed: {e}"),
            })?;

            for op in ops {
                match op {
                    WriteOp::Put { ns, key, value } => {
                        let composite = Self::make_key(*ns, key);
                        table
                            .insert(composite.as_slice(), value.as_slice())
                            .map_err(|e| LiteError::Storage {
                                detail: format!("batch insert failed: {e}"),
                            })?;
                    }
                    WriteOp::Delete { ns, key } => {
                        let composite = Self::make_key(*ns, key);
                        table
                            .remove(composite.as_slice())
                            .map_err(|e| LiteError::Storage {
                                detail: format!("batch remove failed: {e}"),
                            })?;
                    }
                }
            }
        }
        txn.commit().map_err(|e| LiteError::Storage {
            detail: format!("batch commit failed: {e}"),
        })?;
        Ok(())
    }

    async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
        let ns_byte = ns as u8;
        let start = vec![ns_byte];
        let end = vec![ns_byte + 1];

        let db = self.db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_read().map_err(|e| LiteError::Storage {
            detail: format!("read txn failed: {e}"),
        })?;
        let table = match txn.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(e) => {
                return Err(LiteError::Storage {
                    detail: format!("open table failed: {e}"),
                });
            }
        };

        let range =
            table
                .range(start.as_slice()..end.as_slice())
                .map_err(|e| LiteError::Storage {
                    detail: format!("count range failed: {e}"),
                })?;

        let mut count = 0u64;
        for entry in range {
            let _ = entry.map_err(|e| LiteError::Storage {
                detail: format!("count iteration failed: {e}"),
            })?;
            count += 1;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_storage() -> RedbStorage {
        RedbStorage::open_in_memory().unwrap()
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let s = make_storage();
        s.put(Namespace::Vector, b"v1", b"hello").await.unwrap();
        let val = s.get(Namespace::Vector, b"v1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"hello".as_slice()));
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let s = make_storage();
        let val = s.get(Namespace::Vector, b"nope").await.unwrap();
        assert!(val.is_none());
    }

    #[tokio::test]
    async fn put_overwrites() {
        let s = make_storage();
        s.put(Namespace::Graph, b"k", b"first").await.unwrap();
        s.put(Namespace::Graph, b"k", b"second").await.unwrap();
        let val = s.get(Namespace::Graph, b"k").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"second".as_slice()));
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let s = make_storage();
        s.put(Namespace::Crdt, b"k", b"val").await.unwrap();
        s.delete(Namespace::Crdt, b"k").await.unwrap();
        assert!(s.get(Namespace::Crdt, b"k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_is_noop() {
        let s = make_storage();
        s.delete(Namespace::Meta, b"ghost").await.unwrap();
    }

    #[tokio::test]
    async fn namespaces_are_isolated() {
        let s = make_storage();
        s.put(Namespace::Vector, b"k", b"vec").await.unwrap();
        s.put(Namespace::Graph, b"k", b"graph").await.unwrap();

        assert_eq!(
            s.get(Namespace::Vector, b"k").await.unwrap().as_deref(),
            Some(b"vec".as_slice())
        );
        assert_eq!(
            s.get(Namespace::Graph, b"k").await.unwrap().as_deref(),
            Some(b"graph".as_slice())
        );
    }

    #[tokio::test]
    async fn scan_prefix_basic() {
        let s = make_storage();
        s.put(Namespace::Vector, b"vec:001", b"a").await.unwrap();
        s.put(Namespace::Vector, b"vec:002", b"b").await.unwrap();
        s.put(Namespace::Vector, b"vec:003", b"c").await.unwrap();
        s.put(Namespace::Vector, b"other:001", b"d").await.unwrap();

        let results = s.scan_prefix(Namespace::Vector, b"vec:").await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"vec:001");
        assert_eq!(results[1].0, b"vec:002");
        assert_eq!(results[2].0, b"vec:003");
    }

    #[tokio::test]
    async fn scan_prefix_empty_returns_all() {
        let s = make_storage();
        s.put(Namespace::Meta, b"a", b"1").await.unwrap();
        s.put(Namespace::Meta, b"b", b"2").await.unwrap();
        s.put(Namespace::Vector, b"c", b"3").await.unwrap();

        let results = s.scan_prefix(Namespace::Meta, b"").await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn scan_prefix_no_match() {
        let s = make_storage();
        s.put(Namespace::Graph, b"edge:1", b"data").await.unwrap();
        let results = s.scan_prefix(Namespace::Graph, b"node:").await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn batch_write_atomic() {
        let s = make_storage();
        s.put(Namespace::Crdt, b"to_delete", b"old").await.unwrap();

        s.batch_write(&[
            WriteOp::Put {
                ns: Namespace::Crdt,
                key: b"new1".to_vec(),
                value: b"val1".to_vec(),
            },
            WriteOp::Put {
                ns: Namespace::Crdt,
                key: b"new2".to_vec(),
                value: b"val2".to_vec(),
            },
            WriteOp::Delete {
                ns: Namespace::Crdt,
                key: b"to_delete".to_vec(),
            },
        ])
        .await
        .unwrap();

        assert!(s.get(Namespace::Crdt, b"new1").await.unwrap().is_some());
        assert!(s.get(Namespace::Crdt, b"new2").await.unwrap().is_some());
        assert!(
            s.get(Namespace::Crdt, b"to_delete")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn batch_write_empty_is_noop() {
        let s = make_storage();
        s.batch_write(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn count_entries() {
        let s = make_storage();
        assert_eq!(s.count(Namespace::Vector).await.unwrap(), 0);

        s.put(Namespace::Vector, b"v1", b"a").await.unwrap();
        s.put(Namespace::Vector, b"v2", b"b").await.unwrap();
        s.put(Namespace::Graph, b"g1", b"c").await.unwrap();

        assert_eq!(s.count(Namespace::Vector).await.unwrap(), 2);
        assert_eq!(s.count(Namespace::Graph).await.unwrap(), 1);
        assert_eq!(s.count(Namespace::Crdt).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn large_value_roundtrip() {
        let s = make_storage();
        let large = vec![0xABu8; 1_000_000];
        s.put(Namespace::Vector, b"hnsw:layer0", &large)
            .await
            .unwrap();
        let val = s.get(Namespace::Vector, b"hnsw:layer0").await.unwrap();
        assert_eq!(val.unwrap().len(), 1_000_000);
    }

    #[tokio::test]
    async fn binary_keys_work() {
        let s = make_storage();
        let key = vec![0x00, 0x01, 0xFF, 0xFE];
        s.put(Namespace::LoroState, &key, b"binary_key_val")
            .await
            .unwrap();
        let val = s.get(Namespace::LoroState, &key).await.unwrap();
        assert_eq!(val.as_deref(), Some(b"binary_key_val".as_slice()));
    }

    #[tokio::test]
    async fn open_file_based() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.redb");

        {
            let s = RedbStorage::open(&path).unwrap();
            s.put(Namespace::Meta, b"key", b"persistent").await.unwrap();
        }
        {
            let s = RedbStorage::open(&path).unwrap();
            let val = s.get(Namespace::Meta, b"key").await.unwrap();
            assert_eq!(val.as_deref(), Some(b"persistent".as_slice()));
        }
    }
}
