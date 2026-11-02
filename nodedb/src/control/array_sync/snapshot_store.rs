// SPDX-License-Identifier: BUSL-1.1

//! [`OriginSnapshotStore`] — persistent store for array tile snapshots.
//!
//! Snapshots are stored in a database-and-tenant-scoped redb table.

use std::path::Path;
use std::sync::Arc;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::snapshot::{SnapshotSink, TileSnapshot, decode_snapshot, encode_snapshot};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::warn;

/// Database-scoped snapshot table.
const SNAPSHOT_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("array_tile_snapshots");

fn store_err(detail: String) -> crate::Error {
    crate::Error::Storage {
        engine: "array_sync".into(),
        detail,
    }
}

/// Key: `[database_id: u64 BE][tenant_id: u64 BE][name_len: u16 BE][name][hlc: 18 bytes]`.
fn snapshot_key(
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    array: &str,
    hlc: Hlc,
) -> Option<Vec<u8>> {
    let name = array.as_bytes();
    let name_len = u16::try_from(name.len()).ok()?;
    let mut key = Vec::with_capacity(8 + 8 + 2 + name.len() + 18);
    key.extend_from_slice(&database_id.as_u64().to_be_bytes());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name);
    key.extend_from_slice(&hlc.to_bytes());
    Some(key)
}

fn name_prefix(
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    array: &str,
) -> Option<Vec<u8>> {
    let name = array.as_bytes();
    let name_len = u16::try_from(name.len()).ok()?;
    let mut prefix = Vec::with_capacity(8 + 8 + 2 + name.len());
    prefix.extend_from_slice(&database_id.as_u64().to_be_bytes());
    prefix.extend_from_slice(&tenant_id.to_be_bytes());
    prefix.extend_from_slice(&name_len.to_be_bytes());
    prefix.extend_from_slice(name);
    Some(prefix)
}

fn hlc_from_key(key: &[u8], prefix_len: usize) -> Option<Hlc> {
    if key.len() != prefix_len + 18 {
        return None;
    }
    let bytes: [u8; 18] = key[prefix_len..].try_into().ok()?;
    Some(Hlc::from_bytes(&bytes))
}

/// Persistent tile snapshot store for Origin array GC and catch-up serving.
///
/// Thread-safe; `Arc`-wrapped by callers.
pub struct OriginSnapshotStore {
    db: Arc<Database>,
}

impl crate::storage::RedbBacked for OriginSnapshotStore {
    fn redb_database(&self) -> &redb::Database {
        &self.db
    }
}

impl OriginSnapshotStore {
    /// Open or create the snapshot database at `{data_dir}/array_sync/snapshots.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Arc<Self>> {
        let dir = data_dir.join("array_sync");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;
        let path = dir.join("snapshots.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("open snapshots db {}: {e}", path.display()),
        })?;
        Self::init(db)
    }

    /// In-memory-only store for tests.
    pub fn open_in_memory() -> crate::Result<Arc<Self>> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("in-memory snapshots db: {e}"),
            })?;
        Self::init(db)
    }

    fn init(db: Database) -> crate::Result<Arc<Self>> {
        let txn = db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("snapshot_store init begin_write: {e}"),
        })?;
        txn.open_table(SNAPSHOT_TABLE)
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("snapshot_store init open v2 table: {e}"),
            })?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("snapshot_store init commit: {e}"),
        })?;
        Ok(Arc::new(Self { db: Arc::new(db) }))
    }

    /// Persist a DEFAULT-database snapshot. This is the compatibility wrapper
    /// used by [`SnapshotSink`].
    pub fn put(&self, snapshot: &TileSnapshot) -> crate::Result<()> {
        self.put_in_database(crate::types::DatabaseId::DEFAULT, 0, snapshot)
    }

    /// Persist a snapshot under an explicit structural database identity.
    pub fn put_in_database(
        &self,
        database_id: crate::types::DatabaseId,
        tenant_id: u64,
        snapshot: &TileSnapshot,
    ) -> crate::Result<()> {
        let key = snapshot_key(
            database_id,
            tenant_id,
            &snapshot.array,
            snapshot.snapshot_hlc,
        )
        .ok_or_else(|| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("snapshot_store: array name too long: '{}'", snapshot.array),
        })?;
        let encoded = encode_snapshot(snapshot).map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("snapshot_store encode: {e}"),
        })?;
        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("snapshot_store put begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("snapshot_store put open v2 table: {e}"),
                })?;
            table
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("snapshot_store put insert: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("snapshot_store put commit: {e}"),
        })
    }

    /// Retrieve a DEFAULT-database snapshot by exact `(array, hlc)`.
    pub fn get(&self, array: &str, hlc: Hlc) -> Option<TileSnapshot> {
        self.get_in_database(crate::types::DatabaseId::DEFAULT, 0, array, hlc)
    }

    /// Retrieve a snapshot by exact structural database identity and array name.
    pub fn get_in_database(
        &self,
        database_id: crate::types::DatabaseId,
        tenant_id: u64,
        array: &str,
        hlc: Hlc,
    ) -> Option<TileSnapshot> {
        let key = snapshot_key(database_id, tenant_id, array, hlc)?;
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(SNAPSHOT_TABLE).ok()?;
        let entry = table.get(key.as_slice()).ok()??;
        Self::decode(array, entry.value(), "get")
    }

    /// Return the latest DEFAULT-database snapshot for `array`.
    pub fn latest_for_array(&self, array: &str) -> Option<TileSnapshot> {
        self.latest_for_array_in_database(crate::types::DatabaseId::DEFAULT, 0, array)
    }

    /// Return the latest snapshot for one database/array scope.
    pub fn latest_for_array_in_database(
        &self,
        database_id: crate::types::DatabaseId,
        tenant_id: u64,
        array: &str,
    ) -> Option<TileSnapshot> {
        let prefix = name_prefix(database_id, tenant_id, array)?;
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(SNAPSHOT_TABLE).ok()?;
        let bytes = Self::latest_bytes(&table, &prefix)?;
        Self::decode(array, &bytes, "latest")
    }

    /// Delete obsolete snapshots in one explicit database scope.
    ///
    /// Returns how many snapshots it removed. Every error propagates: a
    /// discarded one leaves obsolete snapshots on disk that nothing retries,
    /// so the store grows without bound and no caller learns why.
    pub fn delete_older_than_in_database(
        &self,
        database_id: crate::types::DatabaseId,
        tenant_id: u64,
        array: &str,
        older_than: Hlc,
    ) -> crate::Result<usize> {
        let prefix = name_prefix(database_id, tenant_id, array)
            .ok_or_else(|| store_err(format!("array name '{array}' exceeds the key length")))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| store_err(format!("snapshot delete write txn: {e}")))?;
        let removed = {
            let mut table = txn
                .open_table(SNAPSHOT_TABLE)
                .map_err(|e| store_err(format!("open snapshots: {e}")))?;
            let mut keys: Vec<Vec<u8>> = Vec::new();
            for entry in table
                .iter()
                .map_err(|e| store_err(format!("scan snapshots: {e}")))?
            {
                let (key, _) = entry.map_err(|e| store_err(format!("read snapshot: {e}")))?;
                let key = key.value();
                if key.starts_with(&prefix)
                    && hlc_from_key(key, prefix.len()).is_some_and(|hlc| hlc < older_than)
                {
                    keys.push(key.to_vec());
                }
            }
            for key in &keys {
                table
                    .remove(key.as_slice())
                    .map_err(|e| store_err(format!("remove snapshot: {e}")))?;
            }
            keys.len()
        };
        txn.commit()
            .map_err(|e| store_err(format!("commit snapshot delete: {e}")))?;
        Ok(removed)
    }

    fn latest_bytes(
        table: &impl ReadableTable<&'static [u8], &'static [u8]>,
        prefix: &[u8],
    ) -> Option<Vec<u8>> {
        table
            .iter()
            .ok()?
            .filter_map(|entry| entry.ok())
            .rfind(|(key, _)| key.value().starts_with(prefix))
            .map(|(_, value)| value.value().to_vec())
    }

    fn decode(array: &str, bytes: &[u8], operation: &str) -> Option<TileSnapshot> {
        let snapshot = decode_snapshot(bytes)
            .map_err(|error| warn!(array = %array, error = %error, operation, "snapshot_store: decode error"))
            .ok()?;
        if snapshot.array != array {
            warn!(
                expected_array = %array,
                actual_array = %snapshot.array,
                operation,
                "snapshot_store: snapshot payload array does not match its scoped key"
            );
            return None;
        }
        Some(snapshot)
    }
}

impl SnapshotSink for OriginSnapshotStore {
    /// SnapshotSink has no database identity, so it intentionally targets DEFAULT.
    fn write_snapshot(&self, snapshot: &TileSnapshot) -> nodedb_array::error::ArrayResult<()> {
        self.put_in_database(crate::types::DatabaseId::DEFAULT, 0, snapshot)
            .map_err(|e| nodedb_array::error::ArrayError::SegmentCorruption {
                detail: format!("OriginSnapshotStore::write_snapshot: {e}"),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::sync::snapshot::CoordRange;
    use nodedb_array::types::coord::value::CoordValue;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(1)).unwrap()
    }

    fn snap(array: &str, hlc_ms: u64) -> TileSnapshot {
        TileSnapshot {
            array: array.to_owned(),
            coord_range: CoordRange {
                lo: vec![CoordValue::Int64(0)],
                hi: vec![CoordValue::Int64(100)],
            },
            tile_blob: vec![0xAB; 32],
            snapshot_hlc: hlc(hlc_ms),
            schema_hlc: hlc(1),
        }
    }

    fn store() -> Arc<OriginSnapshotStore> {
        OriginSnapshotStore::open_in_memory().unwrap()
    }

    #[test]
    fn put_and_get_roundtrip() {
        let s = store();
        let snapshot = snap("arr", 100);
        s.put(&snapshot).unwrap();
        assert_eq!(
            s.get("arr", hlc(100)).unwrap().tile_blob,
            snapshot.tile_blob
        );
    }

    #[test]
    fn same_name_is_isolated_by_tenant() {
        let s = store();
        let database_id = crate::types::DatabaseId::new(1);
        let mut one = snap("same", 100);
        one.tile_blob = vec![1];
        let mut two = snap("same", 100);
        two.tile_blob = vec![2];
        s.put_in_database(database_id, 1, &one).unwrap();
        s.put_in_database(database_id, 2, &two).unwrap();
        let loaded_one = s.get_in_database(database_id, 1, "same", hlc(100)).unwrap();
        let loaded_two = s.get_in_database(database_id, 2, "same", hlc(100)).unwrap();
        assert_eq!(loaded_one.array, "same");
        assert_eq!(loaded_one.tile_blob, vec![1]);
        assert_eq!(loaded_two.tile_blob, vec![2]);
    }

    #[test]
    fn a_replacing_put_supersedes_the_snapshot_at_the_same_key() {
        let s = store();
        let mut first = snap("arr", 100);
        first.tile_blob = vec![1];
        s.put(&first).unwrap();
        let mut second = snap("arr", 100);
        second.tile_blob = vec![2];
        s.put(&second).unwrap();
        assert_eq!(s.get("arr", hlc(100)).unwrap().tile_blob, vec![2]);
    }

    #[test]
    fn latest_and_delete_are_scoped() {
        let s = store();
        let db_one = crate::types::DatabaseId::new(1);
        let db_two = crate::types::DatabaseId::new(2);
        s.put_in_database(db_one, 0, &snap("arr", 10)).unwrap();
        s.put_in_database(db_one, 0, &snap("arr", 30)).unwrap();
        s.put_in_database(db_two, 0, &snap("arr", 20)).unwrap();
        let removed = s
            .delete_older_than_in_database(db_one, 0, "arr", hlc(20))
            .expect("delete older than");
        assert_eq!(removed, 1, "only the snapshot below the cutoff is removed");
        assert!(s.get_in_database(db_one, 0, "arr", hlc(10)).is_none());
        assert_eq!(
            s.latest_for_array_in_database(db_one, 0, "arr")
                .unwrap()
                .snapshot_hlc,
            hlc(30)
        );
        assert_eq!(
            s.latest_for_array_in_database(db_two, 0, "arr")
                .unwrap()
                .snapshot_hlc,
            hlc(20)
        );
    }

    #[test]
    fn wrong_array_payload_is_not_returned_for_scoped_key() {
        let s = store();
        let stored_as = "arr";
        let wrong = snap("other", 100);
        let key = snapshot_key(
            crate::types::DatabaseId::DEFAULT,
            0,
            stored_as,
            wrong.snapshot_hlc,
        )
        .unwrap();
        let value = encode_snapshot(&wrong).unwrap();
        let txn = s.db.begin_write().unwrap();
        let mut table = txn.open_table(SNAPSHOT_TABLE).unwrap();
        table.insert(key.as_slice(), value.as_slice()).unwrap();
        drop(table);
        txn.commit().unwrap();

        assert!(s.latest_for_array(stored_as).is_none());
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_id = crate::types::DatabaseId::new(42);
        {
            let s = OriginSnapshotStore::open(dir.path()).unwrap();
            s.put_in_database(db_id, 0, &snap("arr", 100)).unwrap();
        }
        let s = OriginSnapshotStore::open(dir.path()).unwrap();
        assert_eq!(
            s.get_in_database(db_id, 0, "arr", hlc(100))
                .unwrap()
                .snapshot_hlc,
            hlc(100)
        );
    }

    #[test]
    fn sink_impl_targets_default() {
        let s = store();
        let snapshot = snap("arr", 200);
        SnapshotSink::write_snapshot(s.as_ref(), &snapshot).unwrap();
        assert!(s.get("arr", hlc(200)).is_some());
    }
}
