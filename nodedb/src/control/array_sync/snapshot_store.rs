//! [`OriginSnapshotStore`] — persistent store for array tile snapshots.
//!
//! Snapshots are keyed by `(array_name, snapshot_hlc_bytes)` in a redb table
//! at `{data_dir}/array_sync/snapshots.redb`. They are written during GC
//! (via the [`nodedb_array::sync::snapshot::SnapshotSink`] impl) and read
//! back when serving catch-up requests whose `from_hlc` is below the GC
//! boundary.
//!
//! # Key layout
//!
//! ```text
//! [name_len: u8][name bytes][snapshot_hlc: 18 bytes]
//! ```
//!
//! Lexicographic ordering within a name prefix produces chronological
//! ordering by `snapshot_hlc` — `latest_for_array` scans the prefix and
//! returns the last entry.

use std::path::Path;
use std::sync::Arc;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::snapshot::{SnapshotSink, TileSnapshot, decode_snapshot, encode_snapshot};
use redb::{Database, ReadableTable, TableDefinition};
use tracing::warn;

/// redb table: composite key bytes → msgpack-encoded `TileSnapshot`.
const SNAPSHOT_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("array_tile_snapshots");

/// Build the composite key: `[name_len: u8][name bytes][hlc: 18 bytes]`.
fn snapshot_key(array: &str, hlc: Hlc) -> Option<Vec<u8>> {
    let name_bytes = array.as_bytes();
    let name_len = u8::try_from(name_bytes.len()).ok()?;
    let mut key = Vec::with_capacity(1 + name_bytes.len() + 18);
    key.push(name_len);
    key.extend_from_slice(name_bytes);
    key.extend_from_slice(&hlc.to_bytes());
    Some(key)
}

/// Build the name-prefix used for range scans: `[name_len: u8][name bytes]`.
fn name_prefix(array: &str) -> Option<Vec<u8>> {
    let name_bytes = array.as_bytes();
    let name_len = u8::try_from(name_bytes.len()).ok()?;
    let mut prefix = Vec::with_capacity(1 + name_bytes.len());
    prefix.push(name_len);
    prefix.extend_from_slice(name_bytes);
    Some(prefix)
}

/// Parse the HLC from the tail of a composite key given the prefix length.
fn hlc_from_key(key: &[u8], prefix_len: usize) -> Option<Hlc> {
    if key.len() < prefix_len + 18 {
        return None;
    }
    let start = key.len() - 18;
    let bytes: [u8; 18] = key[start..].try_into().ok()?;
    Some(Hlc::from_bytes(&bytes))
}

/// Persistent tile snapshot store for Origin array GC and catch-up serving.
///
/// Thread-safe; `Arc`-wrapped by callers.
pub struct OriginSnapshotStore {
    db: Arc<Database>,
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
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("snapshot_store init begin_write: {e}"),
            })?;
            txn.open_table(SNAPSHOT_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("snapshot_store init open_table: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("snapshot_store init commit: {e}"),
            })?;
        }
        Ok(Arc::new(Self { db: Arc::new(db) }))
    }

    /// Persist a snapshot.
    pub fn put(&self, snapshot: &TileSnapshot) -> crate::Result<()> {
        let key = snapshot_key(&snapshot.array, snapshot.snapshot_hlc).ok_or_else(|| {
            crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("snapshot_store: array name too long: '{}'", snapshot.array),
            }
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
                    detail: format!("snapshot_store put open_table: {e}"),
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

    /// Retrieve a snapshot by exact `(array, hlc)`.
    pub fn get(&self, array: &str, hlc: Hlc) -> Option<TileSnapshot> {
        let key = snapshot_key(array, hlc)?;
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(SNAPSHOT_TABLE).ok()?;
        let entry = table.get(key.as_slice()).ok()??;
        decode_snapshot(entry.value())
            .map_err(|e| {
                warn!(array = %array, error = %e, "snapshot_store: decode error on get");
            })
            .ok()
    }

    /// Return the snapshot with the highest HLC for `array`, or `None` if none exist.
    pub fn latest_for_array(&self, array: &str) -> Option<TileSnapshot> {
        let prefix = name_prefix(array)?;
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(SNAPSHOT_TABLE).ok()?;

        // Scan the full range and collect prefix-matched keys; last = latest HLC.
        let iter = table.iter().ok()?;
        let mut last_bytes: Option<Vec<u8>> = None;
        for entry in iter {
            let Ok((k, v)) = entry else {
                continue;
            };
            let key = k.value();
            if key.starts_with(prefix.as_slice()) {
                last_bytes = Some(v.value().to_vec());
            }
        }

        let bytes = last_bytes?;
        decode_snapshot(&bytes)
            .map_err(|e| {
                warn!(array = %array, error = %e, "snapshot_store: decode error on latest");
            })
            .ok()
    }

    /// Delete all snapshots for `array` whose `snapshot_hlc < older_than`.
    ///
    /// Used after GC to evict superseded snapshots.
    pub fn delete_older_than(&self, array: &str, older_than: Hlc) {
        let Some(prefix) = name_prefix(array) else {
            return;
        };
        let Ok(txn) = self.db.begin_write() else {
            return;
        };
        let Ok(mut table) = txn.open_table(SNAPSHOT_TABLE) else {
            return;
        };

        let keys_to_delete: Vec<Vec<u8>> = table
            .iter()
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let (k, _) = entry.ok()?;
                let key = k.value();
                if !key.starts_with(prefix.as_slice()) {
                    return None;
                }
                let snapshot_hlc = hlc_from_key(key, prefix.len())?;
                if snapshot_hlc < older_than {
                    Some(key.to_vec())
                } else {
                    None
                }
            })
            .collect();

        for key in keys_to_delete {
            if let Err(e) = table.remove(key.as_slice()) {
                warn!(array = %array, error = %e, "snapshot_store: delete_older_than remove error");
            }
        }
        drop(table);
        let _ = txn.commit();
    }
}

impl SnapshotSink for OriginSnapshotStore {
    fn write_snapshot(&self, snapshot: &TileSnapshot) -> nodedb_array::error::ArrayResult<()> {
        self.put(snapshot)
            .map_err(|e| nodedb_array::error::ArrayError::SegmentCorruption {
                detail: format!("OriginSnapshotStore::write_snapshot: {e}"),
            })
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

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
        let snap = snap("arr", 100);
        s.put(&snap).unwrap();
        let loaded = s.get("arr", hlc(100)).unwrap();
        assert_eq!(loaded.snapshot_hlc, snap.snapshot_hlc);
        assert_eq!(loaded.tile_blob, snap.tile_blob);
    }

    #[test]
    fn get_missing_returns_none() {
        let s = store();
        assert!(s.get("arr", hlc(999)).is_none());
    }

    #[test]
    fn latest_returns_highest_hlc() {
        let s = store();
        s.put(&snap("arr", 50)).unwrap();
        s.put(&snap("arr", 100)).unwrap();
        s.put(&snap("arr", 75)).unwrap();
        let latest = s.latest_for_array("arr").unwrap();
        assert_eq!(latest.snapshot_hlc, hlc(100));
    }

    #[test]
    fn delete_older_than_removes_old() {
        let s = store();
        s.put(&snap("arr", 10)).unwrap();
        s.put(&snap("arr", 20)).unwrap();
        s.put(&snap("arr", 30)).unwrap();
        s.delete_older_than("arr", hlc(25));
        assert!(s.get("arr", hlc(10)).is_none());
        assert!(s.get("arr", hlc(20)).is_none());
        assert!(s.get("arr", hlc(30)).is_some());
    }

    #[test]
    fn sink_impl_works() {
        let s = store();
        let snap = snap("arr", 200);
        SnapshotSink::write_snapshot(s.as_ref(), &snap).unwrap();
        assert!(s.get("arr", hlc(200)).is_some());
    }
}
