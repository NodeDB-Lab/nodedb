//! [`ArrayAckRegistry`] — per-replica ack HLC tracking for array GC.
//!
//! Each connected Lite peer periodically sends `ArrayAckMsg { array, replica_id,
//! ack_hlc_bytes }`. The registry persists the latest ack HLC per
//! `(array, replica_id)` pair and exposes `min_ack_hlc(array)` — the GC
//! frontier for that array (ops strictly below this HLC are safe to collapse).
//!
//! # Storage
//!
//! Backed by a dedicated redb table at `{data_dir}/array_sync/acks.redb`.
//! Each row: composite key `[name_len: u8][name bytes][replica_id: u64 BE]`
//! → `[hlc: 18 bytes]`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use nodedb_array::sync::ack::AckVector;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::replica_id::ReplicaId;
use redb::{Database, ReadableTable, TableDefinition};
use tracing::warn;

/// redb table: composite key bytes → 18-byte HLC.
const ACK_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("array_ack_hlcs");

/// Build the key: `[name_len: u8][name bytes][replica_id: u64 BE]`.
fn ack_key(array: &str, replica_id: u64) -> Option<Vec<u8>> {
    let name_bytes = array.as_bytes();
    let name_len = u8::try_from(name_bytes.len()).ok()?;
    let mut key = Vec::with_capacity(1 + name_bytes.len() + 8);
    key.push(name_len);
    key.extend_from_slice(name_bytes);
    key.extend_from_slice(&replica_id.to_be_bytes());
    Some(key)
}

/// Parse the array name from the head of a composite key.
///
/// Returns `None` if the key is malformed.
fn array_from_key(key: &[u8]) -> Option<String> {
    if key.is_empty() {
        return None;
    }
    let name_len = key[0] as usize;
    if key.len() < 1 + name_len + 8 {
        return None;
    }
    std::str::from_utf8(&key[1..1 + name_len])
        .ok()
        .map(|s| s.to_owned())
}

/// Parse the replica_id from the tail of a composite key.
fn replica_id_from_key(key: &[u8]) -> Option<u64> {
    if key.len() < 9 {
        return None;
    }
    let name_len = key[0] as usize;
    let replica_start = 1 + name_len;
    if key.len() < replica_start + 8 {
        return None;
    }
    let bytes: [u8; 8] = key[replica_start..replica_start + 8].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

/// Registry tracking the latest acknowledged HLC per `(array, replica_id)`.
///
/// Thread-safe via an internal `RwLock` over the in-memory map.
pub struct ArrayAckRegistry {
    db: Arc<Database>,
    /// In-memory cache: array name → per-replica AckVector.
    cache: std::sync::RwLock<HashMap<String, AckVector>>,
}

impl ArrayAckRegistry {
    /// Open or create the ack registry database at `{data_dir}/array_sync/acks.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Arc<Self>> {
        let dir = data_dir.join("array_sync");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;
        let path = dir.join("acks.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("open acks db {}: {e}", path.display()),
        })?;
        Self::init_db(db)
    }

    /// In-memory-only registry for tests.
    pub fn open_in_memory() -> crate::Result<Arc<Self>> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("in-memory acks db: {e}"),
            })?;
        Self::init_db(db)
    }

    fn init_db(db: Database) -> crate::Result<Arc<Self>> {
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry init begin_write: {e}"),
            })?;
            txn.open_table(ACK_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("ack_registry init open_table: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry init commit: {e}"),
            })?;
        }

        // Load all persisted rows into the in-memory cache.
        let db = Arc::new(db);
        let cache = Self::load_cache(&db)?;

        Ok(Arc::new(Self {
            db,
            cache: std::sync::RwLock::new(cache),
        }))
    }

    fn load_cache(db: &Database) -> crate::Result<HashMap<String, AckVector>> {
        let txn = db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("ack_registry load begin_read: {e}"),
        })?;
        let table = txn
            .open_table(ACK_TABLE)
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry load open_table: {e}"),
            })?;

        let mut cache: HashMap<String, AckVector> = HashMap::new();
        let iter = table.iter().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("ack_registry load iter: {e}"),
        })?;

        for entry in iter {
            let (k, v) = entry.map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("ack_registry load entry: {e}"),
            })?;
            let key = k.value();
            let val = v.value();

            let Some(array) = array_from_key(key) else {
                warn!("ack_registry: malformed key, skipping");
                continue;
            };
            let Some(replica_raw) = replica_id_from_key(key) else {
                warn!(array = %array, "ack_registry: cannot parse replica_id, skipping");
                continue;
            };
            if val.len() != 18 {
                warn!(array = %array, "ack_registry: ack hlc wrong length, skipping");
                continue;
            }
            let hlc_bytes: [u8; 18] = val.try_into().unwrap_or([0u8; 18]);
            let hlc = Hlc::from_bytes(&hlc_bytes);
            let replica_id = ReplicaId::new(replica_raw);
            cache.entry(array).or_default().record(replica_id, hlc);
        }

        Ok(cache)
    }

    /// Record an ack from `replica_id` for `array` at `ack_hlc`.
    ///
    /// Monotonically advances the stored value — older acks are ignored.
    /// Persists the updated HLC immediately.
    pub fn record(&self, array: &str, replica_id: ReplicaId, ack_hlc: Hlc) {
        {
            let mut cache = self.cache.write().unwrap_or_else(|p| p.into_inner());
            cache
                .entry(array.to_owned())
                .or_default()
                .record(replica_id, ack_hlc);
        }
        // Persist outside the write-lock to avoid holding it across I/O.
        if let Some(key) = ack_key(array, replica_id.as_u64())
            && let Err(e) = self.persist_row(&key, ack_hlc)
        {
            warn!(
                array = %array,
                error = %e,
                "ack_registry: persist failed — in-memory ack advanced but disk not updated"
            );
        }
    }

    fn persist_row(&self, key: &[u8], hlc: Hlc) -> crate::Result<()> {
        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("ack_registry persist begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(ACK_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("ack_registry persist open_table: {e}"),
                })?;
            let hlc_bytes = hlc.to_bytes();
            table
                .insert(key, hlc_bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("ack_registry persist insert: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("ack_registry persist commit: {e}"),
        })
    }

    /// Return the minimum ack HLC across all replicas for `array`.
    ///
    /// Returns `None` if no acks have been recorded for this array (GC must
    /// not proceed — it could discard ops that no replica has seen).
    pub fn min_ack_hlc(&self, array: &str) -> Option<Hlc> {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache.get(array)?.min_ack_hlc()
    }

    /// Return a snapshot of the full [`AckVector`] for `array`.
    ///
    /// Returns an empty `AckVector` if no acks have been recorded yet.
    pub fn ack_vector(&self, array: &str) -> AckVector {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache.get(array).cloned().unwrap_or_else(AckVector::new)
    }

    /// Return all distinct array names that have at least one recorded ack.
    pub fn known_arrays(&self) -> Vec<String> {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        cache.keys().cloned().collect()
    }

    /// Return all replica IDs that have acked `array`.
    pub fn all_replicas(&self, array: &str) -> Vec<ReplicaId> {
        let cache = self.cache.read().unwrap_or_else(|p| p.into_inner());
        if let Some(av) = cache.get(array) {
            av.replicas().collect()
        } else {
            Vec::new()
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(1)).unwrap()
    }

    fn r(n: u64) -> ReplicaId {
        ReplicaId::new(n)
    }

    fn registry() -> Arc<ArrayAckRegistry> {
        ArrayAckRegistry::open_in_memory().unwrap()
    }

    #[test]
    fn min_ack_none_when_no_records() {
        let reg = registry();
        assert!(reg.min_ack_hlc("arr").is_none());
    }

    #[test]
    fn single_replica_ack() {
        let reg = registry();
        reg.record("arr", r(1), hlc(100));
        assert_eq!(reg.min_ack_hlc("arr"), Some(hlc(100)));
    }

    #[test]
    fn two_replicas_min_is_lower() {
        let reg = registry();
        reg.record("arr", r(1), hlc(100));
        reg.record("arr", r(2), hlc(50));
        assert_eq!(reg.min_ack_hlc("arr"), Some(hlc(50)));
    }

    #[test]
    fn ack_is_monotonic() {
        let reg = registry();
        reg.record("arr", r(1), hlc(100));
        reg.record("arr", r(1), hlc(50)); // should not regress
        assert_eq!(reg.min_ack_hlc("arr"), Some(hlc(100)));
    }

    #[test]
    fn known_arrays() {
        let reg = registry();
        reg.record("a", r(1), hlc(10));
        reg.record("b", r(1), hlc(20));
        let mut arrays = reg.known_arrays();
        arrays.sort();
        assert_eq!(arrays, vec!["a", "b"]);
    }
}
