//! [`OriginOpLog`] — redb-backed op-log for array CRDT sync on Origin.
//!
//! Each op is stored in a single redb table under the composite key:
//!
//! ```text
//! [name_len: u8][name: …bytes][hlc_bytes: 18]
//! ```
//!
//! This matches the key layout used by NodeDB-Lite's `RedbOpLog` so the
//! on-disk format is consistent and future migrations are mechanical.
//!
//! # Thread safety
//!
//! `OriginOpLog` is `Send + Sync` and is designed to be wrapped in an
//! `Arc` and shared across control-plane tasks.

use std::path::Path;
use std::sync::Arc;

use nodedb_array::error::{ArrayError, ArrayResult};
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_array::sync::op_log::{OpIter, OpLog};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use tracing::warn;

/// redb table: composite key bytes → msgpack-encoded `ArrayOp`.
const ARRAY_OP_LOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("array_op_log");

/// Build the composite key prefix: `[name_len: u8][name: bytes]`.
fn name_prefix(array: &str) -> Option<Vec<u8>> {
    let name_bytes = array.as_bytes();
    let len = u8::try_from(name_bytes.len()).ok()?;
    let mut key = Vec::with_capacity(1 + name_bytes.len());
    key.push(len);
    key.extend_from_slice(name_bytes);
    Some(key)
}

/// Build the composite op key: `[name_len: u8][name: bytes][hlc: 18]`.
fn op_key(array: &str, hlc: Hlc) -> Option<Vec<u8>> {
    let mut key = name_prefix(array)?;
    key.extend_from_slice(&hlc.to_bytes());
    Some(key)
}

/// Parse the HLC from the tail of a composite key.
///
/// Returns `None` if the key is shorter than `prefix_len + 18`.
fn hlc_from_key(key: &[u8], prefix_len: usize) -> Option<Hlc> {
    if key.len() < prefix_len + 18 {
        return None;
    }
    let start = key.len() - 18;
    let hlc_bytes: [u8; 18] = key[start..].try_into().ok()?;
    Some(Hlc::from_bytes(&hlc_bytes))
}

/// Persistent array op-log backed by a dedicated redb database.
///
/// All methods take synchronous redb write/read transactions. Callers on
/// the Control Plane (Tokio tasks) should call these from `spawn_blocking`
/// when latency matters; for inbound sync handling the blocking cost is
/// acceptable.
pub struct OriginOpLog {
    db: Arc<Database>,
}

impl OriginOpLog {
    /// Open or create the op-log database at `{data_dir}/array_sync/op_log.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("array_sync");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;
        let path = dir.join("op_log.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("open op_log db {}: {e}", path.display()),
        })?;
        // Ensure the table exists.
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("begin_write init: {e}"),
            })?;
            txn.open_table(ARRAY_OP_LOG)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("open_table init: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("commit init: {e}"),
            })?;
        }
        Ok(Self { db: Arc::new(db) })
    }

    /// Create an in-memory op-log (for tests and development setups).
    pub fn open_in_memory() -> crate::Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("in-memory db: {e}"),
            })?;
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("begin_write init: {e}"),
            })?;
            txn.open_table(ARRAY_OP_LOG)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("open_table init: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("commit init: {e}"),
            })?;
        }
        Ok(Self { db: Arc::new(db) })
    }
}

impl OpLog for OriginOpLog {
    fn append(&self, op: &ArrayOp) -> ArrayResult<()> {
        let key = op_key(&op.header.array, op.header.hlc).ok_or_else(|| ArrayError::InvalidOp {
            detail: format!("array name too long (>255 bytes): '{}'", op.header.array),
        })?;
        let encoded = op_codec::encode_op(op).map_err(|e| ArrayError::InvalidOp {
            detail: format!("op_log append encode: {e}"),
        })?;

        let txn = self.db.begin_write().map_err(|e| ArrayError::InvalidOp {
            detail: format!("op_log append begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(ARRAY_OP_LOG)
                .map_err(|e| ArrayError::InvalidOp {
                    detail: format!("op_log append open_table: {e}"),
                })?;
            // Idempotent: insert only if the key is absent.
            if table
                .get(key.as_slice())
                .map_err(|e| ArrayError::InvalidOp {
                    detail: format!("op_log append get: {e}"),
                })?
                .is_none()
            {
                table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|e| ArrayError::InvalidOp {
                        detail: format!("op_log append insert: {e}"),
                    })?;
            }
        }
        txn.commit().map_err(|e| ArrayError::InvalidOp {
            detail: format!("op_log append commit: {e}"),
        })?;
        Ok(())
    }

    fn scan_from<'a>(&'a self, from: Hlc) -> ArrayResult<OpIter<'a>> {
        let txn = self.db.begin_read().map_err(|e| ArrayError::InvalidOp {
            detail: format!("scan_from begin_read: {e}"),
        })?;
        let table = txn
            .open_table(ARRAY_OP_LOG)
            .map_err(|e| ArrayError::InvalidOp {
                detail: format!("scan_from open_table: {e}"),
            })?;

        let mut results: Vec<ArrayOp> = Vec::new();
        let range = table.iter().map_err(|e| ArrayError::InvalidOp {
            detail: format!("scan_from iter: {e}"),
        })?;
        for entry in range {
            let (k, v) = entry.map_err(|e| ArrayError::InvalidOp {
                detail: format!("scan_from entry: {e}"),
            })?;
            let key = k.value();
            // Any key shorter than 19 (1 len + at least 0 name + 18 hlc) is malformed.
            if key.len() < 19 {
                continue;
            }
            if let Some(hlc) = hlc_from_key(key, 0)
                && hlc >= from
            {
                match op_codec::decode_op(v.value()) {
                    Ok(op) => results.push(op),
                    Err(e) => warn!(error = %e, "scan_from: skipping corrupt entry"),
                }
            }
        }
        Ok(Box::new(results.into_iter().map(Ok)))
    }

    fn scan_range<'a>(&'a self, array: &str, from: Hlc, to: Hlc) -> ArrayResult<OpIter<'a>> {
        let prefix = name_prefix(array).ok_or_else(|| ArrayError::InvalidOp {
            detail: format!("array name too long: '{array}'"),
        })?;

        let txn = self.db.begin_read().map_err(|e| ArrayError::InvalidOp {
            detail: format!("scan_range begin_read '{array}': {e}"),
        })?;
        let table = txn
            .open_table(ARRAY_OP_LOG)
            .map_err(|e| ArrayError::InvalidOp {
                detail: format!("scan_range open_table '{array}': {e}"),
            })?;

        let mut results: Vec<ArrayOp> = Vec::new();
        let range = table.iter().map_err(|e| ArrayError::InvalidOp {
            detail: format!("scan_range iter '{array}': {e}"),
        })?;
        for entry in range {
            let (k, v) = entry.map_err(|e| ArrayError::InvalidOp {
                detail: format!("scan_range entry '{array}': {e}"),
            })?;
            let key = k.value();
            if !key.starts_with(prefix.as_slice()) {
                continue;
            }
            if let Some(hlc) = hlc_from_key(key, prefix.len())
                && hlc >= from
                && hlc <= to
            {
                match op_codec::decode_op(v.value()) {
                    Ok(op) => results.push(op),
                    Err(e) => warn!(error = %e, "scan_range: skipping corrupt entry for '{array}'"),
                }
            }
        }
        Ok(Box::new(results.into_iter().map(Ok)))
    }

    fn len(&self) -> ArrayResult<u64> {
        let txn = self.db.begin_read().map_err(|e| ArrayError::InvalidOp {
            detail: format!("op_log len begin_read: {e}"),
        })?;
        let table = txn
            .open_table(ARRAY_OP_LOG)
            .map_err(|e| ArrayError::InvalidOp {
                detail: format!("op_log len open_table: {e}"),
            })?;
        table.len().map_err(|e| ArrayError::InvalidOp {
            detail: format!("op_log len: {e}"),
        })
    }

    fn drop_below(&self, hlc: Hlc) -> ArrayResult<u64> {
        let txn = self.db.begin_write().map_err(|e| ArrayError::InvalidOp {
            detail: format!("drop_below begin_write: {e}"),
        })?;
        let mut dropped: u64 = 0;
        {
            let mut table = txn
                .open_table(ARRAY_OP_LOG)
                .map_err(|e| ArrayError::InvalidOp {
                    detail: format!("drop_below open_table: {e}"),
                })?;
            // Collect keys to delete (can't delete while iterating redb tables).
            let mut to_delete: Vec<Vec<u8>> = Vec::new();
            let iter = table.iter().map_err(|e| ArrayError::InvalidOp {
                detail: format!("drop_below iter: {e}"),
            })?;
            for entry in iter {
                let (k, _) = entry.map_err(|e| ArrayError::InvalidOp {
                    detail: format!("drop_below entry: {e}"),
                })?;
                let key = k.value();
                if key.len() < 19 {
                    continue;
                }
                if let Some(entry_hlc) = hlc_from_key(key, 0)
                    && entry_hlc < hlc
                {
                    to_delete.push(key.to_vec());
                }
            }
            for key in to_delete {
                table
                    .remove(key.as_slice())
                    .map_err(|e| ArrayError::InvalidOp {
                        detail: format!("drop_below remove: {e}"),
                    })?;
                dropped += 1;
            }
        }
        txn.commit().map_err(|e| ArrayError::InvalidOp {
            detail: format!("drop_below commit: {e}"),
        })?;
        Ok(dropped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::op::{ArrayOpHeader, ArrayOpKind};
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;

    fn replica() -> ReplicaId {
        ReplicaId::new(1)
    }

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, replica()).unwrap()
    }

    fn make_op(array: &str, ms: u64) -> ArrayOp {
        ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc: hlc(ms),
                schema_hlc: hlc(1),
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: ms as i64,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(ms as i64)],
            attrs: Some(vec![CellValue::Null]),
        }
    }

    #[test]
    fn append_and_len() {
        let log = OriginOpLog::open_in_memory().unwrap();
        assert_eq!(log.len().unwrap(), 0);
        log.append(&make_op("a", 10)).unwrap();
        log.append(&make_op("a", 20)).unwrap();
        assert_eq!(log.len().unwrap(), 2);
    }

    #[test]
    fn append_idempotent() {
        let log = OriginOpLog::open_in_memory().unwrap();
        log.append(&make_op("a", 10)).unwrap();
        log.append(&make_op("a", 10)).unwrap(); // duplicate
        assert_eq!(log.len().unwrap(), 1);
    }

    #[test]
    fn scan_from_filters_below() {
        let log = OriginOpLog::open_in_memory().unwrap();
        for ms in [10, 20, 30, 40] {
            log.append(&make_op("a", ms)).unwrap();
        }
        let ops: Vec<_> = log
            .scan_from(hlc(25))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().all(|op| op.header.hlc.physical_ms >= 25));
    }

    #[test]
    fn scan_range_filters_array_and_hlc() {
        let log = OriginOpLog::open_in_memory().unwrap();
        log.append(&make_op("a", 10)).unwrap();
        log.append(&make_op("b", 20)).unwrap();
        log.append(&make_op("a", 30)).unwrap();

        let ops: Vec<_> = log
            .scan_range("a", Hlc::ZERO, hlc(u64::MAX >> 16))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().all(|op| op.header.array == "a"));
    }

    #[test]
    fn drop_below_removes_old() {
        let log = OriginOpLog::open_in_memory().unwrap();
        for ms in [10, 20, 30] {
            log.append(&make_op("a", ms)).unwrap();
        }
        let dropped = log.drop_below(hlc(20)).unwrap();
        assert_eq!(dropped, 1);
        assert_eq!(log.len().unwrap(), 2);
    }
}
