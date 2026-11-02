// SPDX-License-Identifier: BUSL-1.1

//! [`OriginOpLog`] — redb-backed op-log for array CRDT sync on Origin.
//!
//! Entries live in `array_op_log`, keyed by
//! `[db: u64 BE][tenant: u64 BE][name_len: u16 BE][name][hlc: 18]`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use nodedb_array::error::{ArrayError, ArrayResult};
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_array::sync::op_log::{OpIter, OpLog};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::warn;

use crate::types::DatabaseId;

/// Key: `[db: u64 BE][tenant: u64 BE][name_len: u16 BE][name][hlc: 18]`.
const ARRAY_OP_LOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("array_op_log");

fn v2_prefix(database_id: DatabaseId, tenant_id: u64, array: &str) -> Option<Vec<u8>> {
    let name = array.as_bytes();
    let len = u16::try_from(name.len()).ok()?;
    let mut key = Vec::with_capacity(18 + name.len());
    key.extend_from_slice(&database_id.as_u64().to_be_bytes());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(name);
    Some(key)
}

fn v2_key(database_id: DatabaseId, tenant_id: u64, array: &str, hlc: Hlc) -> Option<Vec<u8>> {
    let mut key = v2_prefix(database_id, tenant_id, array)?;
    key.extend_from_slice(&hlc.to_bytes());
    Some(key)
}

fn v2_scope_from_key(key: &[u8]) -> Option<(DatabaseId, u64, String, Hlc)> {
    if key.len() < 36 {
        return None;
    }
    let database_id = DatabaseId::new(u64::from_be_bytes(key[..8].try_into().ok()?));
    let tenant_id = u64::from_be_bytes(key[8..16].try_into().ok()?);
    let name_len = u16::from_be_bytes(key[16..18].try_into().ok()?) as usize;
    let hlc_start = 18 + name_len;
    if key.len() != hlc_start + 18 {
        return None;
    }
    let array = std::str::from_utf8(&key[18..hlc_start]).ok()?.to_owned();
    let hlc = Hlc::from_bytes(&key[hlc_start..].try_into().ok()?);
    Some((database_id, tenant_id, array, hlc))
}

fn invalid(detail: impl std::fmt::Display) -> ArrayError {
    ArrayError::InvalidOp {
        detail: detail.to_string(),
    }
}

/// Persistent array op-log backed by a dedicated redb database.
pub struct OriginOpLog {
    db: Arc<Database>,
}

impl crate::storage::RedbBacked for OriginOpLog {
    fn redb_database(&self) -> &redb::Database {
        &self.db
    }
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
        Self::init(db)
    }

    /// Create an in-memory op-log (for tests and development setups).
    pub fn open_in_memory() -> crate::Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("in-memory db: {e}"),
            })?;
        Self::init(db)
    }

    fn init(db: Database) -> crate::Result<Self> {
        let txn = db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("begin_write init: {e}"),
        })?;
        txn.open_table(ARRAY_OP_LOG)
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("open V2 op-log table init: {e}"),
            })?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("commit init: {e}"),
        })?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Append an operation in an explicit database scope. Writes are
    /// idempotent: re-appending an existing key keeps the stored row.
    pub fn append_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        op: &ArrayOp,
    ) -> ArrayResult<()> {
        let key =
            v2_key(database_id, tenant_id, &op.header.array, op.header.hlc).ok_or_else(|| {
                invalid(format!(
                    "array name too long (>65535 bytes): '{}'",
                    op.header.array
                ))
            })?;
        let encoded =
            op_codec::encode_op(op).map_err(|e| invalid(format!("op_log append encode: {e}")))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| invalid(format!("op_log append begin_write: {e}")))?;
        {
            let mut table = txn
                .open_table(ARRAY_OP_LOG)
                .map_err(|e| invalid(format!("op_log append open table: {e}")))?;
            if table
                .get(key.as_slice())
                .map_err(|e| invalid(format!("op_log append get: {e}")))?
                .is_none()
            {
                table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|e| invalid(format!("op_log append insert: {e}")))?;
            }
        }
        txn.commit()
            .map_err(|e| invalid(format!("op_log append commit: {e}")))
    }

    /// Scan all scoped operations at or above `from`, in HLC order.
    pub fn scan_from_in_database<'a>(
        &'a self,
        database_id: DatabaseId,
        tenant_id: u64,
        from: Hlc,
    ) -> ArrayResult<OpIter<'a>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| invalid(format!("scan_from begin_read: {e}")))?;
        let table = txn
            .open_table(ARRAY_OP_LOG)
            .map_err(|e| invalid(format!("scan_from open table: {e}")))?;
        let mut results = BTreeMap::new();
        for entry in table
            .iter()
            .map_err(|e| invalid(format!("scan_from iter: {e}")))?
        {
            let (key, value) = entry.map_err(|e| invalid(format!("scan_from entry: {e}")))?;
            let Some((scope, entry_tenant_id, array, hlc)) = v2_scope_from_key(key.value()) else {
                continue;
            };
            if scope == database_id && entry_tenant_id == tenant_id && hlc >= from {
                match op_codec::decode_op(value.value()) {
                    Ok(op) => {
                        results.insert((hlc, array), op);
                    }
                    Err(e) => warn!(error = %e, "scan_from: skipping corrupt entry"),
                }
            }
        }
        Ok(Box::new(results.into_values().map(Ok)))
    }

    /// Scan an array in an explicit database scope, inclusive of both bounds.
    pub fn scan_range_in_database<'a>(
        &'a self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
        from: Hlc,
        to: Hlc,
    ) -> ArrayResult<OpIter<'a>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| invalid(format!("scan_range begin_read: {e}")))?;
        let table = txn
            .open_table(ARRAY_OP_LOG)
            .map_err(|e| invalid(format!("scan_range open table: {e}")))?;
        let mut results = BTreeMap::new();
        for entry in table
            .iter()
            .map_err(|e| invalid(format!("scan_range iter: {e}")))?
        {
            let (key, value) = entry.map_err(|e| invalid(format!("scan_range entry: {e}")))?;
            let Some((scope, entry_tenant_id, key_array, hlc)) = v2_scope_from_key(key.value())
            else {
                continue;
            };
            if scope == database_id
                && entry_tenant_id == tenant_id
                && key_array == array
                && hlc >= from
                && hlc <= to
            {
                match op_codec::decode_op(value.value()) {
                    Ok(op) => {
                        results.insert(hlc, op);
                    }
                    Err(e) => {
                        warn!(error = %e, array = %array, "scan_range: skipping corrupt entry")
                    }
                }
            }
        }
        Ok(Box::new(results.into_values().map(Ok)))
    }

    /// Return the logical number of operations in an explicit database scope.
    pub fn len_in_database(&self, database_id: DatabaseId, tenant_id: u64) -> ArrayResult<u64> {
        Ok(self
            .scan_from_in_database(database_id, tenant_id, Hlc::ZERO)?
            .count() as u64)
    }

    /// Drop all operations for one structurally scoped array below `hlc` and
    /// return physical rows removed.
    pub fn drop_array_below_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
        hlc: Hlc,
    ) -> ArrayResult<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| invalid(format!("drop_below begin_write: {e}")))?;
        let mut dropped = 0;
        {
            let mut table = txn
                .open_table(ARRAY_OP_LOG)
                .map_err(|e| invalid(format!("drop_below open table: {e}")))?;
            let keys: Vec<Vec<u8>> = table
                .iter()
                .map_err(|e| invalid(format!("drop_below iter: {e}")))?
                .filter_map(|entry| {
                    entry.ok().and_then(|(key, _)| {
                        let bytes = key.value();
                        v2_scope_from_key(bytes)
                            .filter(|(scope, entry_tenant_id, entry_array, entry_hlc)| {
                                *scope == database_id
                                    && *entry_tenant_id == tenant_id
                                    && entry_array == array
                                    && *entry_hlc < hlc
                            })
                            .map(|_| bytes.to_vec())
                    })
                })
                .collect();
            for key in keys {
                table
                    .remove(key.as_slice())
                    .map_err(|e| invalid(format!("drop_below remove: {e}")))?;
                dropped += 1;
            }
        }
        txn.commit()
            .map_err(|e| invalid(format!("drop_below commit: {e}")))?;
        Ok(dropped)
    }
}

impl OpLog for OriginOpLog {
    /// Explicit compatibility wrapper for the DEFAULT database.
    fn append(&self, op: &ArrayOp) -> ArrayResult<()> {
        self.append_in_database(DatabaseId::DEFAULT, 0, op)
    }
    /// Explicit compatibility wrapper for the DEFAULT database.
    fn scan_from<'a>(&'a self, from: Hlc) -> ArrayResult<OpIter<'a>> {
        self.scan_from_in_database(DatabaseId::DEFAULT, 0, from)
    }
    /// Explicit compatibility wrapper for the DEFAULT database.
    fn scan_range<'a>(&'a self, array: &str, from: Hlc, to: Hlc) -> ArrayResult<OpIter<'a>> {
        self.scan_range_in_database(DatabaseId::DEFAULT, 0, array, from, to)
    }
    /// Explicit compatibility wrapper for the DEFAULT database.
    fn len(&self) -> ArrayResult<u64> {
        self.len_in_database(DatabaseId::DEFAULT, 0)
    }
    /// Explicit compatibility wrapper for the DEFAULT database. The generic
    /// trait has no array parameter, so remove each known array independently.
    fn drop_below(&self, hlc: Hlc) -> ArrayResult<u64> {
        let arrays: std::collections::HashSet<String> = self
            .scan_from_in_database(DatabaseId::DEFAULT, 0, Hlc::ZERO)?
            .filter_map(Result::ok)
            .map(|op| op.header.array)
            .collect();
        arrays.into_iter().try_fold(0, |total, array| {
            self.drop_array_below_in_database(DatabaseId::DEFAULT, 0, &array, hlc)
                .map(|dropped| total + dropped)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::op::{ArrayOpHeader, ArrayOpKind};
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(1)).unwrap()
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
    fn ops(
        log: &OriginOpLog,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
    ) -> Vec<ArrayOp> {
        log.scan_range_in_database(
            database_id,
            tenant_id,
            array,
            Hlc::ZERO,
            hlc(u64::MAX >> 16),
        )
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    #[test]
    fn same_name_isolated_between_databases() {
        let log = OriginOpLog::open_in_memory().unwrap();
        let database_id = DatabaseId::new(7);
        log.append_in_database(database_id, 1, &make_op("arr", 10))
            .unwrap();
        log.append_in_database(database_id, 2, &make_op("arr", 20))
            .unwrap();
        assert_eq!(ops(&log, database_id, 1, "arr").len(), 1);
        assert_eq!(ops(&log, database_id, 2, "arr").len(), 1);
        assert_eq!(ops(&log, database_id, 2, "arr")[0].header.hlc, hlc(20));
    }

    #[test]
    fn array_scoped_gc_never_prunes_a_sibling_array() {
        let log = OriginOpLog::open_in_memory().unwrap();
        let database_id = DatabaseId::new(8);
        log.append_in_database(database_id, 4, &make_op("snapshotted", 10))
            .unwrap();
        log.append_in_database(database_id, 4, &make_op("unsnapshotted", 10))
            .unwrap();

        assert_eq!(
            log.drop_array_below_in_database(database_id, 4, "snapshotted", hlc(20))
                .unwrap(),
            1
        );
        assert!(ops(&log, database_id, 4, "snapshotted").is_empty());
        assert_eq!(ops(&log, database_id, 4, "unsnapshotted").len(), 1);
    }

    #[test]
    fn re_appending_one_key_keeps_the_stored_op() {
        let log = OriginOpLog::open_in_memory().unwrap();
        let first = make_op("arr", 10);
        log.append_in_database(DatabaseId::DEFAULT, 0, &first)
            .unwrap();
        let mut second = make_op("arr", 10);
        second.header.system_from_ms = 99;
        log.append_in_database(DatabaseId::DEFAULT, 0, &second)
            .unwrap();

        let stored = ops(&log, DatabaseId::DEFAULT, 0, "arr");
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].header.array, "arr");
        assert_eq!(stored[0].header.system_from_ms, first.header.system_from_ms);
    }

    #[test]
    fn persistence_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let database_id = DatabaseId::new(9);
        {
            let log = OriginOpLog::open(dir.path()).unwrap();
            log.append_in_database(database_id, 3, &make_op("persist", 10))
                .unwrap();
        }
        let log = OriginOpLog::open(dir.path()).unwrap();
        assert_eq!(ops(&log, database_id, 3, "persist").len(), 1);
    }
}
