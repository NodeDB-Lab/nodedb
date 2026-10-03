// SPDX-License-Identifier: BUSL-1.1

//! Corpus statistics against the dedicated `STATS` table
//! keyed by `(database_id, tenant_id, collection, field)`.

use nodedb_fts::IndexScope;
use redb::{ReadableDatabase, ReadableTable};

use super::core::RedbFtsBackend;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::keys::stats_key;
use crate::engine::sparse::fts_redb::tables::STATS;

pub(super) fn read(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
) -> crate::Result<(u32, u64)> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(STATS)
        .map_err(|e| redb_err("open stats", e))?;
    match table.get(stats_key(database_id, tid, index)) {
        Ok(Some(val)) => {
            let stats: (u32, u64) =
                zerompk::from_msgpack(val.value()).map_err(|e| redb_err("deserialize stats", e))?;
            Ok(stats)
        }
        Ok(None) => Ok((0, 0)),
        Err(e) => Err(redb_err("get stats", e)),
    }
}

pub(super) fn increment(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    doc_len: u32,
) -> crate::Result<()> {
    bump(backend, database_id, tid, index, 1, i64::from(doc_len))
}

pub(super) fn decrement(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    doc_len: u32,
) -> crate::Result<()> {
    bump(backend, database_id, tid, index, -1, -i64::from(doc_len))
}

/// Move one index's `(doc_count, total_token_sum)` by explicit deltas,
/// clamped at zero.
fn bump(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    count_delta: i64,
    total_delta: i64,
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(STATS)
            .map_err(|e| redb_err("open stats", e))?;
        let key = stats_key(database_id, tid, index);
        let (count, total) = match table.get(key).map_err(|e| redb_err("get stats", e))? {
            Some(v) => zerompk::from_msgpack::<(u32, u64)>(v.value())
                .map_err(|e| redb_err("deserialize stats", e))?,
            None => (0, 0),
        };
        let count = (i64::from(count) + count_delta).clamp(0, i64::from(u32::MAX)) as u32;
        let total = (total as i64).saturating_add(total_delta).max(0) as u64;
        let bytes =
            zerompk::to_msgpack_vec(&(count, total)).map_err(|e| redb_err("serialize stats", e))?;
        table
            .insert(key, bytes.as_slice())
            .map_err(|e| redb_err("insert stats", e))?;
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}
