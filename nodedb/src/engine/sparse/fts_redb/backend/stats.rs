//! Corpus statistics against the dedicated `STATS` table
//! keyed by `(tenant_id, collection)`.

use redb::ReadableTable;

use super::core::RedbFtsBackend;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::tables::STATS;

pub(super) fn read(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
) -> crate::Result<(u32, u64)> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(STATS)
        .map_err(|e| redb_err("open stats", e))?;
    match table.get((tid, collection)) {
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
    tid: u32,
    collection: &str,
    doc_len: u32,
) -> crate::Result<()> {
    bump(backend, tid, collection, doc_len as i64)
}

pub(super) fn decrement(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
    doc_len: u32,
) -> crate::Result<()> {
    bump(backend, tid, collection, -(doc_len as i64))
}

fn bump(backend: &RedbFtsBackend, tid: u32, collection: &str, delta: i64) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(STATS)
            .map_err(|e| redb_err("open stats", e))?;
        let (mut count, mut total) = table
            .get((tid, collection))
            .ok()
            .flatten()
            .and_then(|v| zerompk::from_msgpack::<(u32, u64)>(v.value()).ok())
            .unwrap_or((0, 0));
        if delta > 0 {
            count += 1;
            total += delta as u64;
        } else {
            count = count.saturating_sub(1);
            total = total.saturating_sub((-delta) as u64);
        }
        let bytes =
            zerompk::to_msgpack_vec(&(count, total)).map_err(|e| redb_err("serialize stats", e))?;
        table
            .insert((tid, collection), bytes.as_slice())
            .map_err(|e| redb_err("insert stats", e))?;
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}
