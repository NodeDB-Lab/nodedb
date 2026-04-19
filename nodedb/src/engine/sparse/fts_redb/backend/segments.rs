//! LSM segment blobs against `SEGMENTS`
//! keyed by `(tenant_id, collection, segment_id)`.

use super::core::RedbFtsBackend;
use super::shared::{MAX_SUBKEY, redb_err};
use crate::engine::sparse::fts_redb::tables::SEGMENTS;

pub(super) fn write(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
    segment_id: &str,
    data: &[u8],
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(SEGMENTS)
            .map_err(|e| redb_err("open segments", e))?;
        table
            .insert((tid, collection, segment_id), data)
            .map_err(|e| redb_err("insert segment", e))?;
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}

pub(super) fn read(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
    segment_id: &str,
) -> crate::Result<Option<Vec<u8>>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(SEGMENTS)
        .map_err(|e| redb_err("open segments", e))?;
    match table.get((tid, collection, segment_id)) {
        Ok(Some(val)) => Ok(Some(val.value().to_vec())),
        Ok(None) => Ok(None),
        Err(e) => Err(redb_err("get segment", e)),
    }
}

pub(super) fn list(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
) -> crate::Result<Vec<String>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(SEGMENTS)
        .map_err(|e| redb_err("open segments", e))?;
    let ids: Vec<String> = table
        .range((tid, collection, "")..=(tid, collection, MAX_SUBKEY))
        .map_err(|e| redb_err("range", e))?
        .filter_map(|r| r.ok().map(|(k, _)| k.value().2.to_string()))
        .collect();
    Ok(ids)
}

pub(super) fn remove(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
    segment_id: &str,
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(SEGMENTS)
            .map_err(|e| redb_err("open segments", e))?;
        let _ = table.remove((tid, collection, segment_id));
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}
