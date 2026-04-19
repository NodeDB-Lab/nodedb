//! Per-document token-length operations against `DOC_LENGTHS`
//! keyed by `(tenant_id, collection, doc_id)`.

use super::core::RedbFtsBackend;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::tables::DOC_LENGTHS;

pub(super) fn read(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
    doc_id: &str,
) -> crate::Result<Option<u32>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(DOC_LENGTHS)
        .map_err(|e| redb_err("open doc_lengths", e))?;
    match table.get((tid, collection, doc_id)) {
        Ok(Some(val)) => {
            let len: u32 = zerompk::from_msgpack(val.value())
                .map_err(|e| redb_err("deserialize doc_length", e))?;
            Ok(Some(len))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(redb_err("get doc_length", e)),
    }
}

pub(super) fn write(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
    doc_id: &str,
    length: u32,
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| redb_err("open doc_lengths", e))?;
        let bytes =
            zerompk::to_msgpack_vec(&length).map_err(|e| redb_err("serialize doc_len", e))?;
        table
            .insert((tid, collection, doc_id), bytes.as_slice())
            .map_err(|e| redb_err("insert doc_len", e))?;
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}

pub(super) fn remove(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
    doc_id: &str,
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| redb_err("open doc_lengths", e))?;
        let _ = table.remove((tid, collection, doc_id));
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}
