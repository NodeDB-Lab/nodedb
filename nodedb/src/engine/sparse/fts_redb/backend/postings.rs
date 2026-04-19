//! Posting-list operations against the `POSTINGS` table
//! keyed by `(tenant_id, collection, term)`.

use nodedb_fts::posting::Posting;

use super::core::RedbFtsBackend;
use super::shared::{MAX_SUBKEY, redb_err};
use crate::engine::sparse::fts_redb::tables::POSTINGS;

pub(super) fn read(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
    term: &str,
) -> crate::Result<Vec<Posting>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(POSTINGS)
        .map_err(|e| redb_err("open postings", e))?;
    match table.get((tid, collection, term)) {
        Ok(Some(val)) => {
            let list: Vec<Posting> = zerompk::from_msgpack(val.value())
                .map_err(|e| redb_err("deserialize postings", e))?;
            Ok(list)
        }
        Ok(None) => Ok(Vec::new()),
        Err(e) => Err(redb_err("get postings", e)),
    }
}

pub(super) fn write(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
    term: &str,
    postings: &[Posting],
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(POSTINGS)
            .map_err(|e| redb_err("open postings", e))?;
        if postings.is_empty() {
            let _ = table.remove((tid, collection, term));
        } else {
            let bytes = zerompk::to_msgpack_vec(&postings.to_vec())
                .map_err(|e| redb_err("serialize postings", e))?;
            table
                .insert((tid, collection, term), bytes.as_slice())
                .map_err(|e| redb_err("insert posting", e))?;
        }
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}

pub(super) fn remove(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
    term: &str,
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(POSTINGS)
            .map_err(|e| redb_err("open postings", e))?;
        let _ = table.remove((tid, collection, term));
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}

pub(super) fn collection_terms(
    backend: &RedbFtsBackend,
    tid: u32,
    collection: &str,
) -> crate::Result<Vec<String>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(POSTINGS)
        .map_err(|e| redb_err("open postings", e))?;

    let terms: Vec<String> = table
        .range((tid, collection, "")..=(tid, collection, MAX_SUBKEY))
        .map_err(|e| redb_err("range", e))?
        .filter_map(|r| r.ok().map(|(k, _)| k.value().2.to_string()))
        .collect();
    Ok(terms)
}
