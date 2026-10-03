// SPDX-License-Identifier: BUSL-1.1

//! Posting-list operations against the `POSTINGS` table
//! keyed by `(database_id, tenant_id, collection, field, term)`.

use nodedb_fts::IndexScope;
use nodedb_fts::posting::Posting;
use redb::{ReadableDatabase, ReadableTable};

use super::core::RedbFtsBackend;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::keys::{KeyOwner, posting_key};
use crate::engine::sparse::fts_redb::scan;
use crate::engine::sparse::fts_redb::tables::POSTINGS;

pub(super) fn read(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
) -> crate::Result<Vec<Posting>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(POSTINGS)
        .map_err(|e| redb_err("open postings", e))?;
    match table.get(posting_key(database_id, tid, index, term)) {
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
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
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
        let key = posting_key(database_id, tid, index, term);
        if postings.is_empty() {
            table
                .remove(key)
                .map_err(|e| redb_err("remove posting", e))?;
        } else {
            let bytes = zerompk::to_msgpack_vec(&postings.to_vec())
                .map_err(|e| redb_err("serialize postings", e))?;
            table
                .insert(key, bytes.as_slice())
                .map_err(|e| redb_err("insert posting", e))?;
        }
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}

pub(super) fn remove(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
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
        table
            .remove(posting_key(database_id, tid, index, term))
            .map_err(|e| redb_err("remove posting", e))?;
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}

pub(super) fn collection_terms(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
) -> crate::Result<Vec<String>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(POSTINGS)
        .map_err(|e| redb_err("open postings", e))?;
    let keys = scan::str_keys(&table, KeyOwner::index(database_id, tid, index))
        .map_err(|e| redb_err("postings range", e))?;
    Ok(keys.into_iter().map(|(_, _, term)| term).collect())
}
