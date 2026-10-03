// SPDX-License-Identifier: BUSL-1.1

//! Per-document token-length operations against `DOC_LENGTHS`
//! keyed by `(database_id, tenant_id, collection, field, surrogate_u32)`.

use std::collections::HashSet;

use nodedb_fts::IndexScope;
use nodedb_types::Surrogate;
use redb::{ReadableDatabase, ReadableTable};

use super::core::RedbFtsBackend;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::keys::doc_key;
use crate::engine::sparse::fts_redb::tables::DOC_LENGTHS;

pub(super) fn read(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    doc_id: Surrogate,
) -> crate::Result<Option<u32>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(DOC_LENGTHS)
        .map_err(|e| redb_err("open doc_lengths", e))?;
    match table.get(doc_key(database_id, tid, index, doc_id)) {
        Ok(Some(val)) => {
            let len: u32 = zerompk::from_msgpack(val.value())
                .map_err(|e| redb_err("deserialize doc_length", e))?;
            Ok(Some(len))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(redb_err("get doc_length", e)),
    }
}

/// The lengths of `doc_ids` in one index, parallel to `doc_ids`, read in one
/// transaction.
pub(super) fn read_many(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    doc_ids: &[Surrogate],
) -> crate::Result<Vec<Option<u32>>> {
    if doc_ids.is_empty() {
        return Ok(Vec::new());
    }
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(DOC_LENGTHS)
        .map_err(|e| redb_err("open doc_lengths", e))?;
    let mut lengths = Vec::with_capacity(doc_ids.len());
    for doc_id in doc_ids {
        let length = match table
            .get(doc_key(database_id, tid, index, *doc_id))
            .map_err(|e| redb_err("get doc_length", e))?
        {
            Some(val) => Some(
                zerompk::from_msgpack::<u32>(val.value())
                    .map_err(|e| redb_err("deserialize doc_length", e))?,
            ),
            None => None,
        };
        lengths.push(length);
    }
    Ok(lengths)
}

/// Every surrogate one index records a length for, read in one transaction.
///
/// An index holds a document exactly when it records the document's length.
pub(super) fn members(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
) -> crate::Result<HashSet<Surrogate>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(DOC_LENGTHS)
        .map_err(|e| redb_err("open doc_lengths", e))?;
    let first = doc_key(database_id, tid, index, Surrogate::new(0));
    let last = doc_key(database_id, tid, index, Surrogate::new(u32::MAX));
    let mut members = HashSet::new();
    for entry in table
        .range(first..=last)
        .map_err(|e| redb_err("range doc_lengths", e))?
    {
        let (key, _) = entry.map_err(|e| redb_err("read doc_length", e))?;
        members.insert(Surrogate::new(key.value().4));
    }
    Ok(members)
}

pub(super) fn write(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    doc_id: Surrogate,
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
            .insert(doc_key(database_id, tid, index, doc_id), bytes.as_slice())
            .map_err(|e| redb_err("insert doc_len", e))?;
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}

pub(super) fn remove(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    doc_id: Surrogate,
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| redb_err("open doc_lengths", e))?;
        table
            .remove(doc_key(database_id, tid, index, doc_id))
            .map_err(|e| redb_err("remove doc_len", e))?;
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}
