// SPDX-License-Identifier: BUSL-1.1

//! Opaque metadata blobs (fieldnorms, analyzer, language, fuzzy) against
//! `INDEX_META` keyed by `(database_id, tenant_id, collection, field, subkey)`.

use super::core::RedbFtsBackend;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::keys::meta_key;
use crate::engine::sparse::fts_redb::tables::INDEX_META;
use nodedb_fts::IndexScope;
use redb::{ReadableDatabase, ReadableTable};

pub(super) fn read(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    subkey: &str,
) -> crate::Result<Option<Vec<u8>>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(INDEX_META)
        .map_err(|e| redb_err("open index_meta", e))?;
    match table.get(meta_key(database_id, tid, index, subkey)) {
        Ok(Some(val)) => Ok(Some(val.value().to_vec())),
        Ok(None) => Ok(None),
        Err(e) => Err(redb_err("get meta", e)),
    }
}

pub(super) fn write(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    subkey: &str,
    value: &[u8],
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(INDEX_META)
            .map_err(|e| redb_err("open index_meta", e))?;
        table
            .insert(meta_key(database_id, tid, index, subkey), value)
            .map_err(|e| redb_err("insert meta", e))?;
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}
