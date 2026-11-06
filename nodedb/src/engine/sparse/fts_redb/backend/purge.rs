// SPDX-License-Identifier: BUSL-1.1

//! Structural collection and tenant drops.
//!
//! Each table is scanned from the owner's lower bound and every owned entry
//! is removed: every index of the collection (whole-document and field),
//! plus the per-document field sets. No lexical-prefix scans appear here.
//! Each drop runs in a single write transaction so the teardown is atomic
//! across tables.
//!
//! A tenant purge is scoped to a single `(database_id, tenant_id)` pair —
//! the same tenant in a different database is unaffected.

use nodedb_fts::IndexScope;
use nodedb_types::Surrogate;
use redb::{TableDefinition, WriteTransaction};

use super::core::RedbFtsBackend;
use super::shared::redb_err;
use crate::engine::sparse::btree::storage_bytes::SPARSE_COUNTER;
use crate::engine::sparse::fts_redb::bytes::{
    posting_key_len, remove_doc_fields_row, remove_doc_row, remove_segment_row,
};
use crate::engine::sparse::fts_redb::keys::{
    KeyOwner, doc_fields_key, doc_key, meta_key, posting_key, segment_key, stats_key,
};
use crate::engine::sparse::fts_redb::scan::{self, DocKeyDef};
use crate::engine::sparse::fts_redb::tables::{
    DOC_FIELDS, DOC_LENGTHS, DOC_TERMS, INDEX_META, POSTINGS, SEGMENTS, STATS,
};
use crate::engine::storage_bytes::StorageDelta;
use crate::storage::quarantine::{CollectionScope, QuarantineEngine};

/// Index metadata sub-keys that configure a collection rather than hold its
/// indexed data: they survive a data-only purge.
const CONFIG_META_KEYS: [&str; 3] = ["analyzer", "language", "fuzzy"];

/// Which metadata rows a purge drops.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MetaRows {
    /// Every metadata row: the collection is gone.
    All,
    /// Data rows only. The collection's analyzer, language, and fuzzy
    /// configuration stay: the collection is emptied, not dropped.
    KeepConfig,
}

pub(super) fn collection(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    coll: &str,
) -> crate::Result<usize> {
    let removed = purge(
        backend,
        database_id,
        tid,
        KeyOwner::collection(database_id, tid, coll),
        MetaRows::All,
    )?;
    release_collection_segments(backend, database_id, tid, coll);
    Ok(removed)
}

/// Forget the quarantine records of the segments a collection purge removed.
fn release_collection_segments(backend: &RedbFtsBackend, database_id: u64, tid: u64, coll: &str) {
    if let Some(registry) = &backend.quarantine_registry {
        registry.release_collection_engine(
            QuarantineEngine::Fts,
            &CollectionScope::new(database_id, tid, coll),
        );
    }
}

/// Drop every indexed row of one collection, keeping its configuration.
pub(super) fn collection_data(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    coll: &str,
) -> crate::Result<usize> {
    let removed = purge(
        backend,
        database_id,
        tid,
        KeyOwner::collection(database_id, tid, coll),
        MetaRows::KeepConfig,
    )?;
    release_collection_segments(backend, database_id, tid, coll);
    Ok(removed)
}

pub(super) fn tenant(backend: &RedbFtsBackend, database_id: u64, tid: u64) -> crate::Result<usize> {
    purge(
        backend,
        database_id,
        tid,
        KeyOwner::tenant(database_id, tid),
        MetaRows::All,
    )
}

/// Drop every row `owner` covers from every FTS table, with metadata rows
/// dropped per `meta`. Returns the number of posting rows, document-length
/// rows, and segments removed.
fn purge(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    owner: KeyOwner<'_>,
    meta: MetaRows,
) -> crate::Result<usize> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("purge write txn", e))?;
    let mut removed = 0;
    let mut delta = StorageDelta::default();

    removed += drop_posting_rows(&write_txn, owner, &mut delta)?;
    // DOC_LENGTHS and DOC_TERMS are per-document rows of the same indexes and
    // must go together: a surviving term set would name posting lists that no
    // longer exist.
    removed += drop_doc_rows(&write_txn, DOC_LENGTHS, owner, "doc_lengths", &mut delta)?;
    drop_doc_rows(&write_txn, DOC_TERMS, owner, "doc_terms", &mut delta)?;
    drop_meta_rows(&write_txn, database_id, tid, owner, meta)?;
    removed += drop_segment_rows(&write_txn, owner, &mut delta)?;
    super::history_rows::drop_owned(&write_txn, owner, &mut delta)?;

    {
        let mut stats = write_txn
            .open_table(STATS)
            .map_err(|e| redb_err("open stats", e))?;
        let keys = scan::stats_keys(&stats, owner).map_err(|e| redb_err("stats range", e))?;
        for (c, f) in &keys {
            stats
                .remove(stats_key(database_id, tid, IndexScope::from_key(c, f)))
                .map_err(|e| redb_err("remove stats", e))?;
        }
    }
    {
        let mut fields = write_txn
            .open_table(DOC_FIELDS)
            .map_err(|e| redb_err("open doc_fields", e))?;
        let keys =
            scan::doc_fields_keys(&fields, owner).map_err(|e| redb_err("doc_fields range", e))?;
        for (c, s) in &keys {
            remove_doc_fields_row(
                &mut fields,
                doc_fields_key(database_id, tid, c, Surrogate::new(*s)),
                &mut delta,
            )
            .map_err(|e| redb_err("remove doc_fields", e))?;
        }
    }
    delta.apply(&write_txn, SPARSE_COUNTER)?;

    write_txn
        .commit()
        .map_err(|e| redb_err("commit purge", e))?;
    Ok(removed)
}

/// Delete every POSTINGS row `owner` covers, recording their bytes in
/// `delta`.
fn drop_posting_rows(
    txn: &WriteTransaction,
    owner: KeyOwner<'_>,
    delta: &mut StorageDelta,
) -> crate::Result<usize> {
    let (database_id, tid) = (owner.database_id(), owner.tid());
    let mut table = txn
        .open_table(POSTINGS)
        .map_err(|e| redb_err("open postings", e))?;
    let keys = scan::posting_keys(&table, owner).map_err(|e| redb_err("postings range", e))?;
    for (c, f, term, s) in &keys {
        let key = posting_key(
            database_id,
            tid,
            IndexScope::from_key(c, f),
            term,
            Surrogate::new(*s),
        );
        let removed = table
            .remove(key)
            .map_err(|e| redb_err("remove postings", e))?
            .map(|prior| prior.value().len());
        if let Some(len) = removed {
            delta.removed(database_id, tid, c, posting_key_len(&key), len);
        }
    }
    Ok(keys.len())
}

/// Delete every SEGMENTS row `owner` covers, recording their bytes in
/// `delta`.
fn drop_segment_rows(
    txn: &WriteTransaction,
    owner: KeyOwner<'_>,
    delta: &mut StorageDelta,
) -> crate::Result<usize> {
    let (database_id, tid) = (owner.database_id(), owner.tid());
    let mut table = txn
        .open_table(SEGMENTS)
        .map_err(|e| redb_err("open segments", e))?;
    let keys = scan::str_keys(&table, owner).map_err(|e| redb_err("segments range", e))?;
    for (c, f, s) in &keys {
        remove_segment_row(
            &mut table,
            segment_key(database_id, tid, IndexScope::from_key(c, f), s),
            delta,
        )
        .map_err(|e| redb_err("remove segments", e))?;
    }
    Ok(keys.len())
}

/// Delete the INDEX_META rows `owner` covers that `meta` drops.
fn drop_meta_rows(
    txn: &WriteTransaction,
    database_id: u64,
    tid: u64,
    owner: KeyOwner<'_>,
    meta: MetaRows,
) -> crate::Result<()> {
    let mut table = txn
        .open_table(INDEX_META)
        .map_err(|e| redb_err("open index_meta", e))?;
    let keys = scan::str_keys(&table, owner).map_err(|e| redb_err("index_meta range", e))?;
    for (c, f, s) in &keys {
        if meta == MetaRows::KeepConfig && CONFIG_META_KEYS.contains(&s.as_str()) {
            continue;
        }
        table
            .remove(meta_key(database_id, tid, IndexScope::from_key(c, f), s))
            .map_err(|e| redb_err("remove index_meta", e))?;
    }
    Ok(())
}

/// Delete every row `owner` covers from a DOC_LENGTHS-shaped table,
/// recording their bytes in `delta`.
fn drop_doc_rows(
    txn: &WriteTransaction,
    def: TableDefinition<'static, DocKeyDef, &'static [u8]>,
    owner: KeyOwner<'_>,
    name: &str,
    delta: &mut StorageDelta,
) -> crate::Result<usize> {
    let (database_id, tid) = (owner.database_id(), owner.tid());
    let mut table = txn
        .open_table(def)
        .map_err(|e| redb_err(&format!("open {name}"), e))?;
    let keys = scan::doc_keys(&table, owner).map_err(|e| redb_err(&format!("{name} range"), e))?;
    for (c, f, s) in &keys {
        let key = doc_key(
            database_id,
            tid,
            IndexScope::from_key(c, f),
            Surrogate::new(*s),
        );
        remove_doc_row(&mut table, key, delta)
            .map_err(|e| redb_err(&format!("remove {name}"), e))?;
    }
    Ok(keys.len())
}
