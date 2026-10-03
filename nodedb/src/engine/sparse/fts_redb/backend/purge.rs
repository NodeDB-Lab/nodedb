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
use crate::engine::sparse::fts_redb::keys::{
    KeyOwner, doc_fields_key, doc_key, posting_key, stats_key,
};
use crate::engine::sparse::fts_redb::scan::{self, DocKeyDef, StrKeyDef};
use crate::engine::sparse::fts_redb::tables::{
    DOC_FIELDS, DOC_LENGTHS, DOC_TERMS, INDEX_META, POSTINGS, SEGMENTS, STATS,
};

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
    purge(
        backend,
        database_id,
        tid,
        KeyOwner::collection(database_id, tid, coll),
        MetaRows::All,
    )
}

/// Drop every indexed row of one collection, keeping its configuration.
pub(super) fn collection_data(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    coll: &str,
) -> crate::Result<usize> {
    purge(
        backend,
        database_id,
        tid,
        KeyOwner::collection(database_id, tid, coll),
        MetaRows::KeepConfig,
    )
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
/// dropped per `meta`. Returns the number of posting lists, document-length
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

    removed += drop_str_rows(&write_txn, POSTINGS, database_id, tid, owner, "postings")?;
    // DOC_LENGTHS and DOC_TERMS are per-document rows of the same indexes and
    // must go together: a surviving term set would name posting lists that no
    // longer exist.
    removed += drop_doc_rows(
        &write_txn,
        DOC_LENGTHS,
        database_id,
        tid,
        owner,
        "doc_lengths",
    )?;
    drop_doc_rows(&write_txn, DOC_TERMS, database_id, tid, owner, "doc_terms")?;
    drop_meta_rows(&write_txn, database_id, tid, owner, meta)?;
    removed += drop_str_rows(&write_txn, SEGMENTS, database_id, tid, owner, "segments")?;

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
            fields
                .remove(doc_fields_key(database_id, tid, c, Surrogate::new(*s)))
                .map_err(|e| redb_err("remove doc_fields", e))?;
        }
    }

    write_txn
        .commit()
        .map_err(|e| redb_err("commit purge", e))?;
    Ok(removed)
}

/// Delete every row `owner` covers from a POSTINGS-shaped table.
fn drop_str_rows(
    txn: &WriteTransaction,
    def: TableDefinition<'static, StrKeyDef, &'static [u8]>,
    database_id: u64,
    tid: u64,
    owner: KeyOwner<'_>,
    name: &str,
) -> crate::Result<usize> {
    let mut table = txn
        .open_table(def)
        .map_err(|e| redb_err(&format!("open {name}"), e))?;
    let keys = scan::str_keys(&table, owner).map_err(|e| redb_err(&format!("{name} range"), e))?;
    for (c, f, s) in &keys {
        table
            .remove(posting_key(database_id, tid, IndexScope::from_key(c, f), s))
            .map_err(|e| redb_err(&format!("remove {name}"), e))?;
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
            .remove(posting_key(database_id, tid, IndexScope::from_key(c, f), s))
            .map_err(|e| redb_err("remove index_meta", e))?;
    }
    Ok(())
}

/// Delete every row `owner` covers from a DOC_LENGTHS-shaped table.
fn drop_doc_rows(
    txn: &WriteTransaction,
    def: TableDefinition<'static, DocKeyDef, &'static [u8]>,
    database_id: u64,
    tid: u64,
    owner: KeyOwner<'_>,
    name: &str,
) -> crate::Result<usize> {
    let mut table = txn
        .open_table(def)
        .map_err(|e| redb_err(&format!("open {name}"), e))?;
    let keys = scan::doc_keys(&table, owner).map_err(|e| redb_err(&format!("{name} range"), e))?;
    for (c, f, s) in &keys {
        table
            .remove(doc_key(
                database_id,
                tid,
                IndexScope::from_key(c, f),
                Surrogate::new(*s),
            ))
            .map_err(|e| redb_err(&format!("remove {name}"), e))?;
    }
    Ok(keys.len())
}
