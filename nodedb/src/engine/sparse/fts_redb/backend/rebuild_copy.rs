// SPDX-License-Identifier: BUSL-1.1

//! Row copies of one collection's full-text index between two databases.
//!
//! A rebuild derives a collection's index into a scratch database off the
//! core. The scratch database takes the collection's analyzer, language and
//! fuzzy rows from the live snapshot first, so it analyzes text as the live
//! index does. The cutover then copies the scratch rows into the live
//! database inside its own write transaction.
//!
//! The copy covers every table a document write fills: postings, document
//! lengths, term sets, field sets, and the system-time history. It skips
//! the rows of every surrogate in `skip`: the cutover replays those
//! documents from their live versions. `STATS` is not copied. The copy
//! counts it from the document lengths it copies, so a skipped document is
//! not counted twice when its replay adds it. A document write never fills
//! `SEGMENTS` or the non-config `INDEX_META` rows, so the scratch database
//! holds none.

use std::collections::BTreeMap;

use nodedb_fts::IndexScope;
use redb::{ReadTransaction, ReadableTable, WriteTransaction};
use roaring::RoaringBitmap;

use super::core::RedbFtsBackend;
use super::purge::CONFIG_META_KEYS;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::bytes::{
    ended_key_len, live_by_from_row_len, live_from_row_len, posting_key_len, put_doc_fields_row,
    put_doc_row,
};
use crate::engine::sparse::fts_redb::keys::{KeyOwner, stats_key};
use crate::engine::sparse::fts_redb::scan::DocKeyDef;
use crate::engine::sparse::fts_redb::tables::{
    DOC_FIELDS, DOC_LENGTHS, DOC_TERMS, INDEX_META, POSTINGS, STATS, TEXT_ENDED, TEXT_LIVE_BY_FROM,
    TEXT_LIVE_FROM,
};
use crate::engine::storage_bytes::StorageDelta;

/// One collection's rows to copy, and the surrogates to leave out.
#[derive(Clone, Copy)]
pub(crate) struct CollectionCopy<'a> {
    pub(crate) database_id: u64,
    pub(crate) tid: u64,
    pub(crate) collection: &'a str,
    pub(crate) skip: &'a RoaringBitmap,
    /// Runs once per copied row. Its error ends the copy.
    pub(crate) on_row: &'a dyn Fn() -> crate::Result<()>,
}

impl CollectionCopy<'_> {
    fn owner(&self) -> KeyOwner<'_> {
        KeyOwner::collection(self.database_id, self.tid, self.collection)
    }

    /// Whether the copy leaves the rows of `surrogate` out. Runs
    /// `on_row` for a row it copies.
    fn skips(&self, surrogate: u32) -> crate::Result<bool> {
        if self.skip.contains(surrogate) {
            return Ok(true);
        }
        (self.on_row)()?;
        Ok(false)
    }
}

/// `(documents, tokens)` per field key, counted from copied lengths.
type CountedStats = BTreeMap<String, (u32, u64)>;

impl RedbFtsBackend {
    /// Drop every indexed row of one collection inside `txn`, keeping its
    /// analyzer, language and fuzzy rows and its synced text. `delta`
    /// records the bytes the rows held. Call
    /// [`Self::release_collection_segments`] after `txn` commits.
    pub(crate) fn clear_collection_data_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tid: u64,
        collection: &str,
        delta: &mut StorageDelta,
    ) -> crate::Result<usize> {
        super::purge::collection_data_in_txn(
            txn,
            KeyOwner::collection(database_id, tid, collection),
            delta,
        )
    }

    /// Forget the quarantine records of a cleared collection's segments.
    pub(crate) fn release_collection_segments(&self, database_id: u64, tid: u64, collection: &str) {
        super::purge::release_segments(self, database_id, tid, collection);
    }
}

/// Copy the analyzer, language and fuzzy rows of one collection from `src`
/// into `dst`.
pub(crate) fn copy_config_meta(
    src: &ReadTransaction,
    dst: &WriteTransaction,
    database_id: u64,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    let owner = KeyOwner::collection(database_id, tid, collection);
    let from = src
        .open_table(INDEX_META)
        .map_err(|e| redb_err("copy open index_meta", e))?;
    let mut to = dst
        .open_table(INDEX_META)
        .map_err(|e| redb_err("copy open index_meta", e))?;
    for entry in from
        .range(owner.str_start()..)
        .map_err(|e| redb_err("copy index_meta range", e))?
    {
        let (key, value) = entry.map_err(|e| redb_err("copy read index_meta", e))?;
        let (d, t, c, f, sub) = key.value();
        if !owner.owns(d, t, c, Some(f)) {
            break;
        }
        if CONFIG_META_KEYS.contains(&sub) {
            to.insert((d, t, c, f, sub), value.value())
                .map_err(|e| redb_err("copy write index_meta", e))?;
        }
    }
    Ok(())
}

/// Copy every derived row `copy` covers from `src` into `dst`, then write
/// the collection's `STATS` rows counted from the copied document lengths.
/// `dst` holds no row of the collection: the caller cleared it. `delta`
/// records the bytes the rows add.
pub(crate) fn copy_collection_rows(
    src: &ReadTransaction,
    dst: &WriteTransaction,
    copy: CollectionCopy<'_>,
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    copy_postings(src, dst, copy, delta)?;
    let stats = copy_doc_rows(src, dst, DocTable::Lengths, copy, delta)?;
    copy_doc_rows(src, dst, DocTable::Terms, copy, delta)?;
    copy_doc_fields(src, dst, copy, delta)?;
    copy_history(src, dst, copy, delta)?;
    write_stats(dst, copy, &stats)
}

fn copy_postings(
    src: &ReadTransaction,
    dst: &WriteTransaction,
    copy: CollectionCopy<'_>,
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let owner = copy.owner();
    let from = src
        .open_table(POSTINGS)
        .map_err(|e| redb_err("copy open postings", e))?;
    let mut to = dst
        .open_table(POSTINGS)
        .map_err(|e| redb_err("copy open postings", e))?;
    for entry in from
        .range(owner.posting_start()..)
        .map_err(|e| redb_err("copy postings range", e))?
    {
        let (key, value) = entry.map_err(|e| redb_err("copy read postings", e))?;
        let row = key.value();
        let (d, t, c, f, _, s) = row;
        if !owner.owns(d, t, c, Some(f)) {
            break;
        }
        if copy.skips(s)? {
            continue;
        }
        let bytes = value.value();
        let old = to
            .insert(row, bytes)
            .map_err(|e| redb_err("copy write postings", e))?
            .map(|prior| prior.value().len());
        delta.replaced(d, t, c, posting_key_len(&row), old, bytes.len());
    }
    Ok(())
}

/// A DOC_LENGTHS-shaped table.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DocTable {
    Lengths,
    Terms,
}

impl DocTable {
    fn def(self) -> redb::TableDefinition<'static, DocKeyDef, &'static [u8]> {
        match self {
            Self::Lengths => DOC_LENGTHS,
            Self::Terms => DOC_TERMS,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Lengths => "doc_lengths",
            Self::Terms => "doc_terms",
        }
    }
}

/// Copy one DOC_LENGTHS-shaped table. For `DOC_LENGTHS`, returns the
/// document count and the token sum per field key. For `DOC_TERMS`, the
/// counts are empty.
fn copy_doc_rows(
    src: &ReadTransaction,
    dst: &WriteTransaction,
    table: DocTable,
    copy: CollectionCopy<'_>,
    delta: &mut StorageDelta,
) -> crate::Result<CountedStats> {
    let owner = copy.owner();
    let (def, name) = (table.def(), table.name());
    let counts_lengths = table == DocTable::Lengths;
    let mut stats = CountedStats::new();
    let from = src
        .open_table(def)
        .map_err(|e| redb_err(&format!("copy open {name}"), e))?;
    let mut to = dst
        .open_table(def)
        .map_err(|e| redb_err(&format!("copy open {name}"), e))?;
    for entry in from
        .range(owner.doc_start()..)
        .map_err(|e| redb_err(&format!("copy {name} range"), e))?
    {
        let (key, value) = entry.map_err(|e| redb_err(&format!("copy read {name}"), e))?;
        let row = key.value();
        let (d, t, c, f, s) = row;
        if !owner.owns(d, t, c, Some(f)) {
            break;
        }
        if copy.skips(s)? {
            continue;
        }
        let bytes = value.value();
        if counts_lengths {
            let len: u32 =
                zerompk::from_msgpack(bytes).map_err(|e| redb_err("copy decode doc_length", e))?;
            let counted = stats.entry(f.to_string()).or_insert((0, 0));
            counted.0 = counted.0.saturating_add(1);
            counted.1 = counted.1.saturating_add(u64::from(len));
        }
        put_doc_row(&mut to, row, bytes, delta)
            .map_err(|e| redb_err(&format!("copy write {name}"), e))?;
    }
    Ok(stats)
}

fn copy_doc_fields(
    src: &ReadTransaction,
    dst: &WriteTransaction,
    copy: CollectionCopy<'_>,
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let owner = copy.owner();
    let from = src
        .open_table(DOC_FIELDS)
        .map_err(|e| redb_err("copy open doc_fields", e))?;
    let mut to = dst
        .open_table(DOC_FIELDS)
        .map_err(|e| redb_err("copy open doc_fields", e))?;
    for entry in from
        .range(owner.doc_fields_start()..)
        .map_err(|e| redb_err("copy doc_fields range", e))?
    {
        let (key, value) = entry.map_err(|e| redb_err("copy read doc_fields", e))?;
        let row = key.value();
        let (d, t, c, s) = row;
        if !owner.owns(d, t, c, None) {
            break;
        }
        if copy.skips(s)? {
            continue;
        }
        put_doc_fields_row(&mut to, row, value.value(), delta)
            .map_err(|e| redb_err("copy write doc_fields", e))?;
    }
    Ok(())
}

/// Copy the live-start rows, their ordered twins, and the ended versions.
fn copy_history(
    src: &ReadTransaction,
    dst: &WriteTransaction,
    copy: CollectionCopy<'_>,
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let (db, tid, coll) = (copy.database_id, copy.tid, copy.collection);
    let owns = |d: u64, t: u64, c: &str| d == db && t == tid && c == coll;
    {
        let from = src
            .open_table(TEXT_LIVE_FROM)
            .map_err(|e| redb_err("copy open text live_from", e))?;
        let mut to = dst
            .open_table(TEXT_LIVE_FROM)
            .map_err(|e| redb_err("copy open text live_from", e))?;
        for entry in from
            .range((db, tid, coll, "", 0u32)..)
            .map_err(|e| redb_err("copy text live_from range", e))?
        {
            let (key, value) = entry.map_err(|e| redb_err("copy read text live_from", e))?;
            let row = key.value();
            let (d, t, c, f, s) = row;
            if !owns(d, t, c) {
                break;
            }
            if copy.skips(s)? {
                continue;
            }
            let was_there = to
                .insert(row, value.value())
                .map_err(|e| redb_err("copy write text live_from", e))?
                .is_some();
            if !was_there {
                delta.stored(d, t, c, live_from_row_len(c, f), 0);
            }
        }
    }
    {
        let from = src
            .open_table(TEXT_LIVE_BY_FROM)
            .map_err(|e| redb_err("copy open text live_by_from", e))?;
        let mut to = dst
            .open_table(TEXT_LIVE_BY_FROM)
            .map_err(|e| redb_err("copy open text live_by_from", e))?;
        for entry in from
            .range((db, tid, coll, "", i64::MIN, 0u32)..)
            .map_err(|e| redb_err("copy text live_by_from range", e))?
        {
            let (key, value) = entry.map_err(|e| redb_err("copy read text live_by_from", e))?;
            let row = key.value();
            let (d, t, c, f, _, s) = row;
            if !owns(d, t, c) {
                break;
            }
            if copy.skips(s)? {
                continue;
            }
            let was_there = to
                .insert(row, value.value())
                .map_err(|e| redb_err("copy write text live_by_from", e))?
                .is_some();
            if !was_there {
                delta.stored(d, t, c, live_by_from_row_len(c, f), 0);
            }
        }
    }
    let from = src
        .open_table(TEXT_ENDED)
        .map_err(|e| redb_err("copy open text ended", e))?;
    let mut to = dst
        .open_table(TEXT_ENDED)
        .map_err(|e| redb_err("copy open text ended", e))?;
    for entry in from
        .range((db, tid, coll, "", i64::MIN, 0u32, i64::MIN)..)
        .map_err(|e| redb_err("copy text ended range", e))?
    {
        let (key, value) = entry.map_err(|e| redb_err("copy read text ended", e))?;
        let row = key.value();
        let (d, t, c, f, _, s, _) = row;
        if !owns(d, t, c) {
            break;
        }
        if copy.skips(s)? {
            continue;
        }
        let bytes = value.value();
        let old = to
            .insert(row, bytes)
            .map_err(|e| redb_err("copy write text ended", e))?
            .map(|prior| prior.value().len());
        delta.replaced(d, t, c, ended_key_len(c, f), old, bytes.len());
    }
    Ok(())
}

/// Write one `STATS` row per field key the copy counted.
fn write_stats(
    dst: &WriteTransaction,
    copy: CollectionCopy<'_>,
    stats: &CountedStats,
) -> crate::Result<()> {
    let mut table = dst
        .open_table(STATS)
        .map_err(|e| redb_err("copy open stats", e))?;
    for (field, counts) in stats {
        let bytes =
            zerompk::to_msgpack_vec(counts).map_err(|e| redb_err("copy encode stats", e))?;
        let index = IndexScope::from_key(copy.collection, field);
        table
            .insert(
                stats_key(copy.database_id, copy.tid, index),
                bytes.as_slice(),
            )
            .map_err(|e| redb_err("copy write stats", e))?;
    }
    Ok(())
}
