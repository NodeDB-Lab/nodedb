// SPDX-License-Identifier: BUSL-1.1

//! System-time history of the inverted index.
//!
//! A write indexes a document's text at its system-time ordinal. An update
//! or a delete ends the indexed version at the write's ordinal: the
//! version's analyzed tokens move to the history rows (see
//! `fts_redb::backend::history_rows`), so a read at an earlier cut still
//! matches it. A read at a cut sees the index as a transaction sees its
//! staged writes: the live documents indexed after the cut are hidden, and
//! the ended versions visible at the cut compete in the ranking from their
//! tokens. Membership is exact at the cut. BM25 scores use the current
//! corpus statistics.

use std::collections::BTreeSet;

use redb::{ReadableDatabase as _, ReadableTable as _, WriteTransaction};

use nodedb_fts::{IndexScope, StagedDoc};
use nodedb_types::{Surrogate, SurrogateBitmap, TenantId};

use super::core::InvertedIndex;
use super::doc_fields;
use super::doc_terms;
use super::errors::inverted_err;
use super::indexing::IndexDocScope;
use crate::engine::sparse::btree::storage_bytes::SPARSE_COUNTER;
use crate::engine::sparse::fts_redb::backend::history_rows::{self, EndedVersion};
use crate::engine::sparse::fts_redb::backend::posting_rows;
use crate::engine::sparse::fts_redb::tables::{POSTINGS, TEXT_ENDED, TEXT_LIVE_BY_FROM};
use crate::engine::storage_bytes::StorageDelta;

/// The ordinal of a write that changes no history: it records no ended
/// version and no start. An index rebuild and the unit tests write this way.
pub const UNVERSIONED: i64 = i64::MIN;

/// The view of one index at a cut: the indexed documents a read at the cut
/// does not see, and the ended versions it sees in their place.
#[derive(Debug, Default)]
pub struct TextCutView {
    pub hidden: SurrogateBitmap,
    pub docs: Vec<StagedDoc>,
}

impl InvertedIndex {
    /// End the indexed version of `doc` in `index`, which holds `doc_len`
    /// tokens, at `doc.ordinal`. Runs before the write strips the version's
    /// postings. A re-apply of the write that indexed the version (a WAL
    /// replay) ends nothing. `delta` records the stored bytes it changes.
    pub(super) fn end_text_version_in_txn(
        &self,
        txn: &WriteTransaction,
        doc: IndexDocScope<'_>,
        index: IndexScope<'_>,
        doc_len: u32,
        delta: &mut StorageDelta,
    ) -> crate::Result<()> {
        let t = doc.tid.as_u64();
        if doc.ordinal == UNVERSIONED {
            return Ok(());
        }
        let live_from = history_rows::live_from(txn, doc.database_id, t, index, doc.surrogate)?;
        if live_from == Some(doc.ordinal) {
            return Ok(());
        }
        let tokens = stored_tokens(txn, doc, index, doc_len)?;
        history_rows::clear_live(txn, doc.database_id, t, index, doc.surrogate, delta)?;
        history_rows::put_ended(
            txn,
            (doc.database_id, t),
            index,
            &EndedVersion {
                surrogate: doc.surrogate,
                sys_from: live_from.unwrap_or(UNVERSIONED),
                sys_to: doc.ordinal,
                tokens,
            },
            delta,
        )
    }

    /// Record the version of `doc` the write indexed into `index` as live
    /// from `doc.ordinal`. `delta` records the stored bytes it changes.
    pub(super) fn start_text_version_in_txn(
        &self,
        txn: &WriteTransaction,
        doc: IndexDocScope<'_>,
        index: IndexScope<'_>,
        delta: &mut StorageDelta,
    ) -> crate::Result<()> {
        if doc.ordinal == UNVERSIONED {
            return Ok(());
        }
        history_rows::set_live(
            txn,
            (doc.database_id, doc.tid.as_u64()),
            index,
            doc.surrogate,
            doc.ordinal,
            delta,
        )
    }

    /// The view of `index` at system-time ordinal `at`.
    pub fn text_cut_view(
        &self,
        database_id: u64,
        tid: TenantId,
        index: IndexScope<'_>,
        at: i64,
    ) -> crate::Result<TextCutView> {
        let db = self.inner.backend().db();
        let read = db.begin_read().map_err(|e| inverted_err("read txn", e))?;
        let live_by_from = read
            .open_table(TEXT_LIVE_BY_FROM)
            .map_err(|e| inverted_err("open text live_by_from", e))?;
        let ended = read
            .open_table(TEXT_ENDED)
            .map_err(|e| inverted_err("open text ended", e))?;
        let rows =
            history_rows::cut_rows(&live_by_from, &ended, database_id, tid.as_u64(), index, at)?;
        Ok(TextCutView {
            hidden: rows.hidden,
            docs: rows.docs,
        })
    }

    /// Reverse the text writes of `surrogate` made at system-time ordinal
    /// `ordinal`, in its own write transaction: the version an index started
    /// at `ordinal` leaves it, and the version an index ended at `ordinal`
    /// is live there again. The undo of a rolled-back put or delete. Returns
    /// whether an ended version was restored.
    pub fn undo_text_version(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogate: Surrogate,
        ordinal: i64,
    ) -> crate::Result<bool> {
        let db = self.inner.backend().db();
        let txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        let t = tid.as_u64();
        let doc = IndexDocScope {
            database_id,
            tid,
            collection,
            surrogate,
            ordinal: UNVERSIONED,
        };
        self.note_doc_write(doc);
        // The version started at `ordinal` leaves its index with its start.
        let mut fields: BTreeSet<String> = doc_fields::read(&txn, doc)?.into_iter().collect();
        let ended_fields = ended_field_keys(&txn, database_id, t, collection, surrogate, ordinal)?;
        let mut keys: BTreeSet<String> = fields.iter().cloned().collect();
        keys.extend(ended_fields.iter().cloned());
        keys.insert(String::new());
        let mut restored_any = false;
        let mut delta = StorageDelta::default();
        for field_key in &keys {
            let index = IndexScope::from_key(collection, field_key);
            if history_rows::live_from(&txn, database_id, t, index, surrogate)? == Some(ordinal) {
                self.remove_scope_in_txn(&txn, doc, index, &mut delta)?;
                history_rows::clear_live(&txn, database_id, t, index, surrogate, &mut delta)?;
                if !field_key.is_empty() {
                    fields.remove(field_key);
                }
            }
            if let Some(version) = history_rows::take_ended_at(
                &txn,
                (database_id, t),
                index,
                surrogate,
                ordinal,
                &mut delta,
            )? {
                let restored = IndexDocScope {
                    ordinal: version.sys_from,
                    ..doc
                };
                self.write_index_data(&txn, restored, index, &version.tokens, &mut delta)?;
                restored_any = true;
                if !field_key.is_empty() {
                    fields.insert(field_key.clone());
                }
            }
        }
        let held: BTreeSet<&str> = fields.iter().map(String::as_str).collect();
        doc_fields::put(&txn, doc, &held, &mut delta)?;
        delta.apply(&txn, SPARSE_COUNTER)?;
        txn.commit()
            .map_err(|e| inverted_err("commit text undo", e))?;
        Ok(restored_any)
    }

    /// Drop every text version no cut at or above `horizon` reads: versions
    /// ended at or below `horizon`. The version of each document visible at
    /// the horizon stays. Returns the number of ended versions dropped.
    pub fn purge_text_versions_below(&self, horizon: i64) -> crate::Result<usize> {
        self.purge_text_versions_below_by(&|_, _, _| Some(horizon))
    }

    /// Bytes the ended text versions of `collection` hold: each version's
    /// key and its analyzed tokens.
    pub fn text_history_bytes(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<u64> {
        let db = self.inner.backend().db();
        let read = db.begin_read().map_err(|e| inverted_err("read txn", e))?;
        let ended = read
            .open_table(TEXT_ENDED)
            .map_err(|e| inverted_err("open text ended", e))?;
        let mut bytes = 0u64;
        for entry in ended
            .iter()
            .map_err(|e| inverted_err("scan text ended", e))?
        {
            let (key, value) = entry.map_err(|e| inverted_err("read text ended", e))?;
            let (d, t, c, f, ..) = key.value();
            if d == database_id && t == tid.as_u64() && c == collection {
                let key_bytes = c.len() + f.len() + 3 * 8 + 4 + 8;
                bytes = bytes.saturating_add((key_bytes + value.value().len()) as u64);
            }
        }
        Ok(bytes)
    }

    /// [`Self::purge_text_versions_below`] with a horizon per collection:
    /// `horizon_of(database, tenant, collection)`, `None` to keep the
    /// collection's whole history.
    pub fn purge_text_versions_below_by(
        &self,
        horizon_of: &dyn Fn(u64, u64, &str) -> Option<i64>,
    ) -> crate::Result<usize> {
        let db = self.inner.backend().db();
        let txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        let mut delta = StorageDelta::default();
        let dropped = history_rows::purge_below(&txn, horizon_of, &mut delta)?;
        delta.apply(&txn, SPARSE_COUNTER)?;
        txn.commit()
            .map_err(|e| inverted_err("commit text purge", e))?;
        Ok(dropped)
    }
}

/// The analyzed tokens of `doc`'s indexed version in `index`, in order,
/// rebuilt from its postings: each term's positions place it in the
/// sequence.
fn stored_tokens(
    txn: &WriteTransaction,
    doc: IndexDocScope<'_>,
    index: IndexScope<'_>,
    doc_len: u32,
) -> crate::Result<Vec<String>> {
    let terms = doc_terms::occupied_terms(txn, doc, index, true)?;
    let postings = txn
        .open_table(POSTINGS)
        .map_err(|e| inverted_err("open postings", e))?;
    let mut tokens: Vec<String> = vec![String::new(); doc_len as usize];
    for term in terms {
        let Some(posting) = posting_rows::read_one(
            &postings,
            doc.database_id,
            doc.tid.as_u64(),
            index,
            &term,
            doc.surrogate,
        )?
        else {
            continue;
        };
        for position in posting.positions {
            if let Some(slot) = tokens.get_mut(position as usize) {
                slot.clone_from(&term);
            }
        }
    }
    Ok(tokens)
}

/// The field keys of `collection` holding a version of `surrogate` that
/// ended at `ordinal`. The whole-document index's key is empty.
fn ended_field_keys(
    txn: &WriteTransaction,
    database_id: u64,
    tid: u64,
    collection: &str,
    surrogate: Surrogate,
    ordinal: i64,
) -> crate::Result<BTreeSet<String>> {
    let ended = txn
        .open_table(TEXT_ENDED)
        .map_err(|e| inverted_err("open text ended", e))?;
    let mut fields = BTreeSet::new();
    let lo = (database_id, tid, collection, "", i64::MIN, 0u32, i64::MIN);
    for entry in ended
        .range(lo..)
        .map_err(|e| inverted_err("range text ended", e))?
    {
        let (key, _) = entry.map_err(|e| inverted_err("read text ended", e))?;
        let (d, t, c, f, to, s, _) = key.value();
        if d != database_id || t != tid || c != collection {
            break;
        }
        if to == ordinal && s == surrogate.as_u32() {
            fields.insert(f.to_string());
        }
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_fts::posting::QueryMode;
    use nodedb_fts::{FtsSearchParams, StagedView};
    use nodedb_types::{Surrogate, TenantId};

    use super::InvertedIndex;
    use crate::engine::durability_gate::GatedDatabase;
    use crate::engine::sparse::inverted::test_support::body;

    const DB: u64 = 0;
    const T: TenantId = TenantId::new(1);

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(GatedDatabase::new(redb::Database::create(&path).unwrap()));
        let idx =
            InvertedIndex::open(db, crate::data::executor::core_loop::test_governor()).unwrap();
        (idx, dir)
    }

    /// The documents a search for `query` at ordinal `at` matches, sorted.
    fn hits_at(idx: &InvertedIndex, query: &str, at: i64) -> Vec<u32> {
        let view = idx.text_cut_view(DB, T, "docs".into(), at).unwrap();
        let staged = StagedView::new(view.hidden, false, view.docs);
        let mut hits: Vec<u32> = idx
            .search_staged(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query,
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
                Some(&staged),
            )
            .unwrap()
            .into_iter()
            .map(|h| h.doc_id.as_u32())
            .collect();
        hits.sort_unstable();
        hits
    }

    /// Index at 10, update at 20, delete at 30: each cut matches the text of
    /// the version visible there.
    #[test]
    fn each_cut_matches_the_version_visible_there() {
        let (idx, _dir) = open_temp();
        let s = Surrogate::new(1);
        idx.index_document_at(DB, T, "docs", s, &body("alpha words"), 10)
            .unwrap();
        idx.index_document_at(DB, T, "docs", Surrogate::new(2), &body("alpha other"), 10)
            .unwrap();
        idx.index_document_at(DB, T, "docs", s, &body("beta words"), 20)
            .unwrap();
        idx.remove_document_at(DB, T, "docs", s, 30).unwrap();

        assert!(hits_at(&idx, "alpha", 5).is_empty());
        assert_eq!(hits_at(&idx, "alpha", 15), vec![1, 2]);
        assert_eq!(hits_at(&idx, "beta", 15), Vec::<u32>::new());
        assert_eq!(hits_at(&idx, "alpha", 25), vec![2]);
        assert_eq!(hits_at(&idx, "beta", 25), vec![1]);
        assert!(hits_at(&idx, "beta", 35).is_empty(), "deleted at 30");
    }

    /// A WAL replay of the write that indexed a version ends nothing.
    #[test]
    fn a_reapplied_write_keeps_no_history() {
        let (idx, _dir) = open_temp();
        let s = Surrogate::new(1);
        idx.index_document_at(DB, T, "docs", s, &body("alpha"), 10)
            .unwrap();
        idx.index_document_at(DB, T, "docs", s, &body("alpha"), 10)
            .unwrap();
        let view = idx.text_cut_view(DB, T, "docs".into(), 15).unwrap();
        assert!(view.docs.is_empty());
        assert_eq!(idx.purge_text_versions_below(i64::MAX).unwrap(), 0);
    }

    /// An undo of the update puts the prior version back live, and a purge
    /// drops the versions no cut at or above the horizon reads.
    #[test]
    fn undo_and_purge() {
        let (idx, _dir) = open_temp();
        let s = Surrogate::new(1);
        idx.index_document_at(DB, T, "docs", s, &body("alpha"), 10)
            .unwrap();
        idx.index_document_at(DB, T, "docs", s, &body("beta"), 20)
            .unwrap();
        assert!(idx.undo_text_version(DB, T, "docs", s, 20).unwrap());
        assert_eq!(hits_at(&idx, "alpha", 25), vec![1]);
        assert!(hits_at(&idx, "beta", 25).is_empty());

        idx.index_document_at(DB, T, "docs", s, &body("gamma"), 30)
            .unwrap();
        assert_eq!(hits_at(&idx, "alpha", 25), vec![1]);
        assert!(idx.purge_text_versions_below(30).unwrap() > 0);
        assert!(hits_at(&idx, "alpha", 25).is_empty(), "purged below 30");
        assert_eq!(hits_at(&idx, "gamma", 30), vec![1]);
    }

    /// A collection whose horizon is `None` keeps its text history. A
    /// collection at a horizon keeps the version a read at it sees.
    #[test]
    fn a_per_collection_purge_keeps_a_retained_history() {
        let (idx, _dir) = open_temp();
        let s = Surrogate::new(1);
        for coll in ["docs", "audit"] {
            idx.index_document_at(DB, T, coll, s, &body("alpha"), 10)
                .unwrap();
            idx.index_document_at(DB, T, coll, s, &body("beta"), 20)
                .unwrap();
        }
        let dropped = idx
            .purge_text_versions_below_by(&|_, _, collection| (collection != "audit").then_some(25))
            .unwrap();
        // A document's text is indexed whole and per field, so docs' alpha
        // version ended in two indexes: the document's and `body`'s.
        assert_eq!(dropped, 2, "only docs' alpha version ended at or below 25");
        assert!(
            idx.text_cut_view(DB, T, "docs".into(), 15)
                .unwrap()
                .docs
                .is_empty(),
            "docs no longer reads alpha at 15"
        );
        let audit_hits = idx
            .text_cut_view(DB, T, "audit".into(), 15)
            .unwrap()
            .docs
            .len();
        assert_eq!(
            audit_hits, 1,
            "the retained collection still reads alpha at 15"
        );
    }
}
