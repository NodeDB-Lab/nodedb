// SPDX-License-Identifier: BUSL-1.1

//! System-time history of the inverted index.
//!
//! The version bookkeeping, the cut view, and the purge live in
//! `nodedb_fts::history`. This module runs them over the redb history
//! stores. A text write ends and starts versions inside the document
//! write's own transaction.

use std::collections::BTreeSet;

use redb::{ReadableDatabase as _, ReadableTable as _, WriteTransaction};

use nodedb_fts::IndexScope;
use nodedb_fts::history::{self, HistoryDoc, TextCut, TextCutView, UNVERSIONED};
use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;
use super::doc_fields;
use super::doc_image::tokens_from_postings;
use super::errors::inverted_err;
use super::indexing::IndexDocScope;
use crate::engine::sparse::btree::storage_bytes::SPARSE_COUNTER;
use crate::engine::sparse::fts_redb::backend::{RedbHistoryRead, RedbHistoryTxn};
use crate::engine::sparse::fts_redb::tables::TEXT_ENDED;
use crate::engine::storage_bytes::StorageDelta;

/// The history address of `doc` in `index`.
fn history_doc<'a>(doc: IndexDocScope<'_>, index: IndexScope<'a>) -> HistoryDoc<'a> {
    HistoryDoc {
        database_id: doc.database_id,
        tid: doc.tid.as_u64(),
        index,
        surrogate: doc.surrogate,
    }
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
        let mut store = RedbHistoryTxn::new(txn, delta);
        // A damaged index errors here. The history never stores a shorter text.
        history::end_version(&mut store, history_doc(doc, index), doc.ordinal, || {
            tokens_from_postings(txn, doc, index, doc_len)
        })
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
        let mut store = RedbHistoryTxn::new(txn, delta);
        history::start_version(&mut store, history_doc(doc, index), doc.ordinal)
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
        let store = RedbHistoryRead::new(&read);
        history::cut_view(&store, database_id, tid.as_u64(), index, TextCut::at(at))
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
            let step = {
                let mut store = RedbHistoryTxn::new(&txn, &mut delta);
                history::undo_at(&mut store, history_doc(doc, index), ordinal)?
            };
            if step.started_here {
                // `doc` writes at UNVERSIONED, so the strip ends no version.
                self.remove_scope_in_txn(&txn, doc, index, &mut delta)?;
                if !field_key.is_empty() {
                    fields.remove(field_key);
                }
            }
            if let Some(version) = step.restored {
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
        let dropped = {
            let mut store = RedbHistoryTxn::new(&txn, &mut delta);
            history::purge_below(&mut store, horizon_of)?
        };
        delta.apply(&txn, SPARSE_COUNTER)?;
        txn.commit()
            .map_err(|e| inverted_err("commit text purge", e))?;
        Ok(dropped)
    }
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

    /// The history stores the tokens this rebuild returns. A length the
    /// postings do not cover is an error, and never a text with empty tokens.
    #[test]
    fn a_length_the_postings_do_not_cover_is_refused() {
        use nodedb_fts::IndexScope;

        use crate::engine::sparse::inverted::doc_image::tokens_from_postings;
        use crate::engine::sparse::inverted::indexing::IndexDocScope;

        let (idx, _dir) = open_temp();
        let s = Surrogate::new(1);
        idx.index_document_at(DB, T, "docs", s, &body("alpha words"), 10)
            .unwrap();
        let doc = IndexDocScope {
            database_id: DB,
            tid: T,
            collection: "docs",
            surrogate: s,
            ordinal: 20,
        };
        let index = IndexScope::document("docs");
        let txn = idx.inner.backend().db().begin_write().unwrap();

        let tokens = tokens_from_postings(&txn, doc, index, 2).expect("covered");
        assert_eq!(tokens.len(), 2);
        assert!(tokens.iter().all(|token| !token.is_empty()));

        assert!(tokens_from_postings(&txn, doc, index, 3).is_err(), "gap");
        assert!(
            tokens_from_postings(&txn, doc, index, 1).is_err(),
            "past end"
        );
        assert!(
            tokens_from_postings(&txn, doc, index, u32::MAX).is_err(),
            "a corrupt length allocates nothing"
        );
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
