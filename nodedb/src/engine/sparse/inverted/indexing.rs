// SPDX-License-Identifier: BUSL-1.1

//! Document indexing for the inverted index.
//!
//! All writes bypass the LSM memtable and go directly to the persistent
//! POSTINGS / DOC_LENGTHS / DOC_TERMS / STATS tables so they can participate
//! in the caller's redb write transaction (Origin transactional indexing).
//!
//! A re-index is a full replacement of the document's index footprint, not an
//! overlay: the terms the new text no longer contains are retracted first
//! (see the `doc_terms` sibling module), then the new text's postings are
//! written. Removal lives in the `removal` sibling module.

use std::collections::{BTreeSet, HashMap};

use redb::{ReadableTable as _, WriteTransaction};
use tracing::debug;

use nodedb_fts::posting::Posting;
use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;
use super::doc_terms;
use super::errors::inverted_err;
use crate::engine::sparse::fts_redb::tables::{DOC_LENGTHS, POSTINGS, STATS};

/// `(database_id, tenant, collection, surrogate)` scope shared by the
/// transaction-participating indexing entry points.
#[derive(Clone, Copy)]
pub struct IndexDocScope<'a> {
    /// Owning database id.
    pub database_id: u64,
    /// Owning tenant id.
    pub tid: TenantId,
    /// Collection the document belongs to.
    pub collection: &'a str,
    /// Global surrogate identity of the document.
    pub surrogate: Surrogate,
}

impl InvertedIndex {
    /// Tokenize `text` with the collection's configured analyzer (falls back
    /// to the default analyzer when the collection has none bound).
    ///
    /// This is the single analyzer-resolution entry point for the whole
    /// inverted-index module: forward indexing (`index_document`,
    /// `index_document_in_txn`) and query-term canonicalization
    /// (`phrase_search`) all call through here so a document is always
    /// tokenized the same way it is later matched against, whether the write
    /// is durable or still staged in an open transaction.
    pub fn analyze_for_collection(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        text: &str,
    ) -> crate::Result<Vec<String>> {
        self.inner
            .analyze_for_collection(database_id, tid.as_u64(), collection, text)
    }

    /// Bind a collection's per-collection FTS analyzer, persisted to backend
    /// metadata. `analyze_for_collection` resolves it from this point on for
    /// every write and read of the collection's text.
    pub fn set_collection_analyzer(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        analyzer_name: &str,
    ) -> crate::Result<()> {
        self.inner
            .set_collection_analyzer(database_id, tid.as_u64(), collection, analyzer_name)
    }

    /// Bind whether searches over `collection` fall back to fuzzy matching by
    /// default, persisted to backend metadata. `FtsIndex::search` ORs it into
    /// every query's own fuzzy flag.
    pub fn set_collection_fuzzy(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        fuzzy: bool,
    ) -> crate::Result<()> {
        self.inner
            .set_collection_fuzzy(database_id, tid.as_u64(), collection, fuzzy)
    }

    /// Index a document's text content.
    ///
    /// Text that analyzes to no terms is a removal, not a no-op: an update
    /// that strips a document of every indexable word must take it out of the
    /// index, or it keeps matching the words it used to contain.
    pub fn index_document(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogate: Surrogate,
        text: &str,
    ) -> crate::Result<()> {
        let tokens = self.analyze_for_collection(database_id, tid, collection, text)?;
        if tokens.is_empty() {
            return self.remove_document(database_id, tid, collection, surrogate);
        }
        let scope = IndexDocScope {
            database_id,
            tid,
            collection,
            surrogate,
        };

        let db = self.inner.backend().db();
        let write_txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        self.write_index_data(&write_txn, scope, &tokens)?;
        write_txn
            .commit()
            .map_err(|e| inverted_err("commit index", e))?;
        Ok(())
    }

    /// Index a document within an externally-owned write transaction.
    ///
    /// Same empty-text semantics as [`Self::index_document`].
    pub fn index_document_in_txn(
        &self,
        txn: &WriteTransaction,
        scope: IndexDocScope<'_>,
        text: &str,
    ) -> crate::Result<()> {
        let tokens =
            self.analyze_for_collection(scope.database_id, scope.tid, scope.collection, text)?;
        if tokens.is_empty() {
            return self.remove_document_in_txn(txn, scope);
        }
        self.write_index_data(txn, scope, &tokens)
    }

    /// Core indexing logic: writes postings, doc length, and stats within
    /// a transaction. Bypasses the LSM memtable so Origin transactions can
    /// stay atomic with the document write.
    pub(super) fn write_index_data(
        &self,
        txn: &WriteTransaction,
        scope: IndexDocScope<'_>,
        tokens: &[String],
    ) -> crate::Result<()> {
        let IndexDocScope {
            database_id,
            tid,
            collection,
            surrogate,
        } = scope;
        let t = tid.as_u64();

        let mut term_postings: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_postings
                .entry(token.as_str())
                .or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        let doc_len = tokens.len() as u32;

        // The surrogate's prior length is read BEFORE anything is written, in
        // the same write transaction as every mutation below, so the
        // check-and-increment is atomic (no TOCTOU). Its presence is the
        // idempotency key AND the "is this an update?" signal: it means this
        // surrogate was already counted into STATS by a prior index (live
        // write or an earlier WAL replay pass), so a repeat index of the SAME
        // surrogate must not increment `count` again — and it means a previous
        // version of the document holds postings that may need retracting.
        let prior_len = prior_doc_length(txn, scope)?;

        // Retract the document from every term the previous version put it in
        // that the new text does not: `write_index_data` below only touches
        // terms present in `tokens`, so without this a word deleted by an
        // update keeps its posting and its inflated `df` forever.
        let new_terms: BTreeSet<&str> = term_postings.keys().copied().collect();
        let dropped: Vec<String> = doc_terms::occupied_terms(txn, scope, prior_len.is_some())?
            .into_iter()
            .filter(|term| !new_terms.contains(term.as_str()))
            .collect();
        doc_terms::strip_postings(txn, scope, &dropped)?;
        doc_terms::put(txn, scope, &new_terms)?;

        let mut postings_table = txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("open postings", e))?;

        for (term, (freq, positions)) in &term_postings {
            let posting = Posting {
                doc_id: surrogate,
                term_freq: *freq,
                positions: positions.clone(),
            };

            let mut existing: Vec<Posting> = postings_table
                .get((database_id, t, collection, *term))
                .ok()
                .flatten()
                .and_then(|v| zerompk::from_msgpack(v.value()).ok())
                .unwrap_or_default();

            existing.retain(|p| p.doc_id != surrogate);
            existing.push(posting);

            let bytes = zerompk::to_msgpack_vec(&existing)
                .map_err(|e| inverted_err("serialize postings", e))?;
            postings_table
                .insert((database_id, t, collection, *term), bytes.as_slice())
                .map_err(|e| inverted_err("insert posting", e))?;
        }
        drop(postings_table);

        let mut lengths = txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| inverted_err("open doc_lengths", e))?;

        let len_bytes =
            zerompk::to_msgpack_vec(&doc_len).map_err(|e| inverted_err("serialize doc_len", e))?;
        lengths
            .insert(
                (database_id, t, collection, surrogate.as_u32()),
                len_bytes.as_slice(),
            )
            .map_err(|e| inverted_err("insert doc_len", e))?;
        drop(lengths);

        let (count_delta, total_delta) = match prior_len {
            // New document: bump the doc count and add its full length.
            None => (1i64, doc_len as i64),
            // Re-index of an already-counted surrogate (replay of an
            // unchanged doc, or a genuine re-index of changed content): the
            // doc was already counted once, so `count` does not change;
            // `total` only moves by the delta between the new and prior
            // length (zero for an identical replay).
            Some(prior) => (0i64, doc_len as i64 - prior as i64),
        };

        Self::update_stats_in_txn(txn, database_id, tid, collection, count_delta, total_delta)?;

        debug!(database_id, tid = t, %collection, surrogate = surrogate.as_u32(), tokens = tokens.len(), terms = term_postings.len(), "indexed document");
        Ok(())
    }

    /// Atomically update `(doc_count, total_token_sum)` in STATS by the given
    /// explicit deltas.
    ///
    /// Callers compute `count_delta` / `total_delta` themselves rather than
    /// this function inferring "new doc vs. removal" from the sign of a
    /// single combined delta: a re-index of an already-counted surrogate
    /// (e.g. WAL replay) needs `count_delta == 0` with a `total_delta` that
    /// may be positive, negative, or zero — a case the old sign-based
    /// inference could not express, which is what caused STATS to be
    /// double-counted on replay.
    pub(super) fn update_stats_in_txn(
        txn: &WriteTransaction,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        count_delta: i64,
        total_delta: i64,
    ) -> crate::Result<()> {
        let t = tid.as_u64();
        let mut stats = txn
            .open_table(STATS)
            .map_err(|e| inverted_err("open stats", e))?;
        let (count, total) = stats
            .get((database_id, t, collection))
            .ok()
            .flatten()
            .and_then(|v| zerompk::from_msgpack::<(u32, u64)>(v.value()).ok())
            .unwrap_or((0, 0));

        let new_count = (i64::from(count) + count_delta).max(0) as u32;
        let new_total = (total as i64 + total_delta).max(0) as u64;

        let bytes = zerompk::to_msgpack_vec(&(new_count, new_total))
            .map_err(|e| inverted_err("serialize stats", e))?;
        stats
            .insert((database_id, t, collection), bytes.as_slice())
            .map_err(|e| inverted_err("insert stats", e))?;
        Ok(())
    }
}

/// The token length a previous index recorded for this surrogate, or `None`
/// when the document is not in the index.
///
/// Shared by the index and removal paths: both need it as the authoritative
/// "is this document already counted into STATS?" answer, and both must read
/// it inside the same write transaction as the mutation it gates.
pub(super) fn prior_doc_length(
    txn: &WriteTransaction,
    scope: IndexDocScope<'_>,
) -> crate::Result<Option<u32>> {
    let table = txn
        .open_table(DOC_LENGTHS)
        .map_err(|e| inverted_err("open doc_lengths", e))?;
    let prior = table
        .get((
            scope.database_id,
            scope.tid.as_u64(),
            scope.collection,
            scope.surrogate.as_u32(),
        ))
        .map_err(|e| inverted_err("read doc_length", e))?
        .and_then(|v| zerompk::from_msgpack::<u32>(v.value()).ok());
    Ok(prior)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use redb::Database;

    use nodedb_fts::FtsSearchParams;
    use nodedb_fts::posting::QueryMode;

    use super::*;

    const DB: u64 = 0;
    const T: TenantId = TenantId::new(1);

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(Database::create(&path).unwrap());
        let idx =
            InvertedIndex::open(db, crate::data::executor::core_loop::test_governor()).unwrap();
        (idx, dir)
    }

    /// A term that an UPDATE removed from a document must leave no posting for
    /// that document, and its `df` must fall back to the documents that really do
    /// contain it — a stale posting would keep the document matching a word it no
    /// longer has AND inflate the term's IDF for every other query.
    #[test]
    fn update_dropping_a_term_removes_its_posting_and_restores_df() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), "bravo charlie")
            .unwrap();
        assert_eq!(idx.term_df(DB, T, "docs", "bravo").unwrap(), 2);

        // Document 1 loses "bravo" and gains "delta".
        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha delta")
            .unwrap();

        assert_eq!(
            idx.term_df(DB, T, "docs", "bravo").unwrap(),
            1,
            "only the document that still contains the term may be counted"
        );
        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "bravo",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 1, "the updated document must not match");
        assert_eq!(results[0].doc_id, Surrogate::new(2));

        // The corpus itself is unchanged: still two documents, both two tokens.
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 2);
        assert_eq!(avg_len, 2.0);
    }

    /// A term the update ADDS must be indexed, so the retraction above cannot be
    /// implemented by simply refusing to touch a re-indexed document.
    #[test]
    fn update_adding_a_term_indexes_it() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha")
            .unwrap();
        assert_eq!(idx.term_df(DB, T, "docs", "delta").unwrap(), 0);

        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha delta")
            .unwrap();

        assert_eq!(idx.term_df(DB, T, "docs", "alpha").unwrap(), 1);
        assert_eq!(
            idx.term_df(DB, T, "docs", "delta").unwrap(),
            1,
            "a term introduced by the update must be searchable"
        );
    }

    /// Re-indexing with an UNCHANGED token set must not move any count — this is
    /// the WAL-replay shape, and the retraction path must not disturb it.
    #[test]
    fn reindex_with_unchanged_tokens_is_a_no_op_for_counts() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo charlie")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), "bravo")
            .unwrap();

        let before = (
            idx.corpus_stats(DB, T, "docs").unwrap(),
            idx.term_df(DB, T, "docs", "alpha").unwrap(),
            idx.term_df(DB, T, "docs", "bravo").unwrap(),
            idx.term_df(DB, T, "docs", "charlie").unwrap(),
        );

        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo charlie")
            .unwrap();

        let after = (
            idx.corpus_stats(DB, T, "docs").unwrap(),
            idx.term_df(DB, T, "docs", "alpha").unwrap(),
            idx.term_df(DB, T, "docs", "bravo").unwrap(),
            idx.term_df(DB, T, "docs", "charlie").unwrap(),
        );
        assert_eq!(before, after, "an identical re-index must change nothing");
    }

    /// A document indexed before term sets were recorded has no stored set. Its
    /// first re-index must still retract dropped terms, via the fallback scan.
    #[test]
    fn update_of_a_document_without_a_stored_term_set_still_retracts() {
        use crate::engine::sparse::fts_redb::tables::DOC_TERMS;

        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo")
            .unwrap();

        // Drop the term-set row to reproduce a document written by a build that
        // did not maintain one.
        {
            let db = idx.backend().db();
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(DOC_TERMS).unwrap();
                table.remove((DB, T.as_u64(), "docs", 1u32)).unwrap();
            }
            txn.commit().unwrap();
        }

        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha")
            .unwrap();

        assert_eq!(
            idx.term_df(DB, T, "docs", "bravo").unwrap(),
            0,
            "the fallback scan must retract the dropped term"
        );
        assert_eq!(idx.term_df(DB, T, "docs", "alpha").unwrap(), 1);
    }

    /// An update that leaves a document with no indexable text at all is the
    /// extreme case of the same bug: every term dropped out, so the document must
    /// leave the index entirely rather than keep matching its old words.
    #[test]
    fn update_to_empty_text_removes_the_document() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), "alpha bravo")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), "bravo")
            .unwrap();

        idx.index_document(DB, T, "docs", Surrogate::new(1), "")
            .unwrap();

        assert_eq!(idx.term_df(DB, T, "docs", "alpha").unwrap(), 0);
        assert_eq!(
            idx.term_df(DB, T, "docs", "bravo").unwrap(),
            1,
            "the other document keeps its posting"
        );
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 1, "the emptied document is no longer in the corpus");
        assert_eq!(avg_len, 1.0);
    }
}
