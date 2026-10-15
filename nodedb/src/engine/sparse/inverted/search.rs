// SPDX-License-Identifier: BUSL-1.1

//! Search paths for the inverted index: BM25, phrase, fuzzy, and the
//! highlighting/offset helpers used by the SQL projection layer.

use redb::ReadableDatabase;
use tracing::debug;

use nodedb_fts::posting::{MatchOffset, Posting, TextSearchResult};
use nodedb_fts::{FtsSearchParams, IndexScope};
use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;
use super::errors::{fts_index_err, inverted_err};
use crate::engine::sparse::fts_redb::backend::posting_rows;
use crate::engine::sparse::fts_redb::tables::POSTINGS;

/// Query and tuning parameters for an inverted-index phrase search.
///
/// The `(database_id, tid, index)` scope is passed separately so callers
/// can reuse their existing scope variables.
pub struct PhraseSearchParams<'a> {
    /// The phrase as analyzed tokens, in order. They must appear as a
    /// contiguous sequence. The caller analyzes the phrase once with the
    /// collection's analyzer, the same analysis its indexed text took.
    pub terms: &'a [String],
    /// Maximum number of results to return.
    pub top_k: usize,
    /// Optional surrogate bitmap restricting candidates before position match.
    pub prefilter: Option<&'a nodedb_types::SurrogateBitmap>,
    /// Documents that never match, excluded before the top-k cut.
    pub exclude: Option<&'a nodedb_types::SurrogateBitmap>,
}

impl InvertedIndex {
    /// Search the inverted index for an exact phrase.
    ///
    /// Returns all documents where `terms` appear as a contiguous sequence in
    /// the original token stream. Positions are stored per-term in every
    /// `Posting`, so phrase matching is a set intersection on position offsets.
    ///
    /// The result is scored by position rank (earlier = higher). An optional
    /// `prefilter` bitmap restricts the candidate set before position matching.
    pub fn phrase_search<'a>(
        &self,
        database_id: u64,
        tid: TenantId,
        index: impl Into<IndexScope<'a>>,
        params: PhraseSearchParams<'_>,
    ) -> crate::Result<Vec<TextSearchResult>> {
        let index = index.into();
        let collection = index.collection();
        let PhraseSearchParams {
            terms,
            top_k,
            prefilter,
            exclude,
        } = params;
        if terms.is_empty() {
            return Ok(Vec::new());
        }

        let t = tid.as_u64();
        let db = self.inner.backend().db();
        let read_txn = db.begin_read().map_err(|e| inverted_err("read txn", e))?;
        let postings_table = read_txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("open postings", e))?;

        // The first term's postings are the candidate set.
        let first: Vec<Posting> =
            posting_rows::read_list(&postings_table, database_id, t, index, &terms[0])?;
        let mut matches: Vec<(Surrogate, u32)> = Vec::new();

        'outer: for posting in &first {
            // Prefilter check.
            if prefilter.is_some_and(|bm| !bm.contains(posting.doc_id))
                || exclude.is_some_and(|bm| bm.contains(posting.doc_id))
            {
                continue;
            }

            let surrogate = posting.doc_id;

            // The candidate's posting under each remaining term: one point
            // read per term. A term the document lacks rules it out.
            let mut rest: Vec<Posting> = Vec::with_capacity(terms.len() - 1);
            for term in &terms[1..] {
                match posting_rows::read_one(
                    &postings_table,
                    database_id,
                    t,
                    index,
                    term,
                    surrogate,
                )? {
                    Some(other) => rest.push(other),
                    None => continue 'outer,
                }
            }

            // For each start position of the first term, check subsequent terms.
            'pos: for &start_pos in &posting.positions {
                for (offset, other_posting) in rest.iter().enumerate() {
                    let expected_pos = start_pos + (offset as u32) + 1;
                    if !other_posting.positions.contains(&expected_pos) {
                        continue 'pos;
                    }
                }
                // All terms found at consecutive positions — record match.
                matches.push((surrogate, start_pos));
                break; // One match per doc is sufficient.
            }
        }

        // Sort by earliest match position (earlier = more relevant).
        matches.sort_by_key(|(_, pos)| *pos);

        let results: Vec<TextSearchResult> = matches
            .into_iter()
            .take(top_k)
            .enumerate()
            .map(|(rank, (doc_id, pos))| TextSearchResult {
                doc_id,
                score: 1.0 / (1.0 + pos as f32 + rank as f32),
                fuzzy: false,
            })
            .collect();

        debug!(
            tid = t,
            %collection,
            field = index.field_key(),
            terms = terms.len(),
            hits = results.len(),
            "phrase search"
        );
        Ok(results)
    }

    /// Search the inverted index using BM25 scoring with explicit params.
    ///
    /// Supports `NOT <term>` and `-<term>` negation in the query string.
    /// Returns `Err` for invalid queries (NOT-only, unsupported parentheses).
    pub fn search<'a>(
        &self,
        database_id: u64,
        tid: TenantId,
        index: impl Into<IndexScope<'a>>,
        params: FtsSearchParams<'_>,
    ) -> crate::Result<Vec<TextSearchResult>> {
        self.inner
            .search(database_id, tid.as_u64(), index, params)
            .map_err(fts_index_err)
    }

    /// Generate highlighted text with matched query terms wrapped in tags.
    pub fn highlight(&self, text: &str, query: &str, prefix: &str, suffix: &str) -> String {
        self.inner.highlight(text, query, prefix, suffix)
    }

    /// Return byte offsets of matched query terms in the original text.
    pub fn offsets(&self, text: &str, query: &str) -> Vec<MatchOffset> {
        self.inner.offsets(text, query)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use redb::Database;

    use nodedb_fts::posting::QueryMode;

    use super::*;
    use crate::engine::sparse::inverted::test_support::body;

    const DB: u64 = 0;
    const T: TenantId = TenantId::new(1);

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(crate::engine::durability_gate::GatedDatabase::new(
            Database::create(&path).unwrap(),
        ));
        let idx =
            InvertedIndex::open(db, crate::data::executor::core_loop::test_governor()).unwrap();
        (idx, dir)
    }

    #[test]
    fn index_and_search() {
        let (idx, _dir) = open_temp();
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &body("The quick brown fox jumps over the lazy dog"),
        )
        .unwrap();
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(2),
            &body("A fast brown dog runs across the field"),
        )
        .unwrap();
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(3),
            &body("Rust programming language for systems"),
        )
        .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "brown fox",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, Surrogate::new(1));
    }

    #[test]
    fn search_with_stemming() {
        let (idx, _dir) = open_temp();
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &body("running distributed databases"),
        )
        .unwrap();
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(2),
            &body("the cat sat on a mat"),
        )
        .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "database distribution",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, Surrogate::new(1));
    }

    #[test]
    fn fuzzy_search() {
        let (idx, _dir) = open_temp();
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &body("distributed database systems"),
        )
        .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "databse",
                    top_k: 10,
                    fuzzy_enabled: true,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(!results.is_empty());
        assert!(results[0].fuzzy);
    }

    #[test]
    fn empty_query() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), &body("some text here"))
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "the a is",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn collections_isolated() {
        let (idx, _dir) = open_temp();
        idx.index_document(
            DB,
            T,
            "col_a",
            Surrogate::new(1),
            &body("alpha bravo charlie"),
        )
        .unwrap();
        idx.index_document(
            DB,
            T,
            "col_b",
            Surrogate::new(1),
            &body("delta echo foxtrot"),
        )
        .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "col_a",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 1);

        let results = idx
            .search(
                DB,
                T,
                "col_b",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(results.is_empty());
    }
}
