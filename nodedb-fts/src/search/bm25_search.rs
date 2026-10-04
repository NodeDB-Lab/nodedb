// SPDX-License-Identifier: Apache-2.0

//! BM25 search over the FtsIndex with AND-first OR-fallback and NOT-term
//! exclusion.
//!
//! ## AND-first / OR-fallback
//!
//! A multi-word query `rust programming` in AND mode matches documents
//! holding every word (a word matches through any of its synonyms). The
//! top-k is the true top-k of those documents, whatever `top_k` is. When no
//! admitted document holds every word, the query falls back to OR with
//! coverage-scaled scores.
//!
//! ## NOT operator
//!
//! `rust NOT python` and `rust -python` are equivalent. The query parser
//! splits the input into positive and negative term lists. BM25 scoring runs
//! on positive terms only. A document holding any negative term is excluded
//! before scoring, so the top-k cut counts only surviving documents.
//! Negative terms do not affect BM25 scores.
//!
//! Synonym expansion applies to both positive and negative term lists, so
//! `rust NOT db` also excludes documents that contain synonym expansions of
//! `db` (e.g. `database`, `datastore`).

use nodedb_types::SurrogateBitmap;

use crate::backend::FtsBackend;
use crate::index::FtsIndex;
use crate::index::error::FtsIndexError;
use crate::posting::{QueryMode, TextSearchResult};
use crate::scope::IndexScope;
use crate::search::bmw::scorer::{BmwInput, bmw_score};
use crate::search::match_mode::staged_candidates;
use crate::search::query_parser::parse_query;
use crate::search::query_terms::TextQuery;
use crate::search::staged::StagedView;

/// Query and tuning parameters for a BM25 search.
///
/// The `(database_id, tid, index)` scope is passed separately so the
/// same struct can be shared by callers that hold the tenant id as either a
/// raw `u64` (this crate) or a strongly-typed `TenantId` (the Origin wrapper).
pub struct FtsSearchParams<'a> {
    /// Raw query string (may contain `NOT <term>` / `-<term>` negation).
    pub query: &'a str,
    /// Maximum number of results to return.
    pub top_k: usize,
    /// When `true`, unmatched terms fall back to fuzzy (Levenshtein) lookup.
    pub fuzzy_enabled: bool,
    /// Boolean combination mode for multi-term queries (AND or OR).
    pub mode: QueryMode,
    /// Optional surrogate bitmap restricting the candidate set before scoring.
    pub prefilter: Option<&'a SurrogateBitmap>,
}

impl<B: FtsBackend> FtsIndex<B> {
    /// Search one index with explicit boolean mode, fuzzy, and optional prefilter.
    ///
    /// Analyzer, fuzzy default, and synonyms come from the index's
    /// collection. Postings and BM25 stats come from the index itself.
    ///
    /// Supports `NOT <term>` and `-<term>` negation in the query string.
    /// Returns `Err(FtsIndexError::InvalidQuery)` for ill-formed queries such
    /// as NOT-only queries or unsupported parenthesised groups.
    pub fn search<'a>(
        &self,
        database_id: u64,
        tid: u64,
        index: impl Into<IndexScope<'a>>,
        params: FtsSearchParams<'_>,
    ) -> Result<Vec<TextSearchResult>, FtsIndexError<B::Error>> {
        self.search_staged(database_id, tid, index, params, None)
    }

    /// [`Self::search`] inside an open transaction: indexed documents the
    /// transaction hides do not match, and its staged documents compete in
    /// the same ranking under the same query semantics. `prefilter` bounds
    /// staged documents too.
    ///
    /// Results are ordered by score descending, then surrogate ascending.
    pub fn search_staged<'a>(
        &self,
        database_id: u64,
        tid: u64,
        index: impl Into<IndexScope<'a>>,
        params: FtsSearchParams<'_>,
        staged: Option<&StagedView>,
    ) -> Result<Vec<TextSearchResult>, FtsIndexError<B::Error>> {
        let index = index.into();
        let FtsSearchParams {
            query,
            top_k,
            fuzzy_enabled,
            mode,
            prefilter,
        } = params;
        if top_k == 0 {
            // The query is still parsed: an ill-formed one is an error at
            // any limit.
            parse_query(query)?;
            return Ok(Vec::new());
        }
        let text_query = TextQuery {
            query,
            fuzzy_enabled,
            mode,
        };
        let Some(resolved) = self.resolve_query(database_id, tid, index, text_query, staged)?
        else {
            return Ok(Vec::new());
        };

        let mut deny = resolved.negated.clone();
        if let Some(view) = staged {
            deny.union_in_place(view.hidden());
        }
        let index_visible = staged.is_none_or(|view| !view.hides_all());
        let staged_docs = staged_candidates(&resolved, staged, prefilter, &self.bm25_params);
        let match_mode = resolved.match_mode(index_visible, prefilter, &deny, &staged_docs);
        let allow = match &match_mode {
            super::match_mode::MatchMode::All(docs) => Some(docs),
            super::match_mode::MatchMode::Coverage | super::match_mode::MatchMode::Any => prefilter,
        };

        let mut hits: Vec<TextSearchResult> = Vec::new();
        if index_visible {
            let heap = bmw_score(&BmwInput {
                terms: &resolved.blocks,
                term_groups: &resolved.term_groups,
                groups: resolved.groups,
                total_docs: resolved.total_docs,
                avg_doc_len: resolved.avg_doc_len,
                params: &self.bm25_params,
                top_k,
                allow,
                deny: Some(&deny),
                combine: match_mode.combine(),
            });
            hits.extend(heap.into_sorted().into_iter().map(|doc| TextSearchResult {
                doc_id: doc.doc_id,
                score: doc.score,
                fuzzy: resolved.fuzzy,
            }));
        }
        for (doc_id, contributions) in &staged_docs {
            if let Some(score) = match_mode.score(&resolved, contributions) {
                hits.push(TextSearchResult {
                    doc_id: *doc_id,
                    score,
                    fuzzy: resolved.fuzzy,
                });
            }
        }
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.doc_id.cmp(&b.doc_id))
        });
        hits.truncate(top_k);
        Ok(hits)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{Surrogate, SurrogateBitmap};

    use super::FtsSearchParams;
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::index::error::FtsIndexError;
    use crate::posting::QueryMode;
    use crate::search::query_parser::InvalidQuery;
    use crate::search::staged::StagedDoc;
    use crate::test_support::test_governor;

    const DB: u64 = 0;
    const T: u64 = 1;
    const D1: Surrogate = Surrogate(1);
    const D2: Surrogate = Surrogate(2);
    const D3: Surrogate = Surrogate(3);

    fn make_index() -> FtsIndex<MemoryBackend> {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(
            DB,
            T,
            "docs",
            D1,
            "The quick brown fox jumps over the lazy dog",
        )
        .unwrap();
        idx.index_document(DB, T, "docs", D2, "A fast brown dog runs across the field")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "Rust programming language for systems")
            .unwrap();
        idx
    }

    #[test]
    fn basic_search() {
        let idx = make_index();
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
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn search_with_stemming() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "running distributed databases")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "the cat sat on a mat")
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
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn or_mode() {
        let idx = make_index();
        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "brown fox",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::Or,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(results.len() >= 2);
    }

    #[test]
    fn and_mode_filters() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "Rust programming language")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "Python programming language")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust programming",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn and_fallback_to_or() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "rust programming language")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "python programming language")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust python",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        for r in &results {
            assert!(r.score > 0.0);
        }
    }

    #[test]
    fn and_no_fallback_when_results_exist() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "rust programming language")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "python programming language")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust programming",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn empty_query() {
        let idx = make_index();
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
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "col_a", D1, "alpha bravo charlie")
            .unwrap();
        idx.index_document(DB, T, "col_b", D1, "delta echo foxtrot")
            .unwrap();

        assert_eq!(
            idx.search(
                DB,
                T,
                "col_a",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None
                }
            )
            .unwrap()
            .len(),
            1
        );
        assert!(
            idx.search(
                DB,
                T,
                "col_b",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None
                }
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn fuzzy_search() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "distributed database systems")
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
    fn phrase_boost_consecutive() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "the quick brown fox jumped")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "a brown dog chased a fox")
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
                    mode: QueryMode::Or,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(results.len() >= 2);
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn phrase_boost_no_effect_single_term() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "hello world")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "hello",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn tenants_isolated() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, 1, "docs", D1, "alpha bravo")
            .unwrap();
        idx.index_document(DB, 2, "docs", D1, "charlie delta")
            .unwrap();

        let r1 = idx
            .search(
                DB,
                1,
                "docs",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        let r2 = idx
            .search(
                DB,
                2,
                "docs",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(r1.len(), 1);
        assert!(r2.is_empty());
    }

    #[test]
    fn prefilter_excludes_non_member_surrogates() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());

        idx.index_document(DB, T, "docs", D1, "rust language system")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust rust rust rust rust")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "rust rust rust rust rust rust")
            .unwrap();

        let mut bm = SurrogateBitmap::new();
        bm.insert(D1);

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: Some(&bm),
                },
            )
            .unwrap();

        assert_eq!(results.len(), 1, "only D1 should be returned");
        assert_eq!(results[0].doc_id, D1);

        assert!(
            !results.iter().any(|r| r.doc_id == D2),
            "D2 must be excluded"
        );
        assert!(
            !results.iter().any(|r| r.doc_id == D3),
            "D3 must be excluded"
        );

        let all_results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(all_results.len(), 3, "all docs returned without prefilter");
        assert!(
            all_results[0].doc_id == D2 || all_results[0].doc_id == D3,
            "D2 or D3 should lead without prefilter (higher tf)"
        );

        let empty_bm = SurrogateBitmap::new();
        let empty_results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: Some(&empty_bm),
                },
            )
            .unwrap();
        assert!(empty_results.is_empty(), "empty prefilter → no results");

        let mut bm23 = SurrogateBitmap::new();
        bm23.insert(D2);
        bm23.insert(D3);
        let results23 = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: Some(&bm23),
                },
            )
            .unwrap();
        assert_eq!(results23.len(), 2);
        assert!(!results23.iter().any(|r| r.doc_id == D1));
    }

    // ── NOT operator tests ────────────────────────────────────────────────────

    #[test]
    fn not_keyword_excludes_documents() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        // D1: rust + python, D2: rust + ruby, D3: python + ruby
        idx.index_document(DB, T, "docs", D1, "rust python programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust ruby programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "python ruby programming")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust NOT python",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        // Must include D2 (rust, no python), must not include D1 (has python).
        assert!(
            results.iter().any(|r| r.doc_id == D2),
            "D2 (rust+ruby) must be in results"
        );
        assert!(
            !results.iter().any(|r| r.doc_id == D1),
            "D1 (rust+python) must be excluded"
        );
    }

    #[test]
    fn dash_prefix_excludes_documents() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "rust python programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust ruby programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "python ruby programming")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust -python",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(results.iter().any(|r| r.doc_id == D2));
        assert!(!results.iter().any(|r| r.doc_id == D1));
    }

    #[test]
    fn multiple_not_excludes_all_negated() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "rust python programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust ruby programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "rust systems programming")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust NOT python NOT ruby",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        // Only D3 has neither python nor ruby.
        assert!(results.iter().any(|r| r.doc_id == D3));
        assert!(!results.iter().any(|r| r.doc_id == D1));
        assert!(!results.iter().any(|r| r.doc_id == D2));
    }

    #[test]
    fn not_nonexistent_term_returns_all_positives() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "rust programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust systems")
            .unwrap();

        let results_plain = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        let results_not = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust NOT nonexistentxyz",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();

        let plain_ids: std::collections::HashSet<Surrogate> =
            results_plain.iter().map(|r| r.doc_id).collect();
        let not_ids: std::collections::HashSet<Surrogate> =
            results_not.iter().map(|r| r.doc_id).collect();
        assert_eq!(
            plain_ids, not_ids,
            "NOT with nonexistent term must not remove any docs"
        );
    }

    #[test]
    fn negative_only_returns_invalid_query_error() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "python programming")
            .unwrap();

        let err = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "NOT python",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap_err();
        assert!(
            matches!(err, FtsIndexError::InvalidQuery(InvalidQuery::NegativeOnly)),
            "expected InvalidQuery(NegativeOnly), got {err:?}"
        );
    }

    #[test]
    fn parentheses_after_not_returns_invalid_query_error() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "rust programming")
            .unwrap();

        let err = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust NOT (python OR ruby)",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                FtsIndexError::InvalidQuery(InvalidQuery::ParenthesesNotSupported)
            ),
            "expected InvalidQuery(ParenthesesNotSupported), got {err:?}"
        );
    }

    // ── top-k semantics ───────────────────────────────────────────────────────

    fn ranked(
        idx: &FtsIndex<MemoryBackend>,
        query: &str,
        top_k: usize,
        mode: QueryMode,
        prefilter: Option<&SurrogateBitmap>,
        staged: Option<&super::StagedView>,
    ) -> Vec<Surrogate> {
        idx.search_staged(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query,
                top_k,
                fuzzy_enabled: false,
                mode,
                prefilter,
            },
            staged,
        )
        .unwrap()
        .into_iter()
        .map(|r| r.doc_id)
        .collect()
    }

    /// The best `rust` documents hold `python`. Negation drops them before
    /// the cut, so `LIMIT 1` still returns the surviving document.
    #[test]
    fn not_terms_are_excluded_before_the_limit() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "rust rust rust python")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust rust rust python")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "rust golang compiler toolchain")
            .unwrap();
        assert_eq!(
            ranked(&idx, "rust -python", 1, QueryMode::And, None, None),
            vec![D3]
        );
        assert_eq!(
            ranked(&idx, "rust -python", 2, QueryMode::Or, None, None),
            vec![D3]
        );
    }

    /// Many documents out-score the single AND match on one word. The AND
    /// match is found whatever the limit, and the query does not fall back.
    #[test]
    fn and_match_is_found_past_many_single_word_out_scorers() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        for i in 1..=40u32 {
            idx.index_document(DB, T, "docs", Surrogate(i), "alpha alpha alpha alpha")
                .unwrap();
        }
        for i in 41..=80u32 {
            idx.index_document(DB, T, "docs", Surrogate(i), "bravo bravo bravo bravo")
                .unwrap();
        }
        let both = Surrogate(81);
        idx.index_document(DB, T, "docs", both, "alpha bravo filler words here")
            .unwrap();
        for limit in [1, 3, 10, usize::MAX] {
            assert_eq!(
                ranked(&idx, "alpha bravo", limit, QueryMode::And, None, None),
                vec![both],
                "limit {limit}"
            );
        }
    }

    /// The AND decision reads only the admitted rows. An AND match outside
    /// the prefilter does not stop the fallback inside it, and an AND match
    /// inside it keeps AND semantics.
    #[test]
    fn and_mode_inside_a_prefilter() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "alpha bravo")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "alpha charlie")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "bravo delta")
            .unwrap();

        let mut without_match = SurrogateBitmap::new();
        without_match.insert(D2);
        without_match.insert(D3);
        let mut fallback = ranked(
            &idx,
            "alpha bravo",
            10,
            QueryMode::And,
            Some(&without_match),
            None,
        );
        fallback.sort();
        assert_eq!(fallback, vec![D2, D3], "no admitted AND match: OR fallback");

        let mut with_match = without_match.clone();
        with_match.insert(D1);
        assert_eq!(
            ranked(
                &idx,
                "alpha bravo",
                10,
                QueryMode::And,
                Some(&with_match),
                None
            ),
            vec![D1],
            "an admitted AND match keeps AND semantics"
        );
    }

    /// A staged update that removes the best hit leaves the limit filled by
    /// the next document, and a staged document competes in the ranking.
    #[test]
    fn staged_rows_are_ranked_before_the_cut() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "rust rust rust rust")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust rust lang")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "rust tooling compiler words")
            .unwrap();

        let mut hidden = SurrogateBitmap::new();
        hidden.insert(D1);
        let removed_top = super::StagedView::new(
            hidden,
            false,
            vec![StagedDoc {
                doc_id: D1,
                tokens: vec!["python".into()],
            }],
        );
        assert_eq!(
            ranked(&idx, "rust", 2, QueryMode::And, None, Some(&removed_top)),
            vec![D2, D3]
        );

        let new_best = super::StagedView::new(
            SurrogateBitmap::new(),
            false,
            vec![StagedDoc {
                doc_id: Surrogate(9),
                tokens: vec!["rust".into(); 6],
            }],
        );
        assert_eq!(
            ranked(&idx, "rust", 1, QueryMode::And, None, Some(&new_best))[0],
            Surrogate(9)
        );
    }

    /// A staged document is scored with AND semantics: holding one of two
    /// words does not match while an AND match exists.
    #[test]
    fn staged_rows_use_and_semantics() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", D1, "alpha bravo")
            .unwrap();
        let view = super::StagedView::new(
            SurrogateBitmap::new(),
            false,
            vec![
                StagedDoc {
                    doc_id: Surrogate(8),
                    tokens: vec!["alpha".into(), "alpha".into()],
                },
                StagedDoc {
                    doc_id: Surrogate(9),
                    tokens: vec!["alpha".into(), "bravo".into()],
                },
            ],
        );
        let mut hits = ranked(&idx, "alpha bravo", 10, QueryMode::And, None, Some(&view));
        hits.sort();
        assert_eq!(hits, vec![D1, Surrogate(9)]);

        let negated = ranked(&idx, "alpha -bravo", 10, QueryMode::And, None, Some(&view));
        assert_eq!(negated, vec![Surrogate(8)]);
    }
}
