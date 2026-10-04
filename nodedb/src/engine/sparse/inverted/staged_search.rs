// SPDX-License-Identifier: BUSL-1.1

//! Transaction-aware BM25 reads: a ranked search that folds an open
//! transaction's staged documents in before the top-k cut, and per-document
//! score columns read by point lookups.

use nodedb_fts::posting::TextSearchResult;
use nodedb_fts::{DocScore, DocScorer, FtsSearchParams, IndexScope, StagedView, TextQuery};
use nodedb_types::{Surrogate, SurrogateBitmap, TenantId};

use super::core::InvertedIndex;
use super::errors::fts_index_err;
use crate::engine::sparse::fts_redb::RedbFtsBackend;

/// Scores documents against one query on one index.
pub struct TextDocScorer<'a> {
    inner: DocScorer<'a, RedbFtsBackend>,
}

impl TextDocScorer<'_> {
    /// The score of each of `docs`, parallel to `docs`.
    pub fn score(&self, docs: &[Surrogate]) -> crate::Result<Vec<DocScore>> {
        self.inner.score(docs).map_err(fts_index_err)
    }
}

impl InvertedIndex {
    /// BM25 search with `staged`, the issuing transaction's view of the
    /// index, folded in before the top-k cut.
    pub fn search_staged<'a>(
        &self,
        database_id: u64,
        tid: TenantId,
        index: impl Into<IndexScope<'a>>,
        params: FtsSearchParams<'_>,
        staged: Option<&StagedView>,
    ) -> crate::Result<Vec<TextSearchResult>> {
        self.inner
            .search_staged(database_id, tid.as_u64(), index, params, staged)
            .map_err(fts_index_err)
    }

    /// A per-document scorer of `query` on `index`. `eligible` is the set of
    /// rows the reading query admits, over which the AND-mode fallback is
    /// decided.
    pub fn doc_scorer<'a>(
        &'a self,
        database_id: u64,
        tid: TenantId,
        index: impl Into<IndexScope<'a>>,
        query: TextQuery<'_>,
        eligible: Option<&SurrogateBitmap>,
        staged: Option<StagedView>,
    ) -> crate::Result<TextDocScorer<'a>> {
        self.inner
            .doc_scorer(database_id, tid.as_u64(), index, query, eligible, staged)
            .map(|inner| TextDocScorer { inner })
            .map_err(fts_index_err)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_fts::posting::QueryMode;
    use nodedb_fts::{DocScore, FtsSearchParams, StagedDoc, StagedView, TextQuery};
    use nodedb_types::{Surrogate, SurrogateBitmap, TenantId};

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

    /// Search scores read from the redb posting table equal the point scores
    /// of the same documents, and a document with no text is absent.
    #[test]
    fn redb_point_scores_equal_search_scores() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), &body("rust systems rust"))
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), &body("rust web"))
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(3), &body("python"))
            .unwrap();

        let hits = idx
            .search_staged(
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
                None,
            )
            .unwrap();
        assert_eq!(hits.len(), 2);
        let scorer = idx
            .doc_scorer(
                DB,
                T,
                "docs",
                TextQuery {
                    query: "rust",
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                },
                None,
                None,
            )
            .unwrap();
        let docs: Vec<Surrogate> = hits.iter().map(|h| h.doc_id).collect();
        for (hit, score) in hits.iter().zip(scorer.score(&docs).unwrap()) {
            assert_eq!(score, DocScore::Match(hit.score));
        }
        assert_eq!(
            scorer
                .score(&[Surrogate::new(3), Surrogate::new(4)])
                .unwrap(),
            vec![DocScore::Miss, DocScore::Absent]
        );
    }

    /// A staged update that removes the top hit leaves the next one inside
    /// the limit.
    #[test]
    fn staged_removal_of_the_top_hit_keeps_the_limit_full() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), &body("rust rust rust"))
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), &body("rust lang"))
            .unwrap();
        let mut hidden = SurrogateBitmap::new();
        hidden.insert(Surrogate::new(1));
        let view = StagedView::new(
            hidden,
            false,
            vec![StagedDoc {
                doc_id: Surrogate::new(1),
                tokens: vec!["golang".into()],
            }],
        );
        let hits = idx
            .search_staged(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 1,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
                Some(&view),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, Surrogate::new(2));
    }
}
