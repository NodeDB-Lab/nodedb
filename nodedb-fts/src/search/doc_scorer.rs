// SPDX-License-Identifier: Apache-2.0

//! Per-document `bm25_score(field, query)` values.
//!
//! A [`DocScorer`] resolves its query once, then scores any document by
//! point lookups: the document's postings of the query terms, and its
//! recorded length for index membership. It reads no corpus-wide score map,
//! so scoring the rows a query emits costs memory in the query terms'
//! postings, never in the collection.
//!
//! The score is the one a search of the same query returns for the
//! document, under the same match-mode decision over the same admitted rows.

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::backend::FtsBackend;
use crate::index::FtsIndex;
use crate::index::error::FtsIndexError;
use crate::scope::IndexScope;
use crate::search::doc_score::term_score;
use crate::search::match_mode::{MatchMode, staged_candidates};
use crate::search::query_terms::{ResolvedQuery, TextQuery};
use crate::search::staged::{StagedView, staged_contributions};

/// One document's score against one index.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DocScore {
    /// The document matches the query with this score.
    Match(f32),
    /// The index holds the document, and the query does not match it.
    Miss,
    /// The index does not hold the document.
    Absent,
}

/// Scores documents against one resolved query.
pub struct DocScorer<'a, B: FtsBackend> {
    fts: &'a FtsIndex<B>,
    database_id: u64,
    tid: u64,
    index: IndexScope<'a>,
    staged: Option<StagedView>,
    /// `None` when the query has no positive term: it matches nothing.
    resolved: Option<(ResolvedQuery, MatchMode)>,
}

impl<B: FtsBackend> FtsIndex<B> {
    /// A scorer of `query` against `index`. `eligible` is the set of rows the
    /// reading query admits: the AND-mode fallback is decided over it, the
    /// same way a search with that prefilter decides it. `staged` is the
    /// open transaction's view of the index.
    pub fn doc_scorer<'a>(
        &'a self,
        database_id: u64,
        tid: u64,
        index: impl Into<IndexScope<'a>>,
        query: TextQuery<'_>,
        eligible: Option<&SurrogateBitmap>,
        staged: Option<StagedView>,
    ) -> Result<DocScorer<'a, B>, FtsIndexError<B::Error>> {
        let index = index.into();
        let resolved = match self.resolve_query(database_id, tid, index, query, staged.as_ref())? {
            Some(resolved) => {
                let mut deny = resolved.negated.clone();
                if let Some(view) = staged.as_ref() {
                    deny.union_in_place(view.hidden());
                }
                let index_visible = staged.as_ref().is_none_or(|view| !view.hides_all());
                let candidates =
                    staged_candidates(&resolved, staged.as_ref(), eligible, &self.bm25_params);
                let mode = resolved.match_mode(index_visible, eligible, &deny, &candidates);
                Some((resolved, mode))
            }
            None => None,
        };
        Ok(DocScorer {
            fts: self,
            database_id,
            tid,
            index,
            staged,
            resolved,
        })
    }
}

impl<B: FtsBackend> DocScorer<'_, B> {
    /// The score of each of `docs`, parallel to `docs`. Index membership of
    /// the documents that do not match is read in one batch.
    pub fn score(&self, docs: &[Surrogate]) -> Result<Vec<DocScore>, FtsIndexError<B::Error>> {
        let mut scores = vec![DocScore::Absent; docs.len()];
        let mut membership: Vec<(usize, Surrogate)> = Vec::new();
        for (slot, doc_id) in docs.iter().enumerate() {
            if let Some(view) = self.staged.as_ref() {
                if let Some(doc) = view.doc(*doc_id) {
                    scores[slot] = self
                        .staged_match(doc)
                        .map_or(DocScore::Miss, DocScore::Match);
                    continue;
                }
                if view.hides(*doc_id) {
                    continue;
                }
            }
            match self.indexed_match(*doc_id) {
                Some(score) => scores[slot] = DocScore::Match(score),
                None => membership.push((slot, *doc_id)),
            }
        }
        if membership.is_empty() {
            return Ok(scores);
        }
        let ids: Vec<Surrogate> = membership.iter().map(|(_, doc_id)| *doc_id).collect();
        let lengths = self
            .fts
            .backend
            .read_doc_lengths(self.database_id, self.tid, self.index, &ids)
            .map_err(FtsIndexError::backend)?;
        for ((slot, _), length) in membership.into_iter().zip(lengths) {
            if length.is_some() {
                scores[slot] = DocScore::Miss;
            }
        }
        Ok(scores)
    }

    fn staged_match(&self, doc: &crate::search::staged::StagedDoc) -> Option<f32> {
        let (query, mode) = self.resolved.as_ref()?;
        let contributions = staged_contributions(query, doc, &self.fts.bm25_params)?;
        mode.score(query, &contributions)
    }

    fn indexed_match(&self, doc_id: Surrogate) -> Option<f32> {
        let (query, mode) = self.resolved.as_ref()?;
        if query.negated.contains(doc_id) {
            return None;
        }
        let params = &self.fts.bm25_params;
        let contributions: Vec<Option<f32>> = query
            .blocks
            .iter()
            .map(|blocks| {
                blocks.lookup(doc_id).map(|(tf, fieldnorm)| {
                    term_score(
                        crate::bm25::idf(blocks.df, query.total_docs),
                        tf,
                        fieldnorm,
                        query.avg_doc_len,
                        params,
                    )
                })
            })
            .collect();
        mode.score(query, &contributions)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{Surrogate, SurrogateBitmap};

    use super::DocScore;
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::posting::QueryMode;
    use crate::search::bm25_search::FtsSearchParams;
    use crate::search::query_terms::TextQuery;
    use crate::search::staged::{StagedDoc, StagedView};
    use crate::test_support::test_governor;

    const DB: u64 = 0;
    const T: u64 = 1;

    fn query(text: &str) -> TextQuery<'_> {
        TextQuery {
            query: text,
            fuzzy_enabled: false,
            mode: QueryMode::And,
        }
    }

    /// A document's point score equals the score a search returns for it.
    #[test]
    fn point_scores_equal_search_scores() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", Surrogate(1), "rust systems language")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(2), "rust rust web")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(3), "python scripting")
            .unwrap();

        let hits = idx
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
        let scorer = idx
            .doc_scorer(DB, T, "docs", query("rust"), None, None)
            .unwrap();
        let docs: Vec<Surrogate> = hits.iter().map(|h| h.doc_id).collect();
        let scores = scorer.score(&docs).unwrap();
        for (hit, score) in hits.iter().zip(scores) {
            assert_eq!(score, DocScore::Match(hit.score));
        }
        assert_eq!(
            scorer.score(&[Surrogate(3), Surrogate(99)]).unwrap(),
            vec![DocScore::Miss, DocScore::Absent]
        );
    }

    /// The fallback decision reads the eligible rows: with the only AND
    /// match outside them, a one-word match scores under OR fallback.
    #[test]
    fn fallback_follows_the_eligible_rows() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", Surrogate(1), "alpha bravo")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(2), "alpha charlie")
            .unwrap();

        let all = idx
            .doc_scorer(DB, T, "docs", query("alpha bravo"), None, None)
            .unwrap();
        assert_eq!(all.score(&[Surrogate(2)]).unwrap(), vec![DocScore::Miss]);

        let mut eligible = SurrogateBitmap::new();
        eligible.insert(Surrogate(2));
        let scoped = idx
            .doc_scorer(DB, T, "docs", query("alpha bravo"), Some(&eligible), None)
            .unwrap();
        assert!(matches!(
            scoped.score(&[Surrogate(2)]).unwrap()[0],
            DocScore::Match(_)
        ));
    }

    /// A staged document scores from its tokens; a hidden indexed document
    /// the transaction removed is absent.
    #[test]
    fn staged_documents_score_and_hidden_documents_are_absent() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.index_document(DB, T, "docs", Surrogate(1), "rust language")
            .unwrap();
        let mut hidden = SurrogateBitmap::new();
        hidden.insert(Surrogate(1));
        let view = StagedView::new(
            hidden,
            false,
            vec![StagedDoc {
                doc_id: Surrogate(2),
                tokens: vec!["rust".into()],
            }],
        );
        let scorer = idx
            .doc_scorer(DB, T, "docs", query("rust"), None, Some(view))
            .unwrap();
        let scores = scorer.score(&[Surrogate(1), Surrogate(2)]).unwrap();
        assert_eq!(scores[0], DocScore::Absent);
        assert!(matches!(scores[1], DocScore::Match(_)));
    }
}
