//! BM25 search over the FtsIndex with AND-first OR-fallback and phrase boost.

use std::collections::HashMap;

use crate::analyzer::pipeline::analyze;
use crate::backend::FtsBackend;
use crate::bm25::bm25_score;
use crate::index::FtsIndex;
use crate::posting::{Posting, QueryMode, TextSearchResult};
use crate::search::phrase;

impl<B: FtsBackend> FtsIndex<B> {
    /// Search the index using BM25 scoring.
    ///
    /// Uses AND-first with automatic OR-fallback: if AND yields zero results
    /// for a multi-term query, retries with OR and applies a coverage penalty
    /// of `matched_terms / total_terms` to each document's score.
    pub fn search(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
    ) -> Result<Vec<TextSearchResult>, B::Error> {
        self.search_with_mode(collection, query, top_k, fuzzy_enabled, QueryMode::And)
    }

    /// Search with explicit boolean mode (AND or OR).
    ///
    /// When `mode` is AND and a multi-term query returns zero results,
    /// automatically falls back to OR with a coverage penalty.
    pub fn search_with_mode(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy_enabled: bool,
        mode: QueryMode,
    ) -> Result<Vec<TextSearchResult>, B::Error> {
        let query_tokens = analyze(query);
        if query_tokens.is_empty() {
            return Ok(Vec::new());
        }
        let num_query_terms = query_tokens.len();

        let (total_docs, avg_doc_len) = self.index_stats(collection)?;
        if total_docs == 0 {
            return Ok(Vec::new());
        }

        // Collect posting lists for all query tokens.
        let mut term_postings: Vec<(Vec<Posting>, bool)> = Vec::with_capacity(num_query_terms);
        for token in &query_tokens {
            let exact = self.backend.read_postings(collection, token)?;
            if !exact.is_empty() {
                term_postings.push((exact, false));
            } else if fuzzy_enabled {
                let (fuzzy_posts, is_fuzzy) = self.fuzzy_lookup(collection, token)?;
                term_postings.push((fuzzy_posts, is_fuzzy));
            } else {
                term_postings.push((Vec::new(), false));
            }
        }

        // Score all documents.
        // (score, fuzzy_flag, term_match_count)
        let mut doc_scores: HashMap<String, (f32, bool, usize)> = HashMap::new();

        for (token_idx, (postings, is_fuzzy)) in term_postings.iter().enumerate() {
            if postings.is_empty() {
                continue;
            }
            let df = postings.len() as u32;

            for posting in postings {
                let doc_len = self
                    .backend
                    .read_doc_length(collection, &posting.doc_id)?
                    .unwrap_or(1);

                let mut score = bm25_score(
                    posting.term_freq,
                    df,
                    doc_len,
                    total_docs,
                    avg_doc_len,
                    &self.bm25_params,
                );

                if *is_fuzzy {
                    score *= crate::fuzzy::fuzzy_discount(1);
                }

                let entry = doc_scores
                    .entry(posting.doc_id.clone())
                    .or_insert((0.0, false, 0));
                entry.0 += score;
                if *is_fuzzy {
                    entry.1 = true;
                }
                entry.2 += 1;
            }
            let _ = token_idx; // used by phrase boost below
        }

        // Apply phrase proximity boost.
        if num_query_terms >= 2 {
            let doc_postings_map =
                phrase::collect_doc_postings(&query_tokens, &term_postings, &self.backend);
            for (doc_id, token_postings) in &doc_postings_map {
                if let Some(entry) = doc_scores.get_mut(doc_id.as_str()) {
                    let boost = phrase::phrase_boost(&query_tokens, token_postings);
                    entry.0 *= boost;
                }
            }
        }

        // AND mode with OR fallback.
        if mode == QueryMode::And && num_query_terms > 1 {
            let and_results: HashMap<String, (f32, bool, usize)> = doc_scores
                .iter()
                .filter(|(_, (_, _, match_count))| *match_count >= num_query_terms)
                .map(|(k, v)| (k.clone(), *v))
                .collect();

            if !and_results.is_empty() {
                return Ok(Self::to_sorted_results(and_results, top_k));
            }

            // AND returned nothing — fall back to OR with coverage penalty.
            for (score, _, match_count) in doc_scores.values_mut() {
                let coverage = *match_count as f32 / num_query_terms as f32;
                *score *= coverage;
            }
        }

        Ok(Self::to_sorted_results(doc_scores, top_k))
    }

    /// Convert score map to sorted, truncated results.
    fn to_sorted_results(
        doc_scores: HashMap<String, (f32, bool, usize)>,
        top_k: usize,
    ) -> Vec<TextSearchResult> {
        let mut results: Vec<TextSearchResult> = doc_scores
            .into_iter()
            .map(|(doc_id, (score, fuzzy_flag, _))| TextSearchResult {
                doc_id,
                score,
                fuzzy: fuzzy_flag,
            })
            .collect();
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::posting::QueryMode;

    fn make_index() -> FtsIndex<MemoryBackend> {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "The quick brown fox jumps over the lazy dog")
            .unwrap();
        idx.index_document("docs", "d2", "A fast brown dog runs across the field")
            .unwrap();
        idx.index_document("docs", "d3", "Rust programming language for systems")
            .unwrap();
        idx
    }

    #[test]
    fn basic_search() {
        let idx = make_index();
        let results = idx.search("docs", "brown fox", 10, false).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn search_with_stemming() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "running distributed databases")
            .unwrap();
        idx.index_document("docs", "d2", "the cat sat on a mat")
            .unwrap();

        let results = idx
            .search("docs", "database distribution", 10, false)
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn or_mode() {
        let idx = make_index();
        let results = idx
            .search_with_mode("docs", "brown fox", 10, false, QueryMode::Or)
            .unwrap();
        assert!(results.len() >= 2);
    }

    #[test]
    fn and_mode_filters() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "Rust programming language")
            .unwrap();
        idx.index_document("docs", "d2", "Python programming language")
            .unwrap();

        let results = idx
            .search_with_mode("docs", "rust programming", 10, false, QueryMode::And)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn and_fallback_to_or() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "rust programming language")
            .unwrap();
        idx.index_document("docs", "d2", "python programming language")
            .unwrap();

        // "rust python" — no doc has BOTH, AND yields nothing, falls back to OR.
        let results = idx.search("docs", "rust python", 10, false).unwrap();
        assert_eq!(results.len(), 2);
        // Coverage penalty: each doc matches 1/2 terms → scores penalized by 0.5.
        for r in &results {
            assert!(r.score > 0.0);
        }
    }

    #[test]
    fn and_no_fallback_when_results_exist() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "rust programming language")
            .unwrap();
        idx.index_document("docs", "d2", "python programming language")
            .unwrap();

        // "rust programming" — d1 has both, AND succeeds, no fallback.
        let results = idx.search("docs", "rust programming", 10, false).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn empty_query() {
        let idx = make_index();
        let results = idx.search("docs", "the a is", 10, false).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn collections_isolated() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("col_a", "d1", "alpha bravo charlie")
            .unwrap();
        idx.index_document("col_b", "d1", "delta echo foxtrot")
            .unwrap();

        assert_eq!(idx.search("col_a", "alpha", 10, false).unwrap().len(), 1);
        assert!(idx.search("col_b", "alpha", 10, false).unwrap().is_empty());
    }

    #[test]
    fn fuzzy_search() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "distributed database systems")
            .unwrap();

        let results = idx.search("docs", "databse", 10, true).unwrap();
        assert!(!results.is_empty());
        assert!(results[0].fuzzy);
    }

    #[test]
    fn phrase_boost_consecutive() {
        let idx = FtsIndex::new(MemoryBackend::new());
        // d1 has "brown fox" as consecutive tokens.
        idx.index_document("docs", "d1", "the quick brown fox jumped")
            .unwrap();
        // d2 has "brown" and "fox" but separated.
        idx.index_document("docs", "d2", "a brown dog chased a fox")
            .unwrap();

        let results = idx
            .search_with_mode("docs", "brown fox", 10, false, QueryMode::Or)
            .unwrap();
        assert!(results.len() >= 2);
        // d1 should rank higher due to phrase boost.
        assert_eq!(results[0].doc_id, "d1");
    }

    #[test]
    fn phrase_boost_no_effect_single_term() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document("docs", "d1", "hello world").unwrap();

        let results = idx.search("docs", "hello", 10, false).unwrap();
        assert_eq!(results.len(), 1);
    }
}
