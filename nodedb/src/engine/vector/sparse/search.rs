// SPDX-License-Identifier: BUSL-1.1

//! Sparse vector dot-product search with top-K selection.
//!
//! Iterates query dimensions, looks up each in the inverted index's posting
//! lists, and accumulates per-document scores. Returns the top-K documents
//! by descending score via a bounded min-heap.

use std::collections::{BinaryHeap, HashMap};

use nodedb_types::SparseVector;

use super::index::SparseInvertedIndex;

/// A scored result from sparse vector search.
#[derive(Debug, Clone)]
pub struct SparseSearchResult {
    /// Internal document ID in the sparse index.
    pub internal_id: u32,
    /// Dot-product score: Σ q[d] * doc[d] for shared dimensions.
    pub score: f32,
    /// Resolved string document ID (if available).
    pub doc_id: Option<String>,
}

/// Search the sparse inverted index for documents most similar to the query.
///
/// Computes dot-product scores by iterating query dimensions and accumulating
/// weights from the live posting lists. Returns top-K results sorted by
/// score descending.
///
/// Complexity: O(Σ postings_length for query dimensions + K log K).
pub fn dot_product_topk(
    index: &SparseInvertedIndex,
    query: &SparseVector,
    top_k: usize,
) -> Vec<SparseSearchResult> {
    if query.is_empty() || index.is_empty() || top_k == 0 {
        return Vec::new();
    }
    let mut scores: HashMap<u32, f32> = HashMap::new();
    for &(dim, q_weight) in query.entries() {
        if let Some(postings) = index.get_postings(dim) {
            for &(doc_id, doc_weight) in postings {
                *scores.entry(doc_id).or_insert(0.0) += q_weight * doc_weight;
            }
        }
    }
    top_k_of(index, &scores, top_k)
}

/// [`dot_product_topk`] over the versions visible at system-time ordinal
/// `at`: the live versions started at or below `at`, and the ended versions
/// whose span holds `at`.
pub fn dot_product_topk_at(
    index: &SparseInvertedIndex,
    query: &SparseVector,
    top_k: usize,
    at: i64,
) -> Vec<SparseSearchResult> {
    if query.is_empty() || top_k == 0 {
        return Vec::new();
    }
    let history = index.history();
    let mut scores: HashMap<u32, f32> = HashMap::new();
    for &(dim, q_weight) in query.entries() {
        if let Some(postings) = index.get_postings(dim) {
            for &(id, doc_weight) in postings {
                if history.live_visible_at(id, at) {
                    *scores.entry(id).or_insert(0.0) += q_weight * doc_weight;
                }
            }
        }
        for &(id, doc_weight) in history.ended_postings(dim) {
            if history.ended_visible_at(id, at) {
                *scores.entry(id).or_insert(0.0) += q_weight * doc_weight;
            }
        }
    }
    top_k_of(index, &scores, top_k)
}

/// The `top_k` highest of `scores`, best first, with each id resolved.
fn top_k_of(
    index: &SparseInvertedIndex,
    scores: &HashMap<u32, f32>,
    top_k: usize,
) -> Vec<SparseSearchResult> {
    if scores.is_empty() {
        return Vec::new();
    }

    // Top-K selection via min-heap bounded to K entries.
    let mut heap: BinaryHeap<std::cmp::Reverse<HeapEntry>> = BinaryHeap::with_capacity(top_k + 1);

    for (doc_id, score) in scores {
        heap.push(std::cmp::Reverse(HeapEntry {
            score: *score,
            doc_id: *doc_id,
        }));
        if heap.len() > top_k {
            heap.pop(); // Remove smallest.
        }
    }

    // `into_sorted_vec` on `BinaryHeap<Reverse<T>>` returns ascending `Reverse`
    // order, which is descending actual score order.
    heap.into_sorted_vec()
        .into_iter()
        .map(|std::cmp::Reverse(entry)| SparseSearchResult {
            internal_id: entry.doc_id,
            score: entry.score,
            doc_id: index.resolve_doc_id(entry.doc_id).map(String::from),
        })
        .collect()
}

/// Min-heap entry: ordered by score ascending so the heap root is the minimum.
#[derive(Debug)]
struct HeapEntry {
    score: f32,
    doc_id: u32,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.doc_id == other.doc_id
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.doc_id.cmp(&other.doc_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sv(entries: &[(u32, f32)]) -> SparseVector {
        SparseVector::from_entries(entries.to_vec()).unwrap()
    }

    #[test]
    fn basic_search() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("doc1", &make_sv(&[(10, 0.5), (20, 0.8)]));
        idx.insert("doc2", &make_sv(&[(10, 0.3), (30, 1.0)]));
        idx.insert("doc3", &make_sv(&[(20, 0.9), (30, 0.2)]));

        let query = make_sv(&[(10, 1.0), (20, 1.0)]);
        let results = dot_product_topk(&idx, &query, 3);

        assert_eq!(results.len(), 3);
        // doc1: 0.5*1.0 + 0.8*1.0 = 1.3
        // doc2: 0.3*1.0 = 0.3
        // doc3: 0.9*1.0 = 0.9
        assert_eq!(results[0].doc_id.as_deref(), Some("doc1"));
        assert!((results[0].score - 1.3).abs() < 1e-6);
        assert_eq!(results[1].doc_id.as_deref(), Some("doc3"));
        assert_eq!(results[2].doc_id.as_deref(), Some("doc2"));
    }

    #[test]
    fn topk_limits_results() {
        let mut idx = SparseInvertedIndex::new();
        for i in 1..=100 {
            idx.insert(&format!("doc{i}"), &make_sv(&[(1, i as f32)]));
        }

        let query = make_sv(&[(1, 1.0)]);
        let results = dot_product_topk(&idx, &query, 5);
        assert_eq!(results.len(), 5);
        // Highest scores should be doc100, doc99, ...
        assert!((results[0].score - 100.0).abs() < 1e-6);
    }

    #[test]
    fn empty_query() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("doc1", &make_sv(&[(10, 0.5)]));
        let query = SparseVector::empty();
        let results = dot_product_topk(&idx, &query, 10);
        assert!(results.is_empty());
    }

    /// Insert at 10, overwrite at 20, delete at 30: each cut scores the
    /// version visible there.
    #[test]
    fn a_read_at_a_cut_scores_the_version_visible_there() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert_at("doc1", &make_sv(&[(10, 1.0)]), 10);
        idx.insert_at("doc1", &make_sv(&[(10, 2.0)]), 20);
        idx.insert_at("doc2", &make_sv(&[(10, 0.5)]), 15);
        idx.delete_at("doc1", 30);
        let query = make_sv(&[(10, 1.0)]);
        let at = |s: i64| -> Vec<(String, f32)> {
            dot_product_topk_at(&idx, &query, 10, s)
                .into_iter()
                .map(|r| (r.doc_id.unwrap_or_default(), r.score))
                .collect()
        };
        assert!(at(5).is_empty());
        assert_eq!(at(12), vec![("doc1".to_string(), 1.0)]);
        assert_eq!(
            at(25),
            vec![("doc1".to_string(), 2.0), ("doc2".to_string(), 0.5)]
        );
        assert_eq!(at(30), vec![("doc2".to_string(), 0.5)]);
        let current: Vec<String> = dot_product_topk(&idx, &query, 10)
            .into_iter()
            .filter_map(|r| r.doc_id)
            .collect();
        assert_eq!(current, vec!["doc2".to_string()]);
    }

    #[test]
    fn no_overlap() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("doc1", &make_sv(&[(10, 0.5)]));
        let query = make_sv(&[(20, 1.0)]);
        let results = dot_product_topk(&idx, &query, 10);
        assert!(results.is_empty());
    }
}
