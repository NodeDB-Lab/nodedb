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
/// weights from posting lists. Returns top-K results sorted by score descending.
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

    // Accumulate scores per document.
    let mut scores: HashMap<u32, f32> = HashMap::new();

    for &(dim, q_weight) in query.entries() {
        if let Some(postings) = index.get_postings(dim) {
            for &(doc_id, doc_weight) in postings {
                *scores.entry(doc_id).or_insert(0.0) += q_weight * doc_weight;
            }
        }
    }

    if scores.is_empty() {
        return Vec::new();
    }

    // Top-K selection via min-heap bounded to K entries.
    let mut heap: BinaryHeap<std::cmp::Reverse<HeapEntry>> = BinaryHeap::with_capacity(top_k + 1);

    for (doc_id, score) in &scores {
        heap.push(std::cmp::Reverse(HeapEntry {
            score: *score,
            doc_id: *doc_id,
        }));
        if heap.len() > top_k {
            heap.pop(); // Remove smallest.
        }
    }

    // Drain heap into results (highest score first).
    // `into_sorted_vec` on `BinaryHeap<Reverse<T>>` returns ascending `Reverse`
    // order, which is descending actual score order.
    let results: Vec<SparseSearchResult> = heap
        .into_sorted_vec()
        .into_iter()
        .map(|std::cmp::Reverse(entry)| SparseSearchResult {
            internal_id: entry.doc_id,
            score: entry.score,
            doc_id: index.resolve_doc_id(entry.doc_id).map(String::from),
        })
        .collect();

    results
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

    #[test]
    fn no_overlap() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("doc1", &make_sv(&[(10, 0.5)]));
        let query = make_sv(&[(20, 1.0)]);
        let results = dot_product_topk(&idx, &query, 10);
        assert!(results.is_empty());
    }
}
