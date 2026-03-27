//! Full-text search operations dispatched to the Data Plane.

use std::sync::Arc;

/// Full-text search physical operations.
#[derive(Debug, Clone)]
pub enum TextOp {
    /// BM25 full-text search on the inverted index.
    Search {
        collection: String,
        query: String,
        top_k: usize,
        /// Enable fuzzy matching (Levenshtein) for typo tolerance.
        fuzzy: bool,
    },

    /// Hybrid search: vector similarity + BM25 text, fused via RRF.
    HybridSearch {
        collection: String,
        query_vector: Arc<[f32]>,
        query_text: String,
        top_k: usize,
        ef_search: usize,
        fuzzy: bool,
        /// Weight for vector results in RRF (0.0–1.0). Default: 0.5.
        vector_weight: f32,
        filter_bitmap: Option<Arc<[u8]>>,
    },
}
