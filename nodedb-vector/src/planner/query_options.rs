//! `VectorQueryOptions` — all knobs the vector planner exposes to callers.
//!
//! Mirrors the SQL syntax:
//! ```sql
//! SELECT … ORDER BY embedding <=> $q LIMIT 10
//! WITH (quantization='rabitq', rerank=100, query_dim=512, meta_tokens=8)
//! ```

/// Which graph index to use for this query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// In-memory HNSW graph.  Default.
    Hnsw,
    /// SSD-resident Vamana graph (DiskANN).  Selected for billion-scale collections.
    Vamana,
}

/// Which quantization codec to use during traversal and optionally rerank.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantizationKind {
    /// 8-bit scalar quantization (~0.97 recall with no rerank).
    Sq8,
    /// Product quantization (~0.92 recall).
    Pq,
    /// 1-bit RaBitQ with `O(1/√D)` error bound (SIGMOD 2024).
    RaBitQ,
    /// Better Binary Quantization — centroid-centered asymmetric 1-bit + rerank.
    Bbq,
    /// Raw sign-bit binary (cheap; ~0.85 recall without rerank).
    Binary,
    /// BitNet 1.58 trit-packed ternary.
    Ternary,
    /// OPQ — learned rotation applied before PQ.
    Opq,
    /// No quantization — FP32 distance in hot path.
    None,
}

/// All knobs the vector planner exposes.
///
/// This struct is constructed by `nodedb-sql`'s engine rules from query hints
/// and passed through to `nodedb-vector`'s search entry-points.  Every field
/// has a sensible default so callers can build with `..Default::default()`.
#[derive(Debug, Clone)]
pub struct VectorQueryOptions {
    /// Graph index type to use.
    pub index_type: IndexType,

    /// Quantization codec for in-graph distance computation.
    pub quantization: QuantizationKind,

    /// Matryoshka coarse dimension for adaptive-dim querying.
    ///
    /// When `Some(d)`, the first `d` dimensions are used for coarse-pass
    /// traversal; a full-dimension rerank follows over top-`ef_search`
    /// candidates.  `None` means use the full embedding dimension.
    pub query_dim: Option<u32>,

    /// BBQ / RaBitQ rerank oversample multiplier.
    ///
    /// The actual rerank candidate count is `oversample * ef_search`.
    /// Ignored when `quantization == None`.
    pub oversample: u8,

    /// MetaEmbed Meta Token budget for budgeted MaxSim.
    ///
    /// `None` uses the default token count for the collection.
    pub meta_token_budget: Option<u8>,

    /// Beam width for HNSW / Vamana search.  Must be >= `k`.
    pub ef_search: usize,

    /// Number of nearest neighbours to return.
    pub k: usize,

    /// Target recall in `[0.0, 1.0]`.  The planner uses this to pick the
    /// quantization tier and oversample ratio when the caller does not
    /// specify them explicitly.
    pub target_recall: f32,
}

impl Default for VectorQueryOptions {
    fn default() -> Self {
        Self {
            index_type: IndexType::Hnsw,
            quantization: QuantizationKind::Sq8,
            query_dim: None,
            oversample: 3,
            meta_token_budget: None,
            ef_search: 64,
            k: 10,
            target_recall: 0.95,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_produces_reasonable_values() {
        let opts = VectorQueryOptions::default();
        assert_eq!(opts.index_type, IndexType::Hnsw);
        assert_eq!(opts.oversample, 3);
        assert!(opts.ef_search >= opts.k, "ef_search must be >= k");
        assert!(opts.target_recall > 0.0 && opts.target_recall <= 1.0);
        assert!(opts.query_dim.is_none());
        assert!(opts.meta_token_budget.is_none());
    }

    #[test]
    fn custom_options_roundtrip() {
        let opts = VectorQueryOptions {
            index_type: IndexType::Vamana,
            quantization: QuantizationKind::RaBitQ,
            query_dim: Some(512),
            oversample: 5,
            meta_token_budget: Some(8),
            ef_search: 128,
            k: 20,
            target_recall: 0.99,
        };
        assert_eq!(opts.index_type, IndexType::Vamana);
        assert_eq!(opts.quantization, QuantizationKind::RaBitQ);
        assert_eq!(opts.query_dim, Some(512));
        assert_eq!(opts.oversample, 5);
        assert_eq!(opts.meta_token_budget, Some(8));
        assert_eq!(opts.ef_search, 128);
        assert_eq!(opts.k, 20);
        assert!((opts.target_recall - 0.99).abs() < f32::EPSILON);
    }
}
