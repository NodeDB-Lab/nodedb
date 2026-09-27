// SPDX-License-Identifier: Apache-2.0

//! The `HnswIndex` value itself: its fields, its constructors, and the
//! read-only properties that describe how it was built.

use std::cell::RefCell;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

use nodedb_types::hnsw::HnswParams;
use nodedb_types::vector_dtype::VectorStorageDtype;

use crate::hnsw::arena::BeamSearchArena;

use super::super::ARENA_INITIAL_CAPACITY;
use super::super::types::{Node, Xorshift64};

/// Hierarchical Navigable Small World graph index.
///
/// - FP32 construction for structural integrity
/// - Heuristic neighbor selection (Algorithm 4)
/// - Beam search with configurable ef parameter
pub struct HnswIndex {
    pub(crate) params: HnswParams,
    pub(crate) dim: usize,
    pub(crate) nodes: Vec<Node>,
    pub(crate) entry_point: Option<u32>,
    pub(crate) max_layer: usize,
    pub(crate) rng: Xorshift64,
    /// Flat neighbor storage for zero-copy access after checkpoint restore.
    /// When present, `neighbors_at()` reads from here instead of per-node Vecs.
    /// Cleared on first mutation (insert/delete).
    pub(crate) flat_neighbors: Option<crate::hnsw::flat_neighbors::FlatNeighborStore>,
    /// Optional backing store for vector data.
    ///
    /// When set (graph-checkpoint-only restore path), per-node vector storage
    /// is left empty and `dist_to_node` falls through to the backing.  Origin
    /// never sets this field; it is only used by Lite's pagedb segment path.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) backing: Option<Arc<dyn crate::segment_backing::VectorSegmentBacking>>,
    /// Per-invocation scratch arena for beam-search heaps.
    ///
    /// Wrapped in `RefCell` so search methods keep `&self` receivers without
    /// forcing `&mut self` across all call sites.  The borrow is taken at the
    /// start of `search_layer` and released before returning.  The arena must
    /// never be borrowed twice simultaneously — it is a per-call scratch buffer
    /// owned exclusively by one Data Plane core.
    pub(crate) arena: RefCell<BeamSearchArena>,
}

impl HnswIndex {
    /// Create a new empty HNSW index.
    pub fn new(dim: usize, params: HnswParams) -> Self {
        Self::seeded(dim, params, 42)
    }

    /// Create with a specific RNG seed (for deterministic testing).
    pub fn with_seed(dim: usize, params: HnswParams, seed: u64) -> Self {
        Self::seeded(dim, params, seed)
    }

    fn seeded(dim: usize, params: HnswParams, seed: u64) -> Self {
        let initial_capacity = params.ef_construction.max(ARENA_INITIAL_CAPACITY);
        Self {
            dim,
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            rng: Xorshift64::new(seed),
            flat_neighbors: None,
            arena: RefCell::new(BeamSearchArena::new(initial_capacity)),
            params,
            #[cfg(not(target_arch = "wasm32"))]
            backing: None,
        }
    }

    /// The distance metric this index was built with. Search-time metric
    /// overrides must match this; differing metrics require either rebuilding
    /// the index or a metric-aware re-rank pass.
    pub fn metric(&self) -> crate::distance::DistanceMetric {
        self.params.metric
    }

    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Storage dtype this index was constructed with.
    pub fn dtype(&self) -> VectorStorageDtype {
        self.params.dtype
    }

    pub fn entry_point(&self) -> Option<u32> {
        self.entry_point
    }

    pub fn max_layer(&self) -> usize {
        self.max_layer
    }

    /// Current RNG state (for snapshot reproducibility).
    pub fn rng_state(&self) -> u64 {
        self.rng.0
    }

    /// Approximate memory usage in bytes (vector data + neighbor lists).
    pub fn memory_usage_bytes(&self) -> usize {
        let vector_bytes = self.nodes.len() * self.params.dtype.bytes_for_dim(self.dim);
        let neighbor_bytes: usize = self
            .nodes
            .iter()
            .map(|n| {
                n.neighbors
                    .iter()
                    .map(|layer| layer.len() * 4)
                    .sum::<usize>()
            })
            .sum();
        let node_overhead = self.nodes.len() * std::mem::size_of::<Node>();
        vector_bytes + neighbor_bytes + node_overhead
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{HnswIndex, HnswParams};
    use crate::distance::DistanceMetric;
    use crate::error::VectorError;
    use nodedb_types::vector_dtype::VectorStorageDtype;

    fn make_params(dtype: VectorStorageDtype) -> HnswParams {
        HnswParams {
            m: 4,
            m0: 8,
            ef_construction: 32,
            metric: DistanceMetric::L2,
            dtype,
        }
    }

    #[test]
    fn create_empty_index() {
        let idx = HnswIndex::new(3, HnswParams::default());
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
        assert!(idx.entry_point().is_none());
    }

    #[test]
    fn params_default() {
        let p = HnswParams::default();
        assert_eq!(p.m, 16);
        assert_eq!(p.m0, 32);
        assert_eq!(p.ef_construction, 200);
        assert_eq!(p.metric, DistanceMetric::Cosine);
        assert_eq!(p.dtype, VectorStorageDtype::F32);
    }

    #[test]
    fn candidate_ordering() {
        let a = crate::hnsw::graph::types::Candidate { dist: 0.1, id: 1 };
        let b = crate::hnsw::graph::types::Candidate { dist: 0.5, id: 2 };
        assert!(a < b);
    }

    #[test]
    fn f32_default_unchanged() {
        let mut idx = HnswIndex::with_seed(3, make_params(VectorStorageDtype::F32), 1);
        assert_eq!(idx.dtype(), VectorStorageDtype::F32);
        for i in 0..10u32 {
            idx.insert(vec![i as f32, 0.0, 0.0]).unwrap();
        }
        // get_vector works on F32 indexes.
        let v = idx.get_vector(3).unwrap();
        assert_eq!(v[0], 3.0_f32);
        // get_vector_bytes also works.
        assert_eq!(idx.get_vector_bytes(3).unwrap().len(), 12); // 3 dims * 4 bytes
    }

    #[test]
    fn f16_insert_search_smoke() {
        let mut idx = HnswIndex::with_seed(3, make_params(VectorStorageDtype::F16), 42);
        assert_eq!(idx.dtype(), VectorStorageDtype::F16);
        for i in 0..10u32 {
            idx.insert(vec![i as f32, 0.0, 0.0]).unwrap();
        }
        let results = idx.search(&[5.0, 0.0, 0.0], 3, 32);
        assert_eq!(results.len(), 3);
        // Results must be in monotonically non-decreasing distance order.
        for w in results.windows(2) {
            assert!(
                w[0].distance <= w[1].distance,
                "results not sorted: {:?}",
                results
            );
        }
    }

    #[test]
    fn bf16_insert_search_smoke() {
        let mut idx = HnswIndex::with_seed(3, make_params(VectorStorageDtype::BF16), 42);
        assert_eq!(idx.dtype(), VectorStorageDtype::BF16);
        for i in 0..10u32 {
            idx.insert(vec![i as f32, 0.0, 0.0]).unwrap();
        }
        let results = idx.search(&[5.0, 0.0, 0.0], 3, 32);
        assert_eq!(results.len(), 3);
        for w in results.windows(2) {
            assert!(
                w[0].distance <= w[1].distance,
                "results not sorted: {:?}",
                results
            );
        }
    }

    #[test]
    fn get_vector_returns_none_on_non_f32_dtype() {
        let mut idx = HnswIndex::with_seed(3, make_params(VectorStorageDtype::F16), 1);
        idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
        // get_vector_bytes works for F16; get_vector does not (returns None in
        // release, fires debug_assert in dev — so we only assert None in release).
        assert!(idx.get_vector_bytes(0).is_some());
        #[cfg(not(debug_assertions))]
        assert!(idx.get_vector(0).is_none());
    }

    /// `materialize_vector` is the accessor every vector-copying caller uses, so it
    /// must work for narrow dtypes where `get_vector` cannot return a borrow.
    #[test]
    fn materialize_vector_decodes_every_dtype() {
        for dtype in [
            VectorStorageDtype::F32,
            VectorStorageDtype::F16,
            VectorStorageDtype::BF16,
        ] {
            let mut idx = HnswIndex::with_seed(3, make_params(dtype), 1);
            idx.insert(vec![1.0, 2.0, 4.0]).unwrap();
            let v = idx
                .materialize_vector(0)
                .unwrap_or_else(|e| panic!("dtype={dtype:?} must materialize, got {e}"));
            assert_eq!(v.len(), 3, "dtype={dtype:?}");
            // F16/BF16 are lossy; these values are exactly representable in both.
            assert_eq!(v, vec![1.0, 2.0, 4.0], "dtype={dtype:?}");
        }
    }

    /// Rerank must see the vector for a narrow dtype too. Before the `Cow` return
    /// this yielded `None` for F16/BF16, which made the FP32 rerank path fail with
    /// "fetch_vector returned None" for every candidate in the collection.
    ///
    /// Native-only: `get_vector_or_backing` is gated with the backing type.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn get_vector_or_backing_serves_narrow_dtypes() {
        for dtype in [VectorStorageDtype::F16, VectorStorageDtype::BF16] {
            let mut idx = HnswIndex::with_seed(3, make_params(dtype), 1);
            idx.insert(vec![1.0, 2.0, 4.0]).unwrap();
            let v = idx
                .get_vector_or_backing(0)
                .unwrap_or_else(|| panic!("dtype={dtype:?} must serve a rerank vector"));
            assert_eq!(&*v, &[1.0, 2.0, 4.0][..], "dtype={dtype:?}");
        }
    }

    /// An index whose per-node storage is empty and which has no backing attached
    /// cannot produce vectors. Every extractor must report that rather than hand back
    /// empty vecs — serializing those writes a segment whose header declares vectors
    /// the payload does not contain, silently destroying the collection.
    #[test]
    fn extractors_reject_an_index_with_no_vector_source() {
        let mut idx = HnswIndex::with_seed(3, make_params(VectorStorageDtype::F32), 1);
        idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
        let graph_only = idx.graph_checkpoint_to_bytes().unwrap();
        // Restored WITHOUT `with_backing`: node storage is an empty placeholder.
        let restored = HnswIndex::from_checkpoint(&graph_only)
            .unwrap()
            .expect("graph checkpoint must be recognized");
        assert_eq!(restored.len(), 1, "graph topology must survive");

        assert!(
            matches!(
                restored.materialize_vector(0),
                Err(VectorError::VectorUnavailable { id: 0 })
            ),
            "materialize_vector must report the missing vector source"
        );
        assert!(
            restored.export_vectors().is_err(),
            "export_vectors must not yield empty placeholder vectors"
        );
        assert!(
            restored.extract_vectors_and_surrogates().is_err(),
            "extract_vectors_and_surrogates must not yield empty placeholder vectors"
        );
        assert!(
            restored.checkpoint_to_bytes().is_err(),
            "a full checkpoint cannot be written from an index with no vector data"
        );
    }

    /// A backing is validated before it is attached. An attached-but-unserviceable
    /// backing is the worst case: the graph looks healthy, so search proceeds and
    /// then scores a node that has no vector — which is what made one poisoned
    /// segment panic the daemon on every query.
    ///
    /// Native-only: the trait under test lives in `crate::segment_backing`,
    /// which is gated off on wasm32 (mmap is the only implementation).
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn with_backing_refuses_a_backing_that_cannot_serve_the_index() {
        use crate::segment_backing::VectorSegmentBacking;

        /// Declares `len`/`dim` in its header but serves `served` vectors, modelling
        /// a segment whose header claims vectors its payload does not contain.
        struct LyingBacking {
            len: usize,
            dim: usize,
            served: Vec<Vec<f32>>,
        }
        impl VectorSegmentBacking for LyingBacking {
            fn len(&self) -> usize {
                self.len
            }
            fn dim(&self) -> usize {
                self.dim
            }
            fn get_vector(&self, id: u32) -> Option<&[f32]> {
                self.served.get(id as usize).map(Vec::as_slice)
            }
            fn get_surrogate(&self, _id: u32) -> Option<u64> {
                None
            }
        }

        let mut src = HnswIndex::with_seed(3, make_params(VectorStorageDtype::F32), 1);
        src.insert(vec![1.0, 2.0, 3.0]).unwrap();
        src.insert(vec![4.0, 5.0, 6.0]).unwrap();
        let graph_only = src.graph_checkpoint_to_bytes().unwrap();
        let restore = || {
            HnswIndex::from_checkpoint(&graph_only)
                .unwrap()
                .expect("graph checkpoint must be recognized")
        };

        // Header claims 2 vectors of dim 3, payload serves none — the poisoned case.
        let mut idx = restore();
        assert!(
            idx.with_backing(Arc::new(LyingBacking {
                len: 2,
                dim: 3,
                served: Vec::new(),
            }))
            .is_err(),
            "a backing that serves no vectors must be refused"
        );

        // Fewer vectors than the index has nodes.
        let mut idx = restore();
        assert!(
            idx.with_backing(Arc::new(LyingBacking {
                len: 1,
                dim: 3,
                served: vec![vec![1.0, 2.0, 3.0]],
            }))
            .is_err(),
            "a backing shorter than the node count must be refused"
        );

        // Right count, wrong dimension.
        let mut idx = restore();
        assert!(
            matches!(
                idx.with_backing(Arc::new(LyingBacking {
                    len: 2,
                    dim: 4,
                    served: vec![vec![0.0; 4], vec![0.0; 4]],
                })),
                Err(VectorError::DimensionMismatch { .. })
            ),
            "a backing with the wrong dim must be refused"
        );

        // A backing that genuinely serves every node IS attached, and search on the
        // restored index then resolves vectors through it without panicking.
        let mut idx = restore();
        idx.with_backing(Arc::new(LyingBacking {
            len: 2,
            dim: 3,
            served: vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
        }))
        .expect("a complete backing must attach");
        assert_eq!(idx.materialize_vector(0).unwrap(), vec![1.0, 2.0, 3.0]);
        assert_eq!(idx.materialize_vector(1).unwrap(), vec![4.0, 5.0, 6.0]);
    }

    /// A node with no vector source must rank last, not abort the search worker.
    #[test]
    fn search_on_a_vectorless_index_does_not_panic() {
        let mut src = HnswIndex::with_seed(3, make_params(VectorStorageDtype::F32), 1);
        src.insert(vec![1.0, 2.0, 3.0]).unwrap();
        let graph_only = src.graph_checkpoint_to_bytes().unwrap();
        // No backing attached: every node's storage is an empty placeholder.
        let idx = HnswIndex::from_checkpoint(&graph_only)
            .unwrap()
            .expect("graph checkpoint must be recognized");

        let results = idx.search(&[1.0, 2.0, 3.0], 5, 16);
        for r in &results {
            assert!(
                r.distance.is_infinite(),
                "a node with no vector source must score infinity, got {}",
                r.distance
            );
        }
    }

    #[test]
    fn get_vector_bytes_works_for_all_dtypes() {
        for (dtype, expected_byte_len) in [
            (VectorStorageDtype::F32, 12usize), // 3 dims * 4 bytes
            (VectorStorageDtype::F16, 6usize),  // 3 dims * 2 bytes
            (VectorStorageDtype::BF16, 6usize), // 3 dims * 2 bytes
        ] {
            let mut idx = HnswIndex::with_seed(3, make_params(dtype), 1);
            idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
            let bytes = idx.get_vector_bytes(0).expect("must be Some for valid id");
            assert_eq!(
                bytes.len(),
                expected_byte_len,
                "wrong byte len for dtype={dtype:?}"
            );
        }
    }
}
