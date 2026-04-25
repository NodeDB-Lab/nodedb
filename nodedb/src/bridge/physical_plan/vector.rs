//! Vector engine operations dispatched to the Data Plane.

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::bridge::envelope::PhysicalPlan;

/// Vector engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum VectorOp {
    /// Vector similarity search.
    Search {
        collection: String,
        query_vector: Vec<f32>,
        top_k: usize,
        /// Optional search beam width override. If 0, uses default `4 * top_k`.
        ef_search: usize,
        /// Pre-computed bitmap of eligible surrogates (from filter evaluation).
        filter_bitmap: Option<SurrogateBitmap>,
        /// Named vector field to search. Empty string = default field.
        field_name: String,
        /// RLS post-candidate filters (serialized `Vec<ScanFilter>`).
        /// Applied after HNSW returns candidates, before returning to client.
        /// Result count may be less than requested `top_k`.
        rls_filters: Vec<u8>,
        /// Optional sub-plan whose output rows materialize a `SurrogateBitmap`
        /// used as an additional prefilter (intersected with `filter_bitmap`).
        ///
        /// Set by the planner when fusing `Vector ORDER BY ... + JOIN
        /// NDARRAY_SLICE(...) ON v.id = s.surrogate` into a single op:
        /// the array slice runs first, its surrogate column becomes the
        /// vector engine's pre-filter, then HNSW search runs against the
        /// reduced candidate set. Mirrors `HashJoin`'s `inline_*_bitmap`
        /// mechanism so the Data Plane composition is uniform.
        inline_prefilter_plan: Option<Box<PhysicalPlan>>,
    },

    /// Insert a vector into the HNSW index (write path).
    Insert {
        collection: String,
        vector: Vec<f32>,
        dim: usize,
        /// Named vector field. Empty string = default (unnamed) field.
        field_name: String,
        /// Global surrogate identifying this row. Allocated by the
        /// Control Plane via `SurrogateAssigner` before dispatch; the
        /// engine binds the HNSW node id to this surrogate.
        surrogate: Surrogate,
    },

    /// Batch insert vectors into the HNSW index.
    BatchInsert {
        collection: String,
        vectors: Vec<Vec<f32>>,
        dim: usize,
        /// One surrogate per inserted vector, parallel to `vectors`.
        /// Empty vector = headless batch (no PK binding).
        surrogates: Vec<Surrogate>,
    },

    /// Multi-vector search: query across all named vector fields, fuse via RRF.
    MultiSearch {
        collection: String,
        query_vector: Vec<f32>,
        top_k: usize,
        ef_search: usize,
        filter_bitmap: Option<SurrogateBitmap>,
        /// RLS post-candidate filters.
        rls_filters: Vec<u8>,
    },

    /// Soft-delete a vector by internal node ID.
    Delete { collection: String, vector_id: u32 },

    /// Set vector index parameters for a collection.
    SetParams {
        collection: String,
        m: usize,
        ef_construction: usize,
        metric: String,
        /// Index type: "hnsw" (default), "hnsw_pq", or "ivf_pq".
        index_type: String,
        /// PQ subvectors (for hnsw_pq and ivf_pq). Default: 8.
        pq_m: usize,
        /// IVF cells (for ivf_pq only). Default: 256.
        ivf_cells: usize,
        /// IVF probe count (for ivf_pq only). Default: 16.
        ivf_nprobe: usize,
    },

    /// Query live vector index statistics. Returns `VectorIndexStats` as payload.
    QueryStats {
        collection: String,
        /// Named vector field. Empty string = default (unnamed) field.
        field_name: String,
    },

    /// Force-seal the growing segment, triggering background HNSW build.
    Seal {
        collection: String,
        /// Named vector field. Empty string = default field.
        field_name: String,
    },

    /// Force tombstone compaction on sealed segments.
    CompactIndex {
        collection: String,
        /// Named vector field. Empty string = default field.
        field_name: String,
    },

    /// Rebuild sealed segments with new HNSW parameters.
    /// Old index serves queries until rebuild completes, then swaps atomically.
    Rebuild {
        collection: String,
        /// Named vector field. Empty string = default field.
        field_name: String,
        /// New M parameter. 0 = keep current.
        m: usize,
        /// New M0 parameter. 0 = keep current.
        m0: usize,
        /// New ef_construction. 0 = keep current.
        ef_construction: usize,
    },

    /// Insert a sparse vector into the inverted index.
    SparseInsert {
        collection: String,
        /// Named sparse vector field.
        field_name: String,
        /// Document ID to associate with this sparse vector.
        doc_id: String,
        /// Sparse vector entries as `(dimension, weight)` pairs.
        entries: Vec<(u32, f32)>,
    },

    /// Search the sparse inverted index via dot-product scoring.
    SparseSearch {
        collection: String,
        /// Named sparse vector field.
        field_name: String,
        /// Query sparse vector entries.
        query_entries: Vec<(u32, f32)>,
        /// Maximum results to return.
        top_k: usize,
    },

    /// Delete a document from the sparse inverted index.
    SparseDelete {
        collection: String,
        /// Named sparse vector field.
        field_name: String,
        /// Document ID to remove.
        doc_id: String,
    },

    /// Insert multiple vectors for a single document (ColBERT-style).
    /// All vectors are inserted as separate HNSW nodes sharing the
    /// same `document_surrogate`.
    MultiVectorInsert {
        collection: String,
        /// Named vector field. Empty = default.
        field_name: String,
        /// Surrogate shared by all vectors of the document.
        document_surrogate: Surrogate,
        /// Flat vector data: count × dim f32 values.
        vectors: Vec<f32>,
        /// Number of vectors.
        count: usize,
        /// Dimensionality of each vector.
        dim: usize,
    },

    /// Delete all vectors for a document from the multi-vector index.
    MultiVectorDelete {
        collection: String,
        /// Named vector field. Empty = default.
        field_name: String,
        /// Document surrogate whose vectors should be tombstoned.
        document_surrogate: Surrogate,
    },

    /// Search with multi-vector aggregated scoring (MaxSim, AvgSim, SumSim).
    /// Over-fetches from HNSW, groups by doc_id, aggregates, deduplicates.
    MultiVectorScoreSearch {
        collection: String,
        /// Named vector field. Empty = default.
        field_name: String,
        /// Query vector.
        query_vector: Vec<f32>,
        /// Maximum documents to return.
        top_k: usize,
        /// HNSW ef_search override. 0 = auto.
        ef_search: usize,
        /// Aggregation mode: "max_sim", "avg_sim", "sum_sim".
        mode: String,
    },
}
