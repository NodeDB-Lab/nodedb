//! Vector engine operations dispatched to the Data Plane.

use std::sync::Arc;

/// Vector engine physical operations.
#[derive(Debug, Clone)]
pub enum VectorOp {
    /// Vector similarity search.
    Search {
        collection: String,
        query_vector: Arc<[f32]>,
        top_k: usize,
        /// Optional search beam width override. If 0, uses default `4 * top_k`.
        ef_search: usize,
        /// Pre-computed bitmap of eligible document IDs (from filter evaluation).
        filter_bitmap: Option<Arc<[u8]>>,
        /// Named vector field to search. Empty string = default field.
        field_name: String,
        /// RLS post-candidate filters (serialized `Vec<ScanFilter>`).
        /// Applied after HNSW returns candidates, before returning to client.
        /// Result count may be less than requested `top_k`.
        rls_filters: Vec<u8>,
    },

    /// Insert a vector into the HNSW index (write path).
    Insert {
        collection: String,
        vector: Vec<f32>,
        dim: usize,
        /// Named vector field. Empty string = default (unnamed) field.
        field_name: String,
        /// Optional document ID to associate with this vector.
        doc_id: Option<String>,
    },

    /// Batch insert vectors into the HNSW index.
    BatchInsert {
        collection: String,
        vectors: Vec<Vec<f32>>,
        dim: usize,
    },

    /// Multi-vector search: query across all named vector fields, fuse via RRF.
    MultiSearch {
        collection: String,
        query_vector: Arc<[f32]>,
        top_k: usize,
        ef_search: usize,
        filter_bitmap: Option<Arc<[u8]>>,
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
}
