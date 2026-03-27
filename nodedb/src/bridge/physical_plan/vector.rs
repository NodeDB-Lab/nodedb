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
}
