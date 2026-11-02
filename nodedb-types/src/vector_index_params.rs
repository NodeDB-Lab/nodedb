// SPDX-License-Identifier: Apache-2.0

//! Durable per-collection/column vector-index parameters.
//!
//! Stored in the system catalog keyed by `(tenant_id, collection, field_name)`
//! so a `CREATE VECTOR INDEX`'s configuration survives a hard crash and can be
//! re-seeded to the Data Plane on boot (the WAL `VectorParams` record alone is
//! not crash-durable).

use serde::{Deserialize, Serialize};

/// Catalog entry for a vector index's build parameters.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct StoredVectorIndexParams {
    /// Database that owns the collection.
    pub database_id: u64,
    /// Tenant that owns the collection.
    pub tenant_id: u64,
    /// Collection the index is on.
    pub collection: String,
    /// Embedding column the index covers (empty = the collection's default vector field).
    pub field_name: String,
    /// Vector dimensionality (0 = unspecified).
    pub dim: usize,
    /// Distance metric (lowercased, e.g. "cosine", "l2").
    pub metric: String,
    /// HNSW M parameter.
    pub m: usize,
    /// HNSW ef_construction parameter.
    pub ef_construction: usize,
    /// Index type ("" = hnsw, else "hnsw_pq" | "ivf_pq").
    pub index_type: String,
    /// Product-quantization M (0 = unused).
    pub pq_m: usize,
    /// IVF cell count (0 = unused).
    pub ivf_cells: usize,
    /// IVF nprobe (0 = unused).
    pub ivf_nprobe: usize,
    /// Stamped at propose time on every put; fences a replayed delete to
    /// the incarnation it targeted.
    pub modification_hlc: crate::hlc::Hlc,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msgpack_roundtrip() {
        let e = StoredVectorIndexParams {
            database_id: 7,
            tenant_id: 1,
            collection: "docs".into(),
            field_name: "embedding".into(),
            dim: 4,
            metric: "cosine".into(),
            m: 16,
            ef_construction: 200,
            index_type: String::new(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
            modification_hlc: crate::hlc::Hlc::ZERO,
        };
        let bytes = zerompk::to_msgpack_vec(&e).unwrap();
        let back: StoredVectorIndexParams = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back.collection, "docs");
        assert_eq!(back.dim, 4);
        assert_eq!(back.field_name, "embedding");
        assert_eq!(back.database_id, 7);
    }
}
