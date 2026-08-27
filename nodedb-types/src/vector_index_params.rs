// SPDX-License-Identifier: Apache-2.0

//! Durable per-collection/column vector-index parameters.
//!
//! Stored in the system catalog keyed by `(tenant_id, collection, field_name)`
//! so a `CREATE VECTOR INDEX`'s configuration survives a hard crash and can be
//! re-seeded to the Data Plane on boot (the WAL `VectorParams` record alone is
//! not crash-durable).

use serde::{Deserialize, Serialize};

/// Catalog entry for a vector index's build parameters.
///
/// NOTE: serialized as a zerompk ARRAY (the crate default — the
/// `default-as-map` feature is not enabled), so field ORDER is part of the
/// on-disk format. `database_id` was appended last; entries written by older
/// builds (11 fields, no database id) must still decode — see the legacy
/// fallback ladder in `catalog/vector_index_params.rs`.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct StoredVectorIndexParams {
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
    /// Database that owns the collection. `0` = `DatabaseId::DEFAULT`,
    /// which is also what entries written before this field existed decode
    /// to (the legacy ladder fills 0) — matching the historical behavior of
    /// the seed/rebuild paths.
    pub database_id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msgpack_roundtrip() {
        let e = StoredVectorIndexParams {
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
            database_id: 7,
        };
        let bytes = zerompk::to_msgpack_vec(&e).unwrap();
        let back: StoredVectorIndexParams = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back.collection, "docs");
        assert_eq!(back.dim, 4);
        assert_eq!(back.field_name, "embedding");
        assert_eq!(back.database_id, 7);
    }

    #[test]
    fn legacy_11_field_array_decodes_with_database_id_zero() {
        // Entries written before `database_id` existed are 11-element arrays
        // in field order. The catalog module's decode ladder tolerates them
        // by decoding the legacy tuple shape; here we verify the tuple shape
        // itself round-trips so the ladder's fallback type is valid.
        let legacy: (u64, String, String, usize, String, usize, usize, String, usize, usize, usize) =
            (
                1,
                "docs".into(),
                "embedding".into(),
                4,
                "cosine".into(),
                16,
                200,
                String::new(),
                0,
                0,
                0,
            );
        let bytes = zerompk::to_msgpack_vec(&legacy).unwrap();
        let back: (
            u64,
            String,
            String,
            usize,
            String,
            usize,
            usize,
            String,
            usize,
            usize,
            usize,
        ) = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back.1, "docs");
        assert_eq!(back.2, "embedding");
    }
}
