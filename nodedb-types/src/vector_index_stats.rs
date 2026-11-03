// SPDX-License-Identifier: Apache-2.0

//! Vector index statistics returned by `SHOW VECTOR INDEX status ON collection.column`.
//!
//! Serialized as MessagePack for transport through the SPSC bridge (Data → Control).

use serde::{Deserialize, Serialize};

/// Quantization mode currently active on a vector index.
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum VectorIndexQuantization {
    None = 0,
    Sq8 = 1,
    Pq = 2,
    Binary = 3,
    RaBitQ = 4,
    Bbq = 5,
    Ternary = 6,
    Opq = 7,
}

impl std::fmt::Display for VectorIndexQuantization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Sq8 => write!(f, "sq8"),
            Self::Pq => write!(f, "pq"),
            Self::Binary => write!(f, "binary"),
            Self::RaBitQ => write!(f, "rabitq"),
            Self::Bbq => write!(f, "bbq"),
            Self::Ternary => write!(f, "ternary"),
            Self::Opq => write!(f, "opq"),
        }
    }
}

/// Index type identifier.
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
#[non_exhaustive]
pub enum VectorIndexType {
    Hnsw = 0,
    HnswPq = 1,
    IvfPq = 2,
    Flat = 3,
}

impl std::fmt::Display for VectorIndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hnsw => write!(f, "hnsw"),
            Self::HnswPq => write!(f, "hnsw_pq"),
            Self::IvfPq => write!(f, "ivf_pq"),
            Self::Flat => write!(f, "flat"),
        }
    }
}

/// Live statistics for a single vector index (one collection × one field).
///
/// Collected on the Data Plane from `VectorCollection` segment state and
/// serialized back to the Control Plane via the SPSC bridge response payload.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct VectorIndexStats {
    /// Number of sealed segments (with completed HNSW indexes).
    pub sealed_count: usize,
    /// Number of segments currently being built (HNSW construction in progress).
    pub building_count: usize,
    /// Number of vectors in the growing segment (not yet sealed).
    pub growing_vectors: usize,
    /// Total vectors across all sealed segments (including tombstoned).
    pub sealed_vectors: usize,
    /// Total live (non-deleted) vectors across all segments.
    pub live_count: usize,
    /// Total tombstoned (soft-deleted) vectors across all segments.
    pub tombstone_count: usize,
    /// Tombstone ratio: tombstone_count / total_vectors. 0.0 if no vectors.
    pub tombstone_ratio: f64,
    /// Quantization mode for sealed segments.
    pub quantization: VectorIndexQuantization,
    /// Approximate RAM usage in bytes (HNSW graph + quantized storage).
    pub memory_bytes: usize,
    /// Approximate disk usage in bytes (mmap FP32 segments on NVMe).
    pub disk_bytes: usize,
    /// Whether any HNSW build is currently in progress.
    pub build_in_progress: bool,
    /// Index type.
    pub index_type: VectorIndexType,
    /// HNSW M parameter (max bidirectional connections per node).
    pub hnsw_m: usize,
    /// HNSW M0 parameter (max connections at layer 0).
    pub hnsw_m0: usize,
    /// HNSW ef_construction parameter.
    pub hnsw_ef_construction: usize,
    /// Distance metric name.
    pub metric: String,
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Seal threshold (vectors per growing segment before auto-seal).
    pub seal_threshold: usize,
    /// Number of sealed segments backed by mmap (L1 NVMe tier).
    pub mmap_segment_count: u32,
    /// Resident memory of the dedicated per-collection jemalloc arena, in
    /// bytes. `None` when the collection has no dedicated arena (e.g., it is
    /// not vector-primary, or the runtime does not support per-arena stats).
    pub arena_bytes: Option<u64>,
    /// IVF-PQ training state. `None` unless the index type is `ivf_pq`.
    pub ivf: Option<VectorIvfStats>,
    /// HNSW builds of this index waiting for or running on the builder.
    pub builds_queued: usize,
    /// HNSW builds of this index installed since the core opened it.
    pub builds_completed: u64,
    /// HNSW builds of this index that failed since the core opened it.
    pub builds_failed: u64,
}

/// Training state of an IVF-PQ vector index.
///
/// An IVF-PQ index buffers vectors, searched exactly, until it holds
/// `training_threshold` live vectors. It then trains its cells and PQ
/// codebooks on them and searches through IVF-PQ from then on.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VectorIvfStats {
    /// Live vectors the index needs before it trains: `max(ivf_cells, pq_k)`.
    pub training_threshold: usize,
    /// Whether training has run.
    pub trained: bool,
    /// Vectors the training read. `0` before training.
    pub trained_on: usize,
    /// Unix milliseconds of the training. `0` before training.
    pub trained_at_ms: u64,
    /// Vectors held by the trained index, live or soft-deleted.
    pub indexed_vectors: usize,
    /// Voronoi cells of the trained index. `0` before training.
    pub cells: usize,
    /// Cells probed per query.
    pub nprobe: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip() {
        let stats = VectorIndexStats {
            sealed_count: 3,
            building_count: 1,
            growing_vectors: 5000,
            sealed_vectors: 180_000,
            live_count: 183_000,
            tombstone_count: 2000,
            tombstone_ratio: 0.011,
            quantization: VectorIndexQuantization::Sq8,
            memory_bytes: 245 * 1024 * 1024,
            disk_bytes: 890 * 1024 * 1024,
            build_in_progress: true,
            index_type: VectorIndexType::Hnsw,
            hnsw_m: 16,
            hnsw_m0: 32,
            hnsw_ef_construction: 200,
            metric: "cosine".into(),
            dimensions: 768,
            seal_threshold: 65_536,
            mmap_segment_count: 1,
            arena_bytes: Some(4 * 1024 * 1024),
            ivf: Some(VectorIvfStats {
                training_threshold: 256,
                trained: true,
                trained_on: 300,
                trained_at_ms: 1_700_000_000_000,
                indexed_vectors: 310,
                cells: 16,
                nprobe: 4,
            }),
            builds_queued: 1,
            builds_completed: 2,
            builds_failed: 0,
        };
        let bytes = zerompk::to_msgpack_vec(&stats).unwrap();
        let restored: VectorIndexStats = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(restored.sealed_count, 3);
        assert_eq!(restored.live_count, 183_000);
        assert_eq!(restored.quantization, VectorIndexQuantization::Sq8);
        assert_eq!(restored.index_type, VectorIndexType::Hnsw);
        assert_eq!(restored.ivf, stats.ivf);
        assert_eq!(restored.builds_completed, 2);
    }
}
