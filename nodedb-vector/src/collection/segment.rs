//! Segment types for the VectorCollection lifecycle.

use crate::collection::tier::StorageTier;
use crate::flat::FlatIndex;
use crate::hnsw::{HnswIndex, HnswParams};
use crate::mmap_segment::MmapVectorSegment;
use crate::quantize::pq::PqCodec;
use crate::quantize::sq8::Sq8Codec;

/// Default threshold for sealing the growing segment.
/// 64K vectors × 768 dims × 4 bytes = ~192 MiB per segment.
pub const DEFAULT_SEAL_THRESHOLD: usize = 65_536;

/// Request to build an HNSW index from sealed vectors (sent to builder thread).
pub struct BuildRequest {
    pub key: String,
    pub segment_id: u32,
    pub vectors: Vec<Vec<f32>>,
    pub dim: usize,
    pub params: HnswParams,
}

/// Completed HNSW build (sent back from builder thread).
pub struct BuildComplete {
    pub key: String,
    pub segment_id: u32,
    pub index: HnswIndex,
}

/// A sealed segment whose HNSW index is being built in background.
pub struct BuildingSegment {
    /// Flat index for brute-force search while HNSW is building.
    pub flat: FlatIndex,
    /// Base ID offset: vectors have global IDs [base_id .. base_id + count).
    pub base_id: u32,
    /// Unique segment identifier (for matching with BuildComplete).
    pub segment_id: u32,
}

/// A sealed segment with a completed HNSW index.
pub struct SealedSegment {
    /// Built HNSW index (immutable after construction).
    pub index: HnswIndex,
    /// Base ID offset.
    pub base_id: u32,
    /// Optional SQ8 quantized vectors for accelerated traversal.
    pub sq8: Option<(Sq8Codec, Vec<u8>)>,
    /// Optional PQ-compressed codes (for HnswPq-configured indexes).
    pub pq: Option<(PqCodec, Vec<u8>)>,
    /// Storage tier: L0Ram = FP32 in HNSW nodes, L1Nvme = FP32 in mmap file.
    pub tier: StorageTier,
    /// mmap-backed vector segment for L1 NVMe tier.
    pub mmap_vectors: Option<MmapVectorSegment>,
}
