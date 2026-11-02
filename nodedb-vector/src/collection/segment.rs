// SPDX-License-Identifier: Apache-2.0

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

/// What a finished build replaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildKind {
    /// Promote building segment `segment_id` to sealed.
    Seal,
    /// Replace the sealed segment at `base_id`, which held `len` nodes when
    /// its vectors were read. A segment that no longer holds `len` nodes
    /// (compaction renumbered it) refuses the result.
    Rebuild { base_id: u32, len: usize },
}

/// Request to build an HNSW index (sent to the builder thread).
///
/// `vectors` holds one vector per local node id, soft-deleted nodes
/// included, so the built graph keeps every id. The owning core applies
/// the tombstones when it installs the result.
pub struct BuildRequest {
    pub key: String,
    pub segment_id: u32,
    pub kind: BuildKind,
    pub vectors: Vec<Vec<f32>>,
    pub dim: usize,
    pub params: HnswParams,
}

/// Completed HNSW build (sent back from builder thread).
pub struct BuildComplete {
    pub key: String,
    pub segment_id: u32,
    pub kind: BuildKind,
    /// The built index, or the error that stopped the build. A failed build
    /// leaves the segment as it was.
    pub result: Result<HnswIndex, crate::error::VectorError>,
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
    /// Bytes the segment holds in RAM ([`SealedSegment::measure`]), set
    /// when the segment is built, restored, or compacted. Reading it is
    /// constant time, so a write can charge its delta at once.
    pub resident_bytes: usize,
}

impl SealedSegment {
    /// Bytes `index` and its SQ8 codes hold in RAM. Walks every node.
    pub fn measure(index: &HnswIndex, sq8: Option<&(Sq8Codec, Vec<u8>)>) -> usize {
        index.memory_usage_bytes() + sq8.map_or(0, |(_, data)| data.len())
    }

    /// Measure the segment again after its index or codes changed.
    pub fn remeasure(&mut self) {
        self.resident_bytes = Self::measure(&self.index, self.sq8.as_ref());
    }
}
