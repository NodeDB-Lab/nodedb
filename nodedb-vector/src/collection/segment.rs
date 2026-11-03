// SPDX-License-Identifier: Apache-2.0

//! Segment types for the VectorCollection lifecycle.

use nodedb_mem::ScopedMemory;

use crate::collection::segment_codec::{CodecCharge, CodecPlan, SegmentCodec};
use crate::collection::tier::StorageTier;
use crate::error::VectorError;
use crate::flat::FlatIndex;
use crate::hnsw::{HnswIndex, HnswParams};
use crate::mmap_segment::MmapVectorSegment;

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

/// Request to build an HNSW index and its codec (sent to the builder thread).
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
    /// The codec the built segment carries.
    pub codec: CodecPlan,
    /// Budget the codec training allocations are charged to.
    pub memory: ScopedMemory,
}

/// A built graph and the outcome of its codec build.
pub struct BuiltSegment {
    pub index: HnswIndex,
    /// The built codec, or the error that stopped its build. A segment whose
    /// codec failed installs its graph with no codec and answers exactly.
    pub codec: Result<SegmentCodec, VectorError>,
    /// Reservations that cover the codec until the owning core installs it.
    pub charge: CodecCharge,
    /// The request's budget. Installing the segment charges an mmap tier
    /// spill to it.
    pub memory: ScopedMemory,
}

/// What installing a build did.
#[derive(Debug)]
#[must_use]
pub enum InstallOutcome {
    /// No segment matched the build: nothing changed.
    Refused,
    /// The sealed segment at `base_id` holds the built graph. `codec_error`
    /// is the error that stopped its codec build: the segment then carries
    /// no codec.
    Installed {
        base_id: u32,
        codec_error: Option<VectorError>,
    },
}

impl InstallOutcome {
    /// Whether the build was installed.
    pub fn installed(&self) -> bool {
        match self {
            Self::Refused => false,
            Self::Installed { .. } => true,
        }
    }
}

/// Completed HNSW build (sent back from builder thread).
pub struct BuildComplete {
    pub key: String,
    pub segment_id: u32,
    pub kind: BuildKind,
    /// The built segment, or the error that stopped its graph build. A
    /// failed graph build leaves the segment as it was. A codec error is
    /// carried in [`BuiltSegment::codec`].
    pub result: Result<BuiltSegment, VectorError>,
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
    /// The codec the collection's plan built for this segment.
    pub codec: SegmentCodec,
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
    /// Bytes `index` and `codec` hold in RAM. Walks every node.
    pub fn measure(index: &HnswIndex, codec: &SegmentCodec) -> usize {
        index.memory_usage_bytes() + codec.resident_bytes()
    }

    /// Measure the segment again after its index or codec changed.
    pub fn remeasure(&mut self) {
        self.resident_bytes = Self::measure(&self.index, &self.codec);
    }
}
