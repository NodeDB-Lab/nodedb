// SPDX-License-Identifier: Apache-2.0

//! VectorCollection lifecycle: construction, seal, counts and settings.
//!
//! Installing finished builds lives in `build`.
//!
//! Identity model: every vector inserted into the collection is bound to
//! a global `Surrogate` allocated by the Control Plane before the engine
//! sees the call. The HNSW segments keep their dense local node-ids
//! internally for cache-locality and SIMD traversal; this file owns the
//! `surrogate_map: HashMap<u32, Surrogate>` (global node-id → surrogate)
//! and reverse `surrogate_to_local: HashMap<Surrogate, u32>` (for
//! point-delete by surrogate). User-PK strings live in the catalog and
//! are translated at the Control Plane response boundary.
//!
//! Insert/delete ops live in `lifecycle_insert_ops`.
//! Compact/snapshot ops live in `lifecycle_compact`.

use std::collections::HashMap;

use nodedb_mem::ScopedMemory;
use nodedb_types::{Surrogate, VectorQuantization};

use crate::flat::FlatIndex;
use crate::hnsw::HnswParams;
use crate::index_config::IndexConfig;
use crate::ivf::IvfPqIndex;

use super::payload_index::PayloadIndexSet;
use super::segment::{
    BuildKind, BuildRequest, BuildingSegment, DEFAULT_SEAL_THRESHOLD, SealedSegment,
};

/// Manages all vector segments for a single collection (one index key).
///
/// This type is `!Send` — owned by a single Data Plane core.
pub struct VectorCollection {
    /// Active growing segment (append-only, brute-force search).
    pub(crate) growing: FlatIndex,
    /// Base ID for the growing segment's vectors.
    pub(crate) growing_base_id: u32,
    /// Sealed segments with completed HNSW indexes.
    pub(crate) sealed: Vec<SealedSegment>,
    /// Segments being built in background (brute-force searchable).
    pub(crate) building: Vec<BuildingSegment>,
    /// HNSW params for this collection.
    pub(crate) params: HnswParams,
    /// Global vector ID counter (monotonic across all segments).
    pub(crate) next_id: u32,
    /// Next segment ID (monotonic).
    pub(crate) next_segment_id: u32,
    /// Dimensionality.
    pub(crate) dim: usize,
    /// Data directory for mmap segment files (L1 NVMe tier).
    pub(crate) data_dir: Option<std::path::PathBuf>,
    /// Memory budget for this collection's RAM vectors (bytes).
    pub(crate) ram_budget_bytes: usize,
    /// Count of segments that fell back to mmap due to budget exhaustion.
    pub(crate) mmap_fallback_count: u32,
    /// Count of segments currently backed by mmap files.
    pub(crate) mmap_segment_count: u32,
    /// Mapping from internal global vector ID → surrogate.
    pub surrogate_map: HashMap<u32, Surrogate>,
    /// Reverse map: surrogate → global vector ID. Used by point delete.
    pub surrogate_to_local: HashMap<Surrogate, u32>,
    /// Reverse mapping for multi-vector documents:
    /// document_surrogate → list of global vector IDs.
    pub multi_doc_map: HashMap<Surrogate, Vec<u32>>,
    /// Number of vectors in the growing segment before sealing.
    pub(crate) seal_threshold: usize,
    /// Full index configuration (index type, PQ params, IVF params).
    pub(crate) index_config: IndexConfig,
    /// Trained IVF-PQ index of an `IvfPq` collection. `None` until the
    /// collection holds the training threshold of vectors: until then its
    /// vectors wait in the growing segment, searched exactly. Training moves
    /// every vector into this index under its global id, and later inserts
    /// land here.
    pub(crate) ivf: Option<IvfPqIndex>,
    /// Quantization requested when the collection was created. `codec_plan`
    /// maps it to the codec each sealed segment carries.
    pub(crate) quantization: VectorQuantization,
    /// In-memory payload bitmap indexes for vector-primary collections.
    ///
    /// Empty (no indexes) by default; populated at construction time from
    /// `VectorPrimaryConfig::payload_indexes`.
    pub payload: PayloadIndexSet,
    /// Optional dedicated memory arena index for this collection.
    ///
    /// Set by the Data Plane after requesting a per-collection arena from
    /// `nodedb_mem::CollectionArenaRegistry`. Used only for stats reporting;
    /// the actual arena pinning is handled externally.
    pub arena_index: Option<u32>,
    /// Highest WAL LSN whose write is already reflected in this collection's
    /// in-memory state, as of the last checkpoint load or save. Persisted in
    /// the checkpoint so that startup WAL replay can skip records the
    /// restored checkpoint already absorbed — the same watermark discipline
    /// the timeseries engine applies via `last_flushed_wal_lsn`. Set ONLY by
    /// checkpoint load (restore) and checkpoint save; it is FROZEN during a
    /// replay pass so all sub-records of a `TransactionRedo` record — which
    /// share the enclosing record's single LSN — apply instead of the first
    /// one gating its siblings. Per-apply advancement lands in
    /// [`Self::applied_wal_lsn`] instead, and is folded into this field at
    /// checkpoint save time. `0` means "no write recorded" (a fresh
    /// collection), which never gates replay.
    pub(crate) checkpoint_wal_lsn: u64,
    /// Running max of WAL LSNs applied to this collection's in-memory state
    /// since the last checkpoint load. Advanced by [`Self::note_checkpoint_lsn`]
    /// at every live/replay apply chokepoint. NEVER read by the replay skip
    /// gate (that reads `checkpoint_wal_lsn`, which stays frozen during a replay
    /// pass so all sub-records of one transaction — which share the enclosing
    /// `TransactionRedo` record's single LSN — apply instead of the first one
    /// gating its siblings). Folded into the persisted watermark at checkpoint
    /// save time via `max(checkpoint_wal_lsn, applied_wal_lsn)`.
    pub(crate) applied_wal_lsn: u64,
    /// HNSW builds installed since the collection was opened.
    pub(crate) builds_completed: u64,
    /// HNSW builds that failed since the collection was opened.
    pub(crate) builds_failed: u64,
    /// Base ids of sealed segments whose codec build failed with an error a
    /// retry does not fix. [`Self::sealed_ids_missing_codec`] skips them, so
    /// the error is reported once. A plan change or a restart clears it.
    pub(crate) codec_refused: Vec<u32>,
    /// System-time spans of the nodes: which node of each row a search at a
    /// past cut reads (see `versions`).
    pub(crate) versions: super::versions::VectorVersions,
}

impl VectorCollection {
    /// Create an empty collection with the default seal threshold.
    pub fn new(dim: usize, params: HnswParams) -> Self {
        Self::with_seal_threshold(dim, params, DEFAULT_SEAL_THRESHOLD)
    }

    /// Create an empty collection with an explicit seal threshold.
    pub fn with_seal_threshold(dim: usize, params: HnswParams, seal_threshold: usize) -> Self {
        let index_config = IndexConfig {
            hnsw: params.clone(),
            ..IndexConfig::default()
        };
        Self::with_seal_threshold_and_config(dim, index_config, seal_threshold)
    }

    /// Create an empty collection with a full index configuration.
    pub fn with_index_config(dim: usize, config: IndexConfig) -> Self {
        Self::with_seal_threshold_and_config(dim, config, DEFAULT_SEAL_THRESHOLD)
    }

    /// Create an empty collection with a full index config and custom seal threshold.
    pub fn with_seal_threshold_and_config(
        dim: usize,
        config: IndexConfig,
        seal_threshold: usize,
    ) -> Self {
        let params = config.hnsw.clone();
        Self {
            growing: FlatIndex::new(dim, params.metric),
            growing_base_id: 0,
            sealed: Vec::new(),
            building: Vec::new(),
            params,
            next_id: 0,
            next_segment_id: 0,
            dim,
            data_dir: None,
            ram_budget_bytes: 0,
            mmap_fallback_count: 0,
            mmap_segment_count: 0,
            surrogate_map: HashMap::new(),
            surrogate_to_local: HashMap::new(),
            multi_doc_map: HashMap::new(),
            seal_threshold,
            index_config: config,
            ivf: None,
            quantization: VectorQuantization::default(),
            payload: PayloadIndexSet::default(),
            arena_index: None,
            checkpoint_wal_lsn: 0,
            applied_wal_lsn: 0,
            builds_completed: 0,
            builds_failed: 0,
            codec_refused: Vec::new(),
            versions: super::versions::VectorVersions::default(),
        }
    }

    /// Advance the applied-watermark to `lsn` if it is higher than the
    /// current value. Called at every apply chokepoint (live write and WAL
    /// replay) with the WAL LSN of the applied write. `0` is ignored — an
    /// unassigned LSN must never move the watermark. This does NOT move the
    /// replay skip gate (`checkpoint_wal_lsn`); it is folded into that gate
    /// only at checkpoint save time via `max(checkpoint_wal_lsn,
    /// applied_wal_lsn)`, so the gate stays frozen across an entire replay
    /// pass and doesn't gate a `TransactionRedo` record's own siblings.
    pub fn note_checkpoint_lsn(&mut self, lsn: u64) {
        if lsn > self.applied_wal_lsn {
            self.applied_wal_lsn = lsn;
        }
    }

    /// Highest WAL LSN already reflected in this collection's in-memory state,
    /// as of the last checkpoint load or save.
    ///
    /// Startup replay skips any record whose `lsn <= checkpoint_wal_lsn()` for
    /// this collection: the restored checkpoint already contains that write, so
    /// re-applying it would append a duplicate HNSW node.
    pub fn checkpoint_wal_lsn(&self) -> u64 {
        self.checkpoint_wal_lsn
    }

    /// Highest WAL LSN applied since the last checkpoint load (the running max
    /// `note_checkpoint_lsn` builds). Folded into the persisted watermark at
    /// checkpoint save time; not consulted by the replay gate.
    pub fn applied_wal_lsn(&self) -> u64 {
        self.applied_wal_lsn
    }

    /// Create with a specific seed (for deterministic testing).
    pub fn with_seed(dim: usize, params: HnswParams, _seed: u64) -> Self {
        Self::with_seal_threshold(dim, params, DEFAULT_SEAL_THRESHOLD)
    }

    /// Set the growing-segment size that triggers a seal. A zero threshold
    /// is raised to one vector.
    pub fn set_seal_threshold(&mut self, threshold: usize) {
        self.seal_threshold = threshold.max(1);
    }

    /// Check if the growing segment should be sealed. An `IvfPq` collection
    /// never seals: its growing segment is the buffer IVF-PQ training reads.
    pub fn needs_seal(&self) -> bool {
        !self.is_ivf() && self.growing.len() >= self.seal_threshold
    }

    /// Seal the growing segment and return a build request whose codec
    /// training is charged to `memory`. `None` for an empty growing segment
    /// or an `IvfPq` collection.
    pub fn seal(&mut self, key: &str, memory: ScopedMemory) -> Option<BuildRequest> {
        if self.growing.is_empty() || self.is_ivf() {
            return None;
        }

        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        // Soft-deleted vectors go too: the built graph keeps every local id,
        // and the tombstones are applied when the build is installed.
        let count = self.growing.len();
        let mut vectors = Vec::with_capacity(count);
        for i in 0..count as u32 {
            if let Some(v) = self.growing.get_vector_raw(i) {
                vectors.push(v.to_vec());
            }
        }

        let old_growing = std::mem::replace(
            &mut self.growing,
            FlatIndex::new(self.dim, self.params.metric),
        );
        let old_base = self.growing_base_id;
        self.growing_base_id = self.next_id;

        self.building.push(BuildingSegment {
            flat: old_growing,
            base_id: old_base,
            segment_id,
        });

        Some(BuildRequest {
            key: key.to_string(),
            segment_id,
            kind: BuildKind::Seal,
            vectors,
            dim: self.dim,
            params: self.params.clone(),
            codec: self.codec_plan(),
            memory,
        })
    }

    /// Access sealed segments (read-only).
    pub fn sealed_segments(&self) -> &[SealedSegment] {
        &self.sealed
    }

    /// Access sealed segments mutably.
    pub fn sealed_segments_mut(&mut self) -> &mut Vec<SealedSegment> {
        &mut self.sealed
    }

    /// Whether every vector is searched by a flat scan: no sealed HNSW
    /// segment and no IVF index. A flat scan ranks all vectors whatever
    /// `top_k` is, so asking for more candidates costs nothing extra.
    pub fn scans_flat_only(&self) -> bool {
        self.sealed.is_empty() && self.ivf.is_none()
    }

    /// Whether the growing segment has no vectors.
    pub fn growing_is_empty(&self) -> bool {
        self.growing.is_empty()
    }

    pub fn len(&self) -> usize {
        let mut total = self.growing.len() + self.ivf.as_ref().map_or(0, IvfPqIndex::len);
        for seg in &self.sealed {
            total += seg.index.len();
        }
        for seg in &self.building {
            total += seg.flat.len();
        }
        total
    }

    pub fn live_count(&self) -> usize {
        let mut total =
            self.growing.live_count() + self.ivf.as_ref().map_or(0, IvfPqIndex::live_count);
        for seg in &self.sealed {
            total += seg.index.live_count();
        }
        for seg in &self.building {
            total += seg.flat.live_count();
        }
        total
    }

    pub fn is_empty(&self) -> bool {
        self.live_count() == 0
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    /// Update HNSW parameters for future builds.
    pub fn set_params(&mut self, params: HnswParams) {
        self.params = params;
    }

    /// Set the collection-level quantization.
    pub fn set_quantization(&mut self, q: VectorQuantization) {
        self.quantization = q;
        // A new plan gets a fresh codec build on every segment.
        self.codec_refused.clear();
    }

    /// Return the configured quantization mode.
    pub fn quantization(&self) -> VectorQuantization {
        self.quantization
    }

    /// Configure payload bitmap indexes from a list of field names.
    pub fn configure_payload_indexes(&mut self, fields: &[String]) {
        use super::payload_index::PayloadIndexKind;
        for field in fields {
            self.payload
                .add_index(field.as_str(), PayloadIndexKind::Equality);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_collection_with_no_sealed_segment_scans_flat() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        assert!(coll.scans_flat_only());
        for i in 0..4u32 {
            coll.insert(vec![i as f32, 0.0]).expect("insert");
        }
        assert!(coll.scans_flat_only());
        assert!(
            coll.seal("seg", crate::test_support::test_memory())
                .is_some()
        );
        assert!(
            coll.scans_flat_only(),
            "a segment that is still building is searched flat"
        );
    }
}
