//! VectorCollection lifecycle: insert, delete, seal, complete_build, compact.
//!
//! Identity model: every vector inserted into the collection is bound to
//! a global `Surrogate` allocated by the Control Plane before the engine
//! sees the call. The HNSW segments keep their dense local node-ids
//! internally for cache-locality and SIMD traversal; this file owns the
//! `surrogate_map: HashMap<u32, Surrogate>` (global node-id → surrogate)
//! and reverse `surrogate_to_local: HashMap<Surrogate, u32>` (for
//! point-delete by surrogate). User-PK strings live in the catalog and
//! are translated at the Control Plane response boundary.

use std::collections::HashMap;

use nodedb_types::{Surrogate, VectorQuantization};

use crate::flat::FlatIndex;
use crate::hnsw::{HnswIndex, HnswParams};
use crate::index_config::{IndexConfig, IndexType};

use super::codec_dispatch::CollectionCodec;
use super::payload_index::PayloadIndexSet;
use super::segment::{BuildRequest, BuildingSegment, DEFAULT_SEAL_THRESHOLD, SealedSegment};

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
    /// Optional collection-level codec-dispatch index (RaBitQ or BBQ).
    /// Present only when the collection was built with a non-Sq8 quantization.
    /// Coexists with sealed segments — for codec-dispatched collections the
    /// per-segment Sq8 builder is skipped and this index is used instead.
    pub codec_dispatch: Option<CollectionCodec>,
    /// Quantization mode requested at collection-creation time.
    ///
    /// When `!= None && != Sq8`, each call to `complete_build` additionally
    /// rebuilds `codec_dispatch` over all vectors so the codec-dispatch path
    /// is always up-to-date after a segment seals.
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
            codec_dispatch: None,
            quantization: VectorQuantization::default(),
            payload: PayloadIndexSet::default(),
            arena_index: None,
        }
    }

    /// Create with a specific seed (for deterministic testing).
    pub fn with_seed(dim: usize, params: HnswParams, _seed: u64) -> Self {
        Self::with_seal_threshold(dim, params, DEFAULT_SEAL_THRESHOLD)
    }

    /// Insert a vector. Returns the global vector ID.
    pub fn insert(&mut self, vector: Vec<f32>) -> u32 {
        let id = self.next_id;
        self.growing.insert(vector);
        self.next_id += 1;
        id
    }

    /// Insert a vector with an associated surrogate. The surrogate is
    /// allocated by the Control Plane before the call; the engine only
    /// stores the binding.
    pub fn insert_with_surrogate(&mut self, vector: Vec<f32>, surrogate: Surrogate) -> u32 {
        let id = self.insert(vector);
        if surrogate != Surrogate::ZERO {
            self.surrogate_map.insert(id, surrogate);
            self.surrogate_to_local.insert(surrogate, id);
        }
        id
    }

    /// Insert multiple vectors for a single document (ColBERT-style).
    /// All N vectors are bound to the same `document_surrogate`.
    pub fn insert_multi_vector(
        &mut self,
        vectors: &[&[f32]],
        document_surrogate: Surrogate,
    ) -> Vec<u32> {
        let mut ids = Vec::with_capacity(vectors.len());
        for &v in vectors {
            let id = self.insert(v.to_vec());
            if document_surrogate != Surrogate::ZERO {
                self.surrogate_map.insert(id, document_surrogate);
            }
            ids.push(id);
        }
        if document_surrogate != Surrogate::ZERO {
            self.multi_doc_map.insert(document_surrogate, ids.clone());
        }
        ids
    }

    /// Delete all vectors belonging to a multi-vector document.
    pub fn delete_multi_vector(&mut self, document_surrogate: Surrogate) -> usize {
        let Some(ids) = self.multi_doc_map.remove(&document_surrogate) else {
            return 0;
        };
        let mut deleted = 0;
        for id in &ids {
            if self.delete(*id) {
                deleted += 1;
            }
            self.surrogate_map.remove(id);
        }
        self.surrogate_to_local.remove(&document_surrogate);
        deleted
    }

    /// Look up the surrogate for a global vector ID.
    pub fn get_surrogate(&self, vector_id: u32) -> Option<Surrogate> {
        self.surrogate_map.get(&vector_id).copied()
    }

    /// Resolve a surrogate back to its global vector ID, if bound.
    pub fn local_for_surrogate(&self, surrogate: Surrogate) -> Option<u32> {
        self.surrogate_to_local.get(&surrogate).copied()
    }

    /// Soft-delete a vector by global ID.
    pub fn delete(&mut self, id: u32) -> bool {
        let ok = self.delete_inner(id);
        if ok && let Some(s) = self.surrogate_map.remove(&id) {
            self.surrogate_to_local.remove(&s);
        }
        ok
    }

    fn delete_inner(&mut self, id: u32) -> bool {
        if id >= self.growing_base_id {
            let local = id - self.growing_base_id;
            if (local as usize) < self.growing.len() {
                return self.growing.delete(local);
            }
        }
        for seg in &mut self.sealed {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.index.len() {
                    return seg.index.delete(local);
                }
            }
        }
        for seg in &mut self.building {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.flat.len() {
                    return seg.flat.delete(local);
                }
            }
        }
        false
    }

    /// Soft-delete a vector by surrogate.
    pub fn delete_by_surrogate(&mut self, surrogate: Surrogate) -> bool {
        let Some(global_id) = self.surrogate_to_local.get(&surrogate).copied() else {
            return false;
        };
        self.delete(global_id)
    }

    /// Un-delete a previously soft-deleted vector (for transaction rollback).
    pub fn undelete(&mut self, id: u32) -> bool {
        for seg in &mut self.sealed {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.index.len() {
                    return seg.index.undelete(local);
                }
            }
        }
        false
    }

    /// Check if the growing segment should be sealed.
    pub fn needs_seal(&self) -> bool {
        self.growing.len() >= self.seal_threshold
    }

    /// Seal the growing segment and return a build request.
    pub fn seal(&mut self, key: &str) -> Option<BuildRequest> {
        if self.growing.is_empty() {
            return None;
        }

        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        let count = self.growing.len();
        let mut vectors = Vec::with_capacity(count);
        for i in 0..count as u32 {
            if let Some(v) = self.growing.get_vector(i) {
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
            vectors,
            dim: self.dim,
            params: self.params.clone(),
        })
    }

    /// Accept a completed HNSW build from the background thread.
    ///
    /// After promoting the segment to sealed, rebuilds the collection-level
    /// codec-dispatch index when `self.quantization` is `RaBitQ` or `Bbq`.
    /// The rebuild trains over all vectors so the codec index always covers
    /// every sealed segment.
    pub fn complete_build(&mut self, segment_id: u32, index: HnswIndex) {
        if let Some(pos) = self
            .building
            .iter()
            .position(|b| b.segment_id == segment_id)
        {
            let building = self.building.remove(pos);
            // For codec-dispatched collections (RaBitQ/BBQ), skip per-segment
            // Sq8/PQ quantization — the codec index handles traversal.
            let use_codec_dispatch = matches!(
                self.quantization,
                VectorQuantization::RaBitQ | VectorQuantization::Bbq
            );
            let use_pq = !use_codec_dispatch && self.index_config.index_type == IndexType::HnswPq;
            let (sq8, pq) = if use_codec_dispatch {
                (None, None)
            } else if use_pq {
                (
                    None,
                    Self::build_pq_for_index(&index, self.index_config.pq_m),
                )
            } else {
                (Self::build_sq8_for_index(&index), None)
            };
            let (tier, mmap_vectors) = self.resolve_tier_for_build(segment_id, &index);

            self.sealed.push(SealedSegment {
                index,
                base_id: building.base_id,
                sq8,
                pq,
                tier,
                mmap_vectors,
            });

            // Rebuild the collection-level codec index to include the new segment.
            if use_codec_dispatch {
                let tag = match self.quantization {
                    VectorQuantization::RaBitQ => "rabitq",
                    VectorQuantization::Bbq => "bbq",
                    _ => unreachable!(),
                };
                self.build_codec_dispatch(tag);
            }
        }
    }

    /// Access sealed segments (read-only).
    pub fn sealed_segments(&self) -> &[SealedSegment] {
        &self.sealed
    }

    /// Access sealed segments mutably.
    pub fn sealed_segments_mut(&mut self) -> &mut Vec<SealedSegment> {
        &mut self.sealed
    }

    /// Whether the growing segment has no vectors.
    pub fn growing_is_empty(&self) -> bool {
        self.growing.is_empty()
    }

    /// Compact sealed segments by removing tombstoned nodes.
    ///
    /// Rewrites `surrogate_map` and `multi_doc_map` for every sealed
    /// segment so that global ids continue to resolve to the correct
    /// surrogate after local-id renumbering.
    pub fn compact(&mut self) -> usize {
        let mut total_removed = 0;
        for seg in &mut self.sealed {
            let base_id = seg.base_id;
            let (removed, id_map) = seg.index.compact_with_map();
            total_removed += removed;
            if removed == 0 {
                continue;
            }

            let segment_end = base_id as u64 + id_map.len() as u64;
            let global_keys: Vec<u32> = self
                .surrogate_map
                .keys()
                .copied()
                .filter(|&k| (k as u64) >= base_id as u64 && (k as u64) < segment_end)
                .collect();
            // Two-phase: remove old entries first, then insert new ones
            // so we don't clobber a freshly-remapped entry with a later
            // tombstone removal.
            let mut new_entries: Vec<(u32, Surrogate)> = Vec::with_capacity(global_keys.len());
            for old_global in &global_keys {
                let surrogate = self.surrogate_map.remove(old_global);
                let old_local = (old_global - base_id) as usize;
                let new_local = id_map[old_local];
                if new_local != u32::MAX
                    && let Some(s) = surrogate
                {
                    new_entries.push((base_id + new_local, s));
                } else if let Some(s) = surrogate {
                    // Tombstoned — drop reverse mapping too.
                    self.surrogate_to_local.remove(&s);
                }
            }
            for (k, s) in new_entries {
                self.surrogate_map.insert(k, s);
                self.surrogate_to_local.insert(s, k);
            }

            // Rewrite multi_doc_map entries for this segment.
            for ids in self.multi_doc_map.values_mut() {
                ids.retain_mut(|vid| {
                    let v = *vid;
                    if (v as u64) >= base_id as u64 && (v as u64) < segment_end {
                        let old_local = (v - base_id) as usize;
                        let new_local = id_map[old_local];
                        if new_local == u32::MAX {
                            false
                        } else {
                            *vid = base_id + new_local;
                            true
                        }
                    } else {
                        true
                    }
                });
            }
        }
        total_removed
    }

    /// Export all live vectors for snapshot.
    pub fn export_snapshot(&self) -> Vec<(u32, Vec<f32>, Option<Surrogate>)> {
        let mut result = Vec::new();

        for i in 0..self.growing.len() as u32 {
            let vid = self.growing_base_id + i;
            if let Some(data) = self.growing.get_vector(i) {
                let surrogate = self.surrogate_map.get(&vid).copied();
                result.push((vid, data.to_vec(), surrogate));
            }
        }

        for seg in &self.sealed {
            let vectors = seg.index.export_vectors();
            for (i, vec_data) in vectors.into_iter().enumerate() {
                let vid = seg.base_id + i as u32;
                let surrogate = self.surrogate_map.get(&vid).copied();
                result.push((vid, vec_data, surrogate));
            }
        }

        for seg in &self.building {
            for i in 0..seg.flat.len() as u32 {
                let vid = seg.base_id + i;
                if let Some(data) = seg.flat.get_vector(i) {
                    let surrogate = self.surrogate_map.get(&vid).copied();
                    result.push((vid, data.to_vec(), surrogate));
                }
            }
        }

        result
    }

    pub fn len(&self) -> usize {
        let mut total = self.growing.len();
        for seg in &self.sealed {
            total += seg.index.len();
        }
        for seg in &self.building {
            total += seg.flat.len();
        }
        total
    }

    pub fn live_count(&self) -> usize {
        let mut total = self.growing.live_count();
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
    ///
    /// Called when a vector-primary collection is first created so the
    /// quantization is stored in checkpoints and drives codec-dispatch
    /// rebuilds after each `complete_build`.
    pub fn set_quantization(&mut self, q: VectorQuantization) {
        self.quantization = q;
    }

    /// Return the configured quantization mode.
    pub fn quantization(&self) -> VectorQuantization {
        self.quantization
    }

    /// Configure payload bitmap indexes from a list of field names.
    ///
    /// Uses `PayloadIndexKind::Equality` for all fields (range support can be
    /// added later without breaking the checkpoint format). Idempotent.
    pub fn configure_payload_indexes(&mut self, fields: &[String]) {
        use super::payload_index::PayloadIndexKind;
        for field in fields {
            self.payload
                .add_index(field.as_str(), PayloadIndexKind::Equality);
        }
    }
}
