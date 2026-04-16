//! VectorCollection lifecycle: insert, delete, seal, complete_build, compact.

use crate::flat::FlatIndex;
use crate::hnsw::{HnswIndex, HnswParams};
use crate::index_config::{IndexConfig, IndexType};

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
    /// Mapping from internal vector ID → user-facing document ID.
    pub doc_id_map: std::collections::HashMap<u32, String>,
    /// Reverse mapping for multi-vector documents: doc_id → list of vector IDs.
    pub multi_doc_map: std::collections::HashMap<String, Vec<u32>>,
    /// Number of vectors in the growing segment before sealing.
    pub(crate) seal_threshold: usize,
    /// Full index configuration (index type, PQ params, IVF params).
    pub(crate) index_config: IndexConfig,
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
            doc_id_map: std::collections::HashMap::new(),
            multi_doc_map: std::collections::HashMap::new(),
            seal_threshold,
            index_config: config,
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

    /// Insert a vector with an associated document ID.
    pub fn insert_with_doc_id(&mut self, vector: Vec<f32>, doc_id: String) -> u32 {
        let id = self.insert(vector);
        self.doc_id_map.insert(id, doc_id);
        id
    }

    /// Insert multiple vectors for a single document (ColBERT-style).
    pub fn insert_multi_vector(&mut self, vectors: &[&[f32]], doc_id: String) -> Vec<u32> {
        let mut ids = Vec::with_capacity(vectors.len());
        for &v in vectors {
            let id = self.insert(v.to_vec());
            self.doc_id_map.insert(id, doc_id.clone());
            ids.push(id);
        }
        self.multi_doc_map.insert(doc_id, ids.clone());
        ids
    }

    /// Delete all vectors belonging to a multi-vector document.
    pub fn delete_multi_vector(&mut self, doc_id: &str) -> usize {
        let Some(ids) = self.multi_doc_map.remove(doc_id) else {
            return 0;
        };
        let mut deleted = 0;
        for id in &ids {
            if self.delete(*id) {
                deleted += 1;
            }
            self.doc_id_map.remove(id);
        }
        deleted
    }

    /// Look up the document ID for a vector ID.
    pub fn get_doc_id(&self, vector_id: u32) -> Option<&str> {
        self.doc_id_map.get(&vector_id).map(|s| s.as_str())
    }

    /// Soft-delete a vector by global ID.
    pub fn delete(&mut self, id: u32) -> bool {
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
    pub fn complete_build(&mut self, segment_id: u32, index: HnswIndex) {
        if let Some(pos) = self
            .building
            .iter()
            .position(|b| b.segment_id == segment_id)
        {
            let building = self.building.remove(pos);
            let use_pq = self.index_config.index_type == IndexType::HnswPq;
            let (sq8, pq) = if use_pq {
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
    /// Rewrites `doc_id_map` and `multi_doc_map` for every sealed segment
    /// so that global ids continue to resolve to the correct document
    /// strings after local-id renumbering.
    pub fn compact(&mut self) -> usize {
        let mut total_removed = 0;
        for seg in &mut self.sealed {
            let base_id = seg.base_id;
            let (removed, id_map) = seg.index.compact_with_map();
            total_removed += removed;
            if removed == 0 {
                continue;
            }

            // Rebuild doc_id_map for entries in [base_id, base_id + id_map.len()).
            let segment_end = base_id as u64 + id_map.len() as u64;
            let doc_keys: Vec<u32> = self
                .doc_id_map
                .keys()
                .copied()
                .filter(|&k| (k as u64) >= base_id as u64 && (k as u64) < segment_end)
                .collect();
            // Two-phase: remove all old entries first, then insert new ones so
            // we don't clobber a freshly-remapped entry with a later tombstone
            // removal.
            let mut new_entries: Vec<(u32, String)> = Vec::with_capacity(doc_keys.len());
            for old_global in &doc_keys {
                let doc = self.doc_id_map.remove(old_global);
                let old_local = (old_global - base_id) as usize;
                let new_local = id_map[old_local];
                if new_local != u32::MAX
                    && let Some(doc) = doc
                {
                    new_entries.push((base_id + new_local, doc));
                }
            }
            for (k, v) in new_entries {
                self.doc_id_map.insert(k, v);
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
    pub fn export_snapshot(&self) -> Vec<(u32, Vec<f32>, Option<String>)> {
        let mut result = Vec::new();

        for i in 0..self.growing.len() as u32 {
            let vid = self.growing_base_id + i;
            if let Some(data) = self.growing.get_vector(i) {
                let doc_id = self.doc_id_map.get(&vid).cloned();
                result.push((vid, data.to_vec(), doc_id));
            }
        }

        for seg in &self.sealed {
            let vectors = seg.index.export_vectors();
            for (i, vec_data) in vectors.into_iter().enumerate() {
                let vid = seg.base_id + i as u32;
                let doc_id = self.doc_id_map.get(&vid).cloned();
                result.push((vid, vec_data, doc_id));
            }
        }

        for seg in &self.building {
            for i in 0..seg.flat.len() as u32 {
                let vid = seg.base_id + i;
                if let Some(data) = seg.flat.get_vector(i) {
                    let doc_id = self.doc_id_map.get(&vid).cloned();
                    result.push((vid, data.to_vec(), doc_id));
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

    /// Collect live statistics from all segments.
    pub fn stats(&self) -> nodedb_types::VectorIndexStats {
        let growing_vectors = self.growing.len();
        let sealed_vectors: usize = self.sealed.iter().map(|s| s.index.len()).sum();
        let building_vectors: usize = self.building.iter().map(|s| s.flat.len()).sum();

        let tombstone_count: usize = self
            .sealed
            .iter()
            .map(|s| s.index.tombstone_count())
            .sum::<usize>()
            + self.growing.tombstone_count()
            + self
                .building
                .iter()
                .map(|s| s.flat.tombstone_count())
                .sum::<usize>();

        let total = growing_vectors + sealed_vectors + building_vectors;
        let tombstone_ratio = if total > 0 {
            tombstone_count as f64 / total as f64
        } else {
            0.0
        };

        let quantization = if self.sealed.iter().any(|s| s.pq.is_some()) {
            nodedb_types::VectorIndexQuantization::Pq
        } else if self.sealed.iter().any(|s| s.sq8.is_some()) {
            nodedb_types::VectorIndexQuantization::Sq8
        } else {
            nodedb_types::VectorIndexQuantization::None
        };

        let index_type = match self.index_config.index_type {
            IndexType::HnswPq => nodedb_types::VectorIndexType::HnswPq,
            IndexType::IvfPq => nodedb_types::VectorIndexType::IvfPq,
            IndexType::Hnsw => nodedb_types::VectorIndexType::Hnsw,
        };

        let hnsw_mem: usize = self
            .sealed
            .iter()
            .map(|s| s.index.memory_usage_bytes())
            .sum();
        let sq8_mem: usize = self
            .sealed
            .iter()
            .filter_map(|s| s.sq8.as_ref().map(|(_, data)| data.len()))
            .sum();
        let growing_mem = growing_vectors * self.dim * std::mem::size_of::<f32>();
        let building_mem = building_vectors * self.dim * std::mem::size_of::<f32>();
        let memory_bytes = hnsw_mem + sq8_mem + growing_mem + building_mem;

        let disk_bytes: usize = self
            .sealed
            .iter()
            .filter_map(|s| s.mmap_vectors.as_ref().map(|m| m.file_size()))
            .sum();

        let metric_name = format!("{:?}", self.params.metric).to_lowercase();

        nodedb_types::VectorIndexStats {
            sealed_count: self.sealed.len(),
            building_count: self.building.len(),
            growing_vectors,
            sealed_vectors,
            live_count: self.live_count(),
            tombstone_count,
            tombstone_ratio,
            quantization,
            memory_bytes,
            disk_bytes,
            build_in_progress: !self.building.is_empty(),
            index_type,
            hnsw_m: self.params.m,
            hnsw_m0: self.params.m0,
            hnsw_ef_construction: self.params.ef_construction,
            metric: metric_name,
            dimensions: self.dim,
            seal_threshold: self.seal_threshold,
            mmap_segment_count: self.mmap_segment_count,
        }
    }
}
