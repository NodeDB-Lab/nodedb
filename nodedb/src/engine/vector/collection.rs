//! Segment-based vector collection with growing/sealed lifecycle.
//!
//! Each collection manages:
//! - One **growing segment** (FlatIndex, append-only, brute-force searchable)
//! - Zero or more **sealed segments** (HnswIndex, immutable, graph-searchable)
//! - A build queue for pending HNSW constructions
//!
//! Inserts land in the growing segment (O(1) append). When it reaches
//! `SEAL_THRESHOLD` vectors, it's sealed: vectors are frozen and HNSW
//! construction is dispatched to a background thread. The growing segment
//! is replaced with a fresh empty one. Queries probe all segments and
//! merge results by distance.

use super::distance::{DistanceMetric, distance};
use super::flat::FlatIndex;
use super::hnsw::{HnswIndex, HnswParams, SearchResult};
use super::mmap_segment::MmapVectorSegment;
use super::quantize::sq8::Sq8Codec;
use crate::storage::tier::StorageTier;

/// Threshold for sealing the growing segment.
/// 64K vectors × 768 dims × 4 bytes = ~192 MiB per segment.
pub const SEAL_THRESHOLD: usize = 65_536;

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
pub(super) struct BuildingSegment {
    /// Flat index for brute-force search while HNSW is building.
    pub(super) flat: FlatIndex,
    /// Base ID offset: vectors have global IDs [base_id .. base_id + count).
    pub(super) base_id: u32,
    /// Unique segment identifier (for matching with BuildComplete).
    pub(super) segment_id: u32,
}

/// A sealed segment with a completed HNSW index.
pub struct SealedSegment {
    /// Built HNSW index (immutable after construction).
    pub index: HnswIndex,
    /// Base ID offset.
    pub(super) base_id: u32,
    /// Optional SQ8 quantized vectors for accelerated traversal.
    /// Contiguous layout: `[v0_q0, v0_q1, ..., v1_q0, ...]` (dim bytes per vector).
    /// When present, search uses asymmetric SQ8 distance for candidate
    /// selection (4x fewer cache misses), then reranks top-K×3 with FP32.
    pub(super) sq8: Option<(Sq8Codec, Vec<u8>)>,
    /// Storage tier: L0Ram = FP32 vectors in HNSW nodes (default),
    /// L1Nvme = FP32 vectors in mmap segment file (budget-constrained).
    pub(super) tier: StorageTier,
    /// mmap-backed vector segment for L1 NVMe tier.
    /// When present, FP32 reranking reads from this instead of HnswIndex nodes.
    pub(super) mmap_vectors: Option<MmapVectorSegment>,
}

/// Manages all vector segments for a single collection (one index key).
///
/// This type is `!Send` — owned by a single Data Plane core.
pub struct VectorCollection {
    /// Active growing segment (append-only, brute-force search).
    pub(super) growing: FlatIndex,
    /// Base ID for the growing segment's vectors.
    pub(super) growing_base_id: u32,
    /// Sealed segments with completed HNSW indexes.
    pub(super) sealed: Vec<SealedSegment>,
    /// Segments being built in background (brute-force searchable).
    pub(super) building: Vec<BuildingSegment>,
    /// HNSW params for this collection.
    pub(super) params: HnswParams,
    /// Global vector ID counter (monotonic across all segments).
    pub(super) next_id: u32,
    /// Next segment ID (monotonic).
    pub(super) next_segment_id: u32,
    /// Dimensionality.
    pub(super) dim: usize,
    /// Data directory for mmap segment files (L1 NVMe tier).
    pub(super) data_dir: Option<std::path::PathBuf>,
    /// Memory budget for this collection's RAM vectors (bytes).
    /// 0 = unlimited (default). When exceeded, new sealed segments
    /// spill FP32 vectors to mmap files (L1 NVMe) instead of RAM.
    pub(super) ram_budget_bytes: usize,
    /// Count of segments that fell back to mmap due to budget exhaustion.
    pub(super) mmap_fallback_count: u32,
    /// Count of segments currently backed by mmap files.
    pub(super) mmap_segment_count: u32,
    /// Mapping from internal vector ID → user-facing document ID.
    /// Populated when vectors are inserted with an associated document ID.
    pub doc_id_map: std::collections::HashMap<u32, String>,
}

impl VectorCollection {
    /// Create an empty collection.
    pub fn new(dim: usize, params: HnswParams) -> Self {
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
        }
    }

    /// Create with a specific RNG-like seed (for deterministic testing).
    pub fn with_seed(dim: usize, params: HnswParams, _seed: u64) -> Self {
        Self::new(dim, params)
    }

    /// Insert a vector. Returns the global vector ID.
    pub fn insert(&mut self, vector: Vec<f32>) -> u32 {
        let id = self.next_id;
        self.growing.insert(vector);
        self.next_id += 1;
        id
    }

    /// Insert a vector with an associated document ID.
    /// The doc_id is stored in the mapping so search results can return
    /// human-readable IDs instead of internal numeric indices.
    pub fn insert_with_doc_id(&mut self, vector: Vec<f32>, doc_id: String) -> u32 {
        let id = self.insert(vector);
        self.doc_id_map.insert(id, doc_id);
        id
    }

    /// Look up the document ID for a vector ID, if one was provided at insert time.
    pub fn get_doc_id(&self, vector_id: u32) -> Option<&str> {
        self.doc_id_map.get(&vector_id).map(|s| s.as_str())
    }

    /// Soft-delete a vector by global ID.
    pub fn delete(&mut self, id: u32) -> bool {
        // Check growing segment.
        if id >= self.growing_base_id {
            let local = id - self.growing_base_id;
            if (local as usize) < self.growing.len() {
                return self.growing.delete(local);
            }
        }
        // Check sealed segments.
        for seg in &mut self.sealed {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.index.len() {
                    return seg.index.delete(local);
                }
            }
        }
        // Check building segments.
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
        // Only HNSW sealed segments support undelete.
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

    /// Search across all segments, merging results by distance.
    pub fn search(&self, query: &[f32], top_k: usize, ef: usize) -> Vec<SearchResult> {
        let mut all: Vec<SearchResult> = Vec::new();

        // Search growing segment (brute-force).
        let growing_results = self.growing.search(query, top_k);
        for mut r in growing_results {
            r.id += self.growing_base_id;
            all.push(r);
        }

        // Search sealed segments.
        for seg in &self.sealed {
            let results = if let Some((codec, sq8_data)) = &seg.sq8 {
                // Quantized two-phase search:
                // Phase 1: SQ8 asymmetric distance for candidate selection (4x faster).
                let rerank_k = top_k.saturating_mul(3).max(20);
                let mut candidates: Vec<(u32, f32)> = Vec::with_capacity(seg.index.len());
                let dim = seg.index.dim();
                for i in 0..seg.index.len() {
                    if seg.index.is_deleted(i as u32) {
                        continue;
                    }
                    let sq8_vec = &sq8_data[i * dim..(i + 1) * dim];
                    let d = match self.params.metric {
                        DistanceMetric::L2 => codec.asymmetric_l2(query, sq8_vec),
                        DistanceMetric::Cosine => codec.asymmetric_cosine(query, sq8_vec),
                        DistanceMetric::InnerProduct => codec.asymmetric_ip(query, sq8_vec),
                    };
                    candidates.push((i as u32, d));
                }
                if candidates.len() > rerank_k {
                    candidates.select_nth_unstable_by(rerank_k, |a, b| {
                        a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                    });
                    candidates.truncate(rerank_k);
                }

                // Phase 2: Rerank with exact FP32 distance.
                // For L1 NVMe tier, read FP32 from mmap segment file
                // instead of HNSW node vectors (which may be dummy/empty).
                let mut reranked: Vec<SearchResult> = candidates
                    .iter()
                    .filter_map(|&(id, _)| {
                        let v = if let Some(mmap) = &seg.mmap_vectors {
                            mmap.get_vector(id)?
                        } else {
                            seg.index.get_vector(id)?
                        };
                        Some(SearchResult {
                            id,
                            distance: distance(query, v, self.params.metric),
                        })
                    })
                    .collect();
                reranked.sort_by(|a, b| {
                    a.distance
                        .partial_cmp(&b.distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                reranked.truncate(top_k);
                reranked
            } else {
                // No quantization — standard HNSW search.
                seg.index.search(query, top_k, ef)
            };
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        // Search building segments (brute-force while HNSW builds).
        for seg in &self.building {
            let results = seg.flat.search(query, top_k);
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        // Merge: sort by distance, take top-k.
        all.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(top_k);
        all
    }

    /// Search with a pre-filter bitmap.
    pub fn search_with_bitmap_bytes(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
        bitmap: &[u8],
    ) -> Vec<SearchResult> {
        let mut all: Vec<SearchResult> = Vec::new();

        let growing_results = self.growing.search_filtered(query, top_k, bitmap);
        for mut r in growing_results {
            r.id += self.growing_base_id;
            all.push(r);
        }

        for seg in &self.sealed {
            let results = seg.index.search_with_bitmap_bytes(query, top_k, ef, bitmap);
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        for seg in &self.building {
            let results = seg.flat.search_filtered(query, top_k, bitmap);
            for mut r in results {
                r.id += seg.base_id;
                all.push(r);
            }
        }

        all.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(top_k);
        all
    }

    /// Check if the growing segment should be sealed.
    pub fn needs_seal(&self) -> bool {
        self.growing.len() >= SEAL_THRESHOLD
    }

    /// Seal the growing segment and return a build request for the builder thread.
    ///
    /// The growing segment is moved to the building list (still searchable via
    /// brute-force). A new empty growing segment is created. The returned
    /// `BuildRequest` should be sent to the background builder thread.
    pub fn seal(&mut self, key: &str) -> Option<BuildRequest> {
        if self.growing.is_empty() {
            return None;
        }

        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        // Extract vectors from the growing segment for HNSW construction.
        let count = self.growing.len();
        let mut vectors = Vec::with_capacity(count);
        for i in 0..count as u32 {
            if let Some(v) = self.growing.get_vector(i) {
                vectors.push(v.to_vec());
            }
        }

        // Move the growing FlatIndex to building list.
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
    /// Finds the matching building segment, replaces it with a sealed segment
    /// containing the built HNSW index. If the RAM budget is exceeded, FP32
    /// vectors are spilled to NVMe via mmap. The flat index is dropped.
    pub fn complete_build(&mut self, segment_id: u32, index: HnswIndex) {
        if let Some(pos) = self
            .building
            .iter()
            .position(|b| b.segment_id == segment_id)
        {
            let building = self.building.remove(pos);
            let sq8 = Self::build_sq8_for_index(&index);
            let (tier, mmap_vectors) = self.resolve_tier_for_build(segment_id, &index);

            self.sealed.push(SealedSegment {
                index,
                base_id: building.base_id,
                sq8,
                tier,
                mmap_vectors,
            });
        }
    }

    /// Build SQ8 quantized data for an HNSW index.
    ///
    /// Calibrates min/max from all live vectors, then quantizes each vector
    /// to INT8. Returns `None` if the index is too small to benefit from
    /// quantization (<1000 vectors).
    pub(super) fn build_sq8_for_index(index: &HnswIndex) -> Option<(Sq8Codec, Vec<u8>)> {
        if index.live_count() < 1000 {
            return None; // Not worth quantizing small indexes.
        }
        let dim = index.dim();
        let n = index.len();

        // Collect vector references for calibration.
        let mut refs: Vec<&[f32]> = Vec::with_capacity(n);
        for i in 0..n {
            if !index.is_deleted(i as u32)
                && let Some(v) = index.get_vector(i as u32)
            {
                refs.push(v);
            }
        }
        if refs.is_empty() {
            return None;
        }

        let codec = Sq8Codec::calibrate(&refs, dim);

        // Quantize all vectors (including deleted — preserves ID alignment).
        let mut data = Vec::with_capacity(dim * n);
        for i in 0..n {
            if let Some(v) = index.get_vector(i as u32) {
                data.extend(codec.quantize(v));
            } else {
                data.extend(vec![0u8; dim]);
            }
        }

        Some((codec, data))
    }

    /// Access sealed segments (read-only) for tombstone ratio inspection.
    pub fn sealed_segments(&self) -> &[SealedSegment] {
        &self.sealed
    }

    /// Compact sealed segments: merge all into one, rebuild HNSW.
    ///
    /// Returns the number of tombstoned vectors removed. Also compacts
    /// each individual sealed segment's HNSW via `HnswIndex::compact()`.
    pub fn compact(&mut self) -> usize {
        let mut total_removed = 0;
        for seg in &mut self.sealed {
            total_removed += seg.index.compact();
        }
        total_removed
    }

    /// Total vector count across all segments (including deleted).
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

    /// Total live (non-deleted) vectors.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_collection() -> VectorCollection {
        VectorCollection::new(
            3,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        )
    }

    #[test]
    fn insert_and_search() {
        let mut coll = make_collection();
        for i in 0..100u32 {
            coll.insert(vec![i as f32, 0.0, 0.0]);
        }
        assert_eq!(coll.len(), 100);
        let results = coll.search(&[50.0, 0.0, 0.0], 3, 64);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 50);
    }

    #[test]
    fn seal_moves_to_building() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        // Insert enough to trigger seal threshold check.
        for i in 0..SEAL_THRESHOLD {
            coll.insert(vec![i as f32, 0.0]);
        }
        assert!(coll.needs_seal());

        let req = coll.seal("test_key").unwrap();
        assert_eq!(req.vectors.len(), SEAL_THRESHOLD);
        assert_eq!(coll.building.len(), 1);
        assert_eq!(coll.growing.len(), 0);

        // Building segment is still searchable.
        let results = coll.search(&[100.0, 0.0], 1, 64);
        assert!(!results.is_empty());
    }

    #[test]
    fn complete_build_promotes_to_sealed() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        for i in 0..100 {
            coll.insert(vec![i as f32, 0.0]);
        }
        let req = coll.seal("test").unwrap();

        // Simulate background build.
        let mut index = HnswIndex::new(req.dim, req.params);
        for v in &req.vectors {
            index.insert(v.clone());
        }
        coll.complete_build(req.segment_id, index);

        assert_eq!(coll.building.len(), 0);
        assert_eq!(coll.sealed.len(), 1);

        // Sealed segment searchable via HNSW.
        let results = coll.search(&[50.0, 0.0], 3, 64);
        assert!(!results.is_empty());
    }

    #[test]
    fn checkpoint_roundtrip() {
        let mut coll = make_collection();
        for i in 0..50u32 {
            coll.insert(vec![i as f32, 0.0, 0.0]);
        }
        let bytes = coll.checkpoint_to_bytes();
        let restored = VectorCollection::from_checkpoint(&bytes).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.dim(), 3);

        let results = restored.search(&[25.0, 0.0, 0.0], 1, 64);
        assert_eq!(results[0].id, 25);
    }

    #[test]
    fn multi_segment_search_merges() {
        let mut coll = VectorCollection::new(
            2,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );

        // Insert, seal, build — creates first sealed segment.
        for i in 0..100 {
            coll.insert(vec![i as f32, 0.0]);
        }
        let req = coll.seal("test").unwrap();
        let mut idx = HnswIndex::new(2, req.params);
        for v in &req.vectors {
            idx.insert(v.clone());
        }
        coll.complete_build(req.segment_id, idx);

        // Insert more into growing segment.
        for i in 100..200 {
            coll.insert(vec![i as f32, 0.0]);
        }

        // Search should find results from both segments.
        let results = coll.search(&[150.0, 0.0], 3, 64);
        assert_eq!(results.len(), 3);
        // Closest should be 150 (in growing segment).
        assert_eq!(results[0].id, 150);
    }

    #[test]
    fn delete_across_segments() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        for i in 0..10 {
            coll.insert(vec![i as f32, 0.0]);
        }
        assert!(coll.delete(5));
        assert_eq!(coll.live_count(), 9);

        let results = coll.search(&[5.0, 0.0], 10, 64);
        assert!(results.iter().all(|r| r.id != 5));
    }
}
