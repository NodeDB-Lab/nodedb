// SPDX-License-Identifier: Apache-2.0

//! HNSW search algorithm (Malkov & Yashunin, Algorithm 2).
//!
//! Beam search through the multi-layer graph with optional Roaring bitmap
//! pre-filtering for efficient filtered k-NN queries.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Issue a non-faulting prefetch hint for the memory region starting at `ptr`
/// into all cache levels (T0). No-op on architectures without a prefetch
/// intrinsic.
#[inline(always)]
fn prefetch_t0(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: _mm_prefetch is a pure hint — it never faults even on an
        // invalid address, and it does not dereference the pointer.
        unsafe {
            core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(
                ptr as *const i8,
            );
        }
    }
    // aarch64 hardware prefetchers handle sequential + pointer-chasing patterns
    // well without explicit hints. wasm32 and all other targets: intentional no-op.
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

use roaring::RoaringBitmap;

use crate::error::{VectorError, check_dim};
use crate::hnsw::graph::{Candidate, HnswIndex, Probe, SearchResult};

/// Maximum beam width to prevent runaway search cost.
const MAX_EF: usize = 8192;

impl HnswIndex {
    /// K-NN search: find the `k` closest vectors to `query`.
    ///
    /// `ef` controls the search beam width (higher = better recall, slower).
    /// Must be >= k. Typical values: ef = 2*k to 10*k.
    ///
    /// Returns [`VectorError::DimensionMismatch`] when `query` does not have
    /// the index dimension.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.search_inner(query, k, ef.max(k).min(MAX_EF), None, 0)
    }

    /// Filtered K-NN search with Roaring bitmap pre-filtering.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: &RoaringBitmap,
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.search_filtered_offset(query, k, ef, filter, 0)
    }

    /// Filtered K-NN search where the bitmap is keyed in a shifted ID space.
    ///
    /// `id_offset` is added to local node IDs before testing `filter.contains`.
    /// Used by multi-segment collections where the bitmap holds GLOBAL ids
    /// and each segment's HNSW nodes are numbered starting at `base_id`.
    pub fn search_filtered_offset(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: &RoaringBitmap,
        id_offset: u32,
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.search_inner(query, k, ef.max(k), Some(filter), id_offset)
    }

    /// Deserialize a Roaring bitmap from bytes and perform filtered search.
    pub fn search_with_bitmap_bytes(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        bitmap_bytes: &[u8],
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.search_with_bitmap_bytes_offset(query, k, ef, bitmap_bytes, 0)
    }

    /// Deserialize a Roaring bitmap and search with an ID offset applied
    /// before testing membership. See `search_filtered_offset` for rationale.
    ///
    /// Bytes that do not decode fail with
    /// [`VectorError::InvalidFilterBitmap`]: an unfiltered search would
    /// return rows the filter excludes.
    pub fn search_with_bitmap_bytes_offset(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        bitmap_bytes: &[u8],
        id_offset: u32,
    ) -> Result<Vec<SearchResult>, VectorError> {
        let bitmap = decode_filter_bitmap(bitmap_bytes)?;
        self.search_filtered_offset(query, k, ef, &bitmap, id_offset)
    }

    /// K-NN search admitting the nodes `filter` admits. `id_offset` is added
    /// to local node ids before `filter` tests them. A soft-deleted node
    /// enters the results only when `filter` revives it. A search at a
    /// past system-time cut reads ended nodes this way.
    pub fn search_admitting(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: &crate::visibility::NodeFilter<'_>,
        id_offset: u32,
    ) -> Result<Vec<SearchResult>, VectorError> {
        check_dim(self.dim, query.len())?;
        // An index whose every node is deleted answers only when the filter
        // revives a deleted node.
        if self.nodes.is_empty() || (self.is_empty() && !filter.admits_deleted()) {
            return Ok(Vec::new());
        }
        let admit = |id: u32, deleted: bool| filter.admits(id.saturating_add(id_offset), deleted);
        self.search_with_admit(query, k, ef.max(k).min(MAX_EF), &admit)
    }

    /// Greedy descent to layer 1, then a beam search of width `ef` at layer
    /// 0, restricted to `filter` when one is given.
    fn search_inner(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: Option<&RoaringBitmap>,
        id_offset: u32,
    ) -> Result<Vec<SearchResult>, VectorError> {
        check_dim(self.dim, query.len())?;
        if self.is_empty() {
            return Ok(Vec::new());
        }
        let admit = |id: u32, deleted: bool| {
            !deleted
                && match filter {
                    Some(f) => f.contains(id + id_offset),
                    None => true,
                }
        };
        self.search_with_admit(query, k, ef, &admit)
    }

    /// Greedy descent to layer 1, then a beam search of width `ef` at layer
    /// 0 whose results are the nodes `admit` accepts.
    fn search_with_admit<A: Fn(u32, bool) -> bool>(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        admit: &A,
    ) -> Result<Vec<SearchResult>, VectorError> {
        let Some(ep) = self.entry_point else {
            return Ok(Vec::new());
        };

        let probe = self.probe(query);

        // Phase 1: Greedy descent from top layer to layer 1.
        let mut current_ep = ep;
        for layer in (1..=self.max_layer).rev() {
            let results = search_layer(self, &probe, current_ep, 1, layer, None, 0);
            if let Some(nearest) = results.first() {
                current_ep = nearest.id;
            }
        }

        // Phase 2: Beam search at layer 0.
        let results = search_layer_with(self, &probe, current_ep, ef, 0, admit);

        Ok(results
            .into_iter()
            .take(k)
            .map(|c| SearchResult {
                id: c.id,
                distance: c.dist,
            })
            .collect())
    }
}

/// Decode a serialized Roaring pre-filter bitmap.
pub fn decode_filter_bitmap(bytes: &[u8]) -> Result<RoaringBitmap, VectorError> {
    RoaringBitmap::deserialize_from(bytes).map_err(|e| VectorError::InvalidFilterBitmap {
        detail: e.to_string(),
    })
}

/// Unified HNSW beam search on a single layer with optional pre-filter.
///
/// When `filter` is `None`, all non-deleted nodes enter the result set.
/// When `filter` is `Some`, only nodes present in the bitmap enter results,
/// but all nodes are still traversed for graph connectivity.
///
/// Distances use the build metric. A node without a vector is traversed at
/// infinite distance and never enters the results.
///
/// Scratch buffers are drawn from `index.arena` (a `RefCell`-guarded
/// `BeamSearchArena`).  The arena is reset at entry and its `Vec`/`HashSet`
/// capacity grows to the high-water mark over successive calls, giving
/// amortised zero-allocation steady state.
pub(crate) fn search_layer(
    index: &HnswIndex,
    probe: &Probe<'_>,
    entry_point: u32,
    ef: usize,
    layer: usize,
    filter: Option<&RoaringBitmap>,
    id_offset: u32,
) -> Vec<Candidate> {
    let admit = |id: u32, deleted: bool| {
        !deleted
            && match filter {
                Some(f) => f.contains(id + id_offset),
                None => true,
            }
    };
    search_layer_with(index, probe, entry_point, ef, layer, &admit)
}

/// [`search_layer`] whose results are the nodes `admit` accepts. `admit`
/// takes a local node id and whether the node is soft-deleted. Every node
/// is still traversed for graph connectivity.
pub(crate) fn search_layer_with<A: Fn(u32, bool) -> bool>(
    index: &HnswIndex,
    probe: &Probe<'_>,
    entry_point: u32,
    ef: usize,
    layer: usize,
    admit: &A,
) -> Vec<Candidate> {
    let metric = index.params.metric;
    let mut arena = index.arena.borrow_mut();

    // Reset scratch buffers — retains Vec/HashSet capacity from prior calls.
    arena.reset();

    arena.visited.insert(entry_point);

    let ep_dist = index.dist_to_node(probe, entry_point, metric);
    let ep_candidate = Candidate {
        dist: ep_dist.unwrap_or(f32::INFINITY),
        id: entry_point,
    };

    // Strategy (c): take the arena Vecs, build BinaryHeaps on them, do the
    // search, then move the Vecs back.  The Vecs retain their allocated
    // capacity across the take/into_vec round-trip, so steady-state searches
    // incur zero heap allocation.
    let mut cand_vec = std::mem::take(&mut arena.candidates);
    cand_vec.push(Reverse(ep_candidate));
    let mut candidates = BinaryHeap::from(cand_vec);

    let mut res_vec = std::mem::take(&mut arena.results);

    let passes_filter = |id: u32| -> bool { admit(id, index.nodes[id as usize].deleted) };

    if ep_dist.is_some() && passes_filter(entry_point) {
        res_vec.push(ep_candidate);
    }
    let mut results = BinaryHeap::from(res_vec);

    while let Some(Reverse(current)) = candidates.pop() {
        if let Some(worst) = results.peek()
            && current.dist > worst.dist
            && results.len() >= ef
        {
            break;
        }

        // Prefetch the vector bytes of the next candidate before touching this
        // iteration's neighbor list, so it lands in cache by the time the
        // inner loop calls dist_to_node on it.
        if let Some(Reverse(next)) = candidates.peek()
            && let Some(node) = index.nodes.get(next.id as usize)
        {
            prefetch_t0(node.storage.as_bytes().as_ptr());
        }

        let neighbors = index.neighbors_at(current.id, layer);
        if neighbors.is_empty() && layer >= index.node_num_layers(current.id) {
            continue;
        }

        for &neighbor_id in neighbors {
            if !arena.visited.insert(neighbor_id) {
                continue;
            }

            let scored = index.dist_to_node(probe, neighbor_id, metric);
            let neighbor = Candidate {
                dist: scored.unwrap_or(f32::INFINITY),
                id: neighbor_id,
            };

            let worst_dist = results.peek().map_or(f32::INFINITY, |w| w.dist);
            let should_explore = neighbor.dist < worst_dist || results.len() < ef;

            if should_explore {
                candidates.push(Reverse(neighbor));
            }

            if scored.is_some() && passes_filter(neighbor_id) {
                results.push(neighbor);
                if results.len() > ef {
                    results.pop();
                }
            }
        }
    }

    // Restore the candidates Vec to the arena before releasing the borrow.
    arena.candidates = candidates.into_vec();

    let mut result_vec = results.into_vec();
    // arena.results stays empty — reset() will clear it on the next call.
    drop(arena);

    result_vec.sort_unstable_by(|a, b| a.dist.total_cmp(&b.dist));
    result_vec
}

#[cfg(test)]
mod tests {
    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};
    use roaring::RoaringBitmap;

    fn build_index(n: usize, dim: usize) -> HnswIndex {
        let mut idx = HnswIndex::with_seed(
            dim,
            HnswParams {
                m: 16,
                m0: 32,
                ef_construction: 100,
                metric: DistanceMetric::L2,
                dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
            },
            42,
        );
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|d| (i * dim + d) as f32).collect();
            idx.insert(v).unwrap();
        }
        idx
    }

    #[test]
    fn search_empty_index() {
        let idx = HnswIndex::new(3, HnswParams::default());
        let results = idx.search(&[1.0, 2.0, 3.0], 5, 50).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn search_single_element() {
        let mut idx = HnswIndex::with_seed(
            2,
            HnswParams {
                m: 4,
                m0: 8,
                ef_construction: 16,
                metric: DistanceMetric::L2,
                dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
            },
            1,
        );
        idx.insert(vec![1.0, 0.0]).unwrap();
        let results = idx.search(&[1.0, 0.0], 1, 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 0);
        assert!(results[0].distance < 1e-6);
    }

    #[test]
    fn search_finds_exact_match() {
        let idx = build_index(50, 3);
        let query = idx.get_vector(25).unwrap().to_vec();
        let results = idx.search(&query, 1, 50).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 25);
        assert!(results[0].distance < 1e-6);
    }

    #[test]
    fn search_returns_sorted_by_distance() {
        let idx = build_index(100, 4);
        let query = vec![50.0, 50.0, 50.0, 50.0];
        let results = idx.search(&query, 10, 64).unwrap();
        assert_eq!(results.len(), 10);
        for w in results.windows(2) {
            assert!(w[0].distance <= w[1].distance);
        }
    }

    #[test]
    fn search_k_larger_than_index() {
        let idx = build_index(5, 2);
        let results = idx.search(&[0.0, 0.0], 20, 50).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn search_recall_at_10() {
        let idx = build_index(500, 3);
        let query = vec![100.0, 100.0, 100.0];
        let results = idx.search(&query, 10, 128).unwrap();

        let mut truth: Vec<(u32, f32)> = (0..500)
            .map(|i| {
                let v = idx.get_vector(i).unwrap();
                let d = crate::distance::l2_squared(&query, v);
                (i, d)
            })
            .collect();
        truth.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let truth_top10: std::collections::HashSet<u32> = truth[..10].iter().map(|t| t.0).collect();

        let found: std::collections::HashSet<u32> = results.iter().map(|r| r.id).collect();
        let recall = found.intersection(&truth_top10).count() as f64 / 10.0;
        assert!(recall >= 0.8, "recall@10 = {recall:.2}, expected >= 0.80");
    }

    #[test]
    fn search_excludes_tombstoned() {
        let mut idx = build_index(20, 3);
        idx.delete(0);
        let results = idx.search(&[0.0, 0.0, 0.0], 5, 32).unwrap();
        for r in &results {
            assert_ne!(r.id, 0, "tombstoned node appeared in results");
        }
    }

    #[test]
    fn search_filtered_respects_bitmap() {
        let idx = build_index(50, 3);
        let mut filter = RoaringBitmap::new();
        for i in (0..50u32).step_by(2) {
            filter.insert(i);
        }
        let results = idx
            .search_filtered(&[0.0, 0.0, 0.0], 5, 64, &filter)
            .unwrap();
        assert_eq!(results.len(), 5);
        for r in &results {
            assert!(r.id % 2 == 0, "got odd id {}", r.id);
        }
    }

    #[test]
    fn search_filtered_empty_returns_empty() {
        let idx = build_index(20, 3);
        let filter = RoaringBitmap::new();
        let results = idx
            .search_filtered(&[0.0, 0.0, 0.0], 5, 64, &filter)
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn bitmap_bytes_roundtrip() {
        let idx = build_index(50, 3);
        let mut filter = RoaringBitmap::new();
        for i in 0..25u32 {
            filter.insert(i);
        }
        let mut bytes = Vec::new();
        filter.serialize_into(&mut bytes).unwrap();
        let results = idx
            .search_with_bitmap_bytes(&[0.0, 0.0, 0.0], 5, 32, &bytes)
            .unwrap();
        for r in &results {
            assert!(r.id < 25, "got filtered-out node {}", r.id);
        }
    }

    /// A query of the wrong dimension is a typed error on every entry point,
    /// on an empty index and a populated one alike.
    #[test]
    fn wrong_dimension_query_is_a_typed_error() {
        use crate::error::VectorError;
        let empty = HnswIndex::new(3, HnswParams::default());
        let idx = build_index(20, 3);
        let filter: RoaringBitmap = (0..20u32).collect();
        let mut bytes = Vec::new();
        filter.serialize_into(&mut bytes).unwrap();
        let short = [0.0_f32, 0.0];
        for result in [
            empty.search(&short, 5, 32),
            idx.search(&short, 5, 32),
            idx.search_filtered(&short, 5, 32, &filter),
            idx.search_filtered_offset(&short, 5, 32, &filter, 0),
            idx.search_with_bitmap_bytes(&short, 5, 32, &bytes),
            idx.search_with_bitmap_bytes_offset(&short, 5, 32, &bytes, 0),
        ] {
            assert!(
                matches!(
                    result,
                    Err(VectorError::DimensionMismatch {
                        expected: 3,
                        got: 2
                    })
                ),
                "{result:?}"
            );
        }
    }

    /// Filter bytes that do not decode fail the search: an unfiltered
    /// search would return rows the filter excludes.
    #[test]
    fn undecodable_filter_bitmap_is_a_typed_error() {
        use crate::error::VectorError;
        let idx = build_index(20, 3);
        let result = idx.search_with_bitmap_bytes(&[0.0, 0.0, 0.0], 5, 32, &[0xff, 0x01]);
        assert!(
            matches!(result, Err(VectorError::InvalidFilterBitmap { .. })),
            "{result:?}"
        );
    }
}
