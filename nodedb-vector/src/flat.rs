// SPDX-License-Identifier: Apache-2.0

//! Flat (brute-force) vector index for small collections.
//!
//! Simple linear scan over all stored vectors. No graph overhead, exact
//! results. Automatically used when a collection has fewer than
//! `DEFAULT_FLAT_INDEX_THRESHOLD` vectors (default 10K). Also serves as the
//! search method for growing segments before HNSW construction.
//!
//! Complexity: O(N × D) per query where N = vectors, D = dimensions.

use roaring::RoaringBitmap;

use crate::distance::{DistanceMetric, distance};
use crate::error::{VectorError, check_dim};
use crate::hnsw::SearchResult;
use crate::hnsw::search::decode_filter_bitmap;

/// Default threshold below which collections use flat index instead of HNSW.
pub const DEFAULT_FLAT_INDEX_THRESHOLD: usize = 10_000;

/// Flat vector index: append-only buffer with brute-force search.
pub struct FlatIndex {
    dim: usize,
    metric: DistanceMetric,
    /// Vectors stored contiguously for cache-friendly sequential scan.
    data: Vec<f32>,
    /// Tombstone bitmap: `deleted[i]` = true means vector i is soft-deleted.
    deleted: Vec<bool>,
    /// Number of live (non-deleted) vectors.
    live_count: usize,
}

impl FlatIndex {
    /// Create a new empty flat index.
    pub fn new(dim: usize, metric: DistanceMetric) -> Self {
        Self {
            dim,
            metric,
            data: Vec::new(),
            deleted: Vec::new(),
            live_count: 0,
        }
    }

    /// Insert a vector. Returns the assigned vector ID, or
    /// [`VectorError::DimensionMismatch`] when `vector` does not have the
    /// index dimension.
    pub fn insert(&mut self, vector: Vec<f32>) -> Result<u32, VectorError> {
        check_dim(self.dim, vector.len())?;
        let id = self.len() as u32;
        self.data.extend_from_slice(&vector);
        self.deleted.push(false);
        self.live_count += 1;
        Ok(id)
    }

    /// Soft-delete a vector by ID.
    pub fn delete(&mut self, id: u32) -> bool {
        let idx = id as usize;
        if idx < self.deleted.len() && !self.deleted[idx] {
            self.deleted[idx] = true;
            self.live_count -= 1;
            true
        } else {
            false
        }
    }

    /// Un-delete (clear the soft-delete tombstone of) a vector by ID. Reverses
    /// [`FlatIndex::delete`] — used for transaction rollback so a rolled-back
    /// delete restores the vector to the searchable set. Returns `true` if a
    /// tombstone was actually cleared, `false` if the id was out of range or
    /// already live.
    pub fn undelete(&mut self, id: u32) -> bool {
        let idx = id as usize;
        if idx < self.deleted.len() && self.deleted[idx] {
            self.deleted[idx] = false;
            self.live_count += 1;
            true
        } else {
            false
        }
    }

    /// Brute-force k-NN search with an explicit distance metric override.
    /// Overrides the `self.metric` configured at collection creation time.
    pub fn search_with_metric(
        &self,
        query: &[f32],
        top_k: usize,
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.scan(query, top_k, metric, None)
    }

    /// Brute-force k-NN search. Exact results — no approximation.
    pub fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<SearchResult>, VectorError> {
        self.scan(query, top_k, self.metric, None)
    }

    /// Search with a pre-filter bitmap (byte-array format).
    pub fn search_filtered(
        &self,
        query: &[f32],
        top_k: usize,
        bitmap: &[u8],
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.search_filtered_offset(query, top_k, bitmap, 0)
    }

    /// Filtered search with an explicit metric override.
    pub fn search_filtered_offset_with_metric(
        &self,
        query: &[f32],
        top_k: usize,
        bitmap: &[u8],
        id_offset: u32,
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>, VectorError> {
        let filter = decode_filter_bitmap(bitmap)?;
        self.scan(query, top_k, metric, Some((&filter, id_offset)))
    }

    /// Search with a pre-filter bitmap applying a global id offset.
    ///
    /// `bitmap` is a serialized `RoaringBitmap` (matching the HNSW filter
    /// format). Bit `i + id_offset` tests local id `i`. Used by multi-segment
    /// collections where the bitmap holds GLOBAL vector ids. Bytes that do
    /// not decode fail with [`VectorError::InvalidFilterBitmap`].
    pub fn search_filtered_offset(
        &self,
        query: &[f32],
        top_k: usize,
        bitmap: &[u8],
        id_offset: u32,
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.search_filtered_offset_with_metric(query, top_k, bitmap, id_offset, self.metric)
    }

    /// Exact search of the vectors `filter` admits under `metric`. Local id
    /// `i` is tested as global id `i + id_offset`. A soft-deleted vector is
    /// scored only when `filter` revives it.
    pub fn search_admitting(
        &self,
        query: &[f32],
        top_k: usize,
        metric: DistanceMetric,
        filter: &crate::visibility::NodeFilter<'_>,
        id_offset: u32,
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.scan_admitting(query, top_k, metric, &|i: u32, deleted: bool| {
            filter.admits(i.saturating_add(id_offset), deleted)
        })
    }

    /// Exact scan of every live vector under `metric`, restricted to ids
    /// whose `local + offset` is in the filter when one is given.
    fn scan(
        &self,
        query: &[f32],
        top_k: usize,
        metric: DistanceMetric,
        filter: Option<(&RoaringBitmap, u32)>,
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.scan_admitting(query, top_k, metric, &|i: u32, deleted: bool| {
            !deleted
                && filter
                    .is_none_or(|(bitmap, id_offset)| bitmap.contains(i.saturating_add(id_offset)))
        })
    }

    /// Exact scan of the vectors `admit` accepts under `metric`. `admit`
    /// takes a local id and whether the vector is soft-deleted.
    fn scan_admitting<A: Fn(u32, bool) -> bool>(
        &self,
        query: &[f32],
        top_k: usize,
        metric: DistanceMetric,
        admit: &A,
    ) -> Result<Vec<SearchResult>, VectorError> {
        check_dim(self.dim, query.len())?;
        let n = self.len();
        if n == 0 || top_k == 0 {
            return Ok(Vec::new());
        }

        let mut candidates: Vec<SearchResult> = Vec::with_capacity(n.min(top_k * 2));
        for i in 0..n {
            if !admit(i as u32, self.deleted[i]) {
                continue;
            }
            let start = i * self.dim;
            let vec_slice = &self.data[start..start + self.dim];
            candidates.push(SearchResult {
                id: i as u32,
                distance: distance(query, vec_slice, metric),
            });
        }

        if candidates.len() > top_k {
            candidates.select_nth_unstable_by(top_k, |a, b| {
                a.distance
                    .partial_cmp(&b.distance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            candidates.truncate(top_k);
        }
        candidates.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(candidates)
    }

    pub fn len(&self) -> usize {
        self.deleted.len()
    }

    /// Drop every vector at position `len` or later, as if it was never
    /// inserted. A rollback uses it to withdraw the newest inserts.
    pub fn truncate(&mut self, len: usize) {
        if len >= self.deleted.len() {
            return;
        }
        let dropped_live = self.deleted[len..]
            .iter()
            .filter(|deleted| !**deleted)
            .count();
        self.live_count -= dropped_live;
        self.deleted.truncate(len);
        self.data.truncate(len * self.dim);
    }

    pub fn live_count(&self) -> usize {
        self.live_count
    }

    pub fn is_empty(&self) -> bool {
        self.live_count == 0
    }

    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        let idx = id as usize;
        if idx < self.deleted.len() && !self.deleted[idx] {
            let start = idx * self.dim;
            Some(&self.data[start..start + self.dim])
        } else {
            None
        }
    }

    /// Raw access bypassing tombstone filter — used by snapshot/restore.
    pub fn get_vector_raw(&self, id: u32) -> Option<&[f32]> {
        let idx = id as usize;
        if idx < self.deleted.len() {
            let start = idx * self.dim;
            Some(&self.data[start..start + self.dim])
        } else {
            None
        }
    }

    /// Whether the given local id has been tombstoned.
    pub fn is_deleted(&self, id: u32) -> bool {
        let idx = id as usize;
        idx < self.deleted.len() && self.deleted[idx]
    }

    /// Insert a vector that is already tombstoned (for checkpoint restore).
    pub fn insert_tombstoned(&mut self, vector: Vec<f32>) -> Result<u32, VectorError> {
        check_dim(self.dim, vector.len())?;
        let id = self.len() as u32;
        self.data.extend_from_slice(&vector);
        self.deleted.push(true);
        // No live_count increment — it's dead on arrival.
        Ok(id)
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }

    pub fn tombstone_count(&self) -> usize {
        self.len().saturating_sub(self.live_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_search() {
        let mut idx = FlatIndex::new(3, DistanceMetric::L2);
        for i in 0..100u32 {
            idx.insert(vec![i as f32, 0.0, 0.0]).unwrap();
        }
        assert_eq!(idx.len(), 100);
        assert_eq!(idx.live_count(), 100);

        let results = idx.search(&[50.0, 0.0, 0.0], 3).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 50);
        assert!(results[0].distance < 0.01);
    }

    #[test]
    fn delete_excludes_from_search() {
        let mut idx = FlatIndex::new(2, DistanceMetric::L2);
        idx.insert(vec![0.0, 0.0]).unwrap();
        idx.insert(vec![1.0, 0.0]).unwrap();
        idx.insert(vec![2.0, 0.0]).unwrap();

        assert!(idx.delete(1));
        assert_eq!(idx.live_count(), 2);

        let results = idx.search(&[1.0, 0.0], 3).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.id != 1));
    }

    #[test]
    fn exact_results() {
        let mut idx = FlatIndex::new(2, DistanceMetric::Cosine);
        idx.insert(vec![1.0, 0.0]).unwrap();
        idx.insert(vec![0.0, 1.0]).unwrap();
        idx.insert(vec![1.0, 1.0]).unwrap();

        let results = idx.search(&[1.0, 0.0], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 0);
    }

    #[test]
    fn empty_search() {
        let idx = FlatIndex::new(3, DistanceMetric::L2);
        let results = idx.search(&[1.0, 0.0, 0.0], 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn filtered_search() {
        let mut idx = FlatIndex::new(2, DistanceMetric::L2);
        for i in 0..8u32 {
            idx.insert(vec![i as f32, 0.0]).unwrap();
        }
        let filter: RoaringBitmap = [2u32, 3, 6, 7].into_iter().collect();
        let mut bitmap = Vec::new();
        filter.serialize_into(&mut bitmap).unwrap();
        let results = idx.search_filtered(&[4.0, 0.0], 2, &bitmap).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 3);
        assert!(results.iter().all(|r| filter.contains(r.id)), "{results:?}");
    }

    #[test]
    fn wrong_dimension_is_a_typed_error() {
        let mut idx = FlatIndex::new(2, DistanceMetric::L2);
        assert!(matches!(
            idx.insert(vec![1.0, 2.0, 3.0]),
            Err(VectorError::DimensionMismatch {
                expected: 2,
                got: 3
            })
        ));
        assert!(matches!(
            idx.insert_tombstoned(vec![1.0]),
            Err(VectorError::DimensionMismatch {
                expected: 2,
                got: 1
            })
        ));
        idx.insert(vec![1.0, 0.0]).unwrap();
        let filter: RoaringBitmap = [0u32].into_iter().collect();
        let mut bitmap = Vec::new();
        filter.serialize_into(&mut bitmap).unwrap();
        let short = [1.0_f32];
        for result in [
            idx.search(&short, 1),
            idx.search_with_metric(&short, 1, DistanceMetric::Cosine),
            idx.search_filtered(&short, 1, &bitmap),
            idx.search_filtered_offset(&short, 1, &bitmap, 0),
            idx.search_filtered_offset_with_metric(&short, 1, &bitmap, 0, DistanceMetric::L2),
        ] {
            assert!(
                matches!(
                    result,
                    Err(VectorError::DimensionMismatch {
                        expected: 2,
                        got: 1
                    })
                ),
                "{result:?}"
            );
        }
    }

    #[test]
    fn undecodable_filter_bitmap_is_a_typed_error() {
        let mut idx = FlatIndex::new(2, DistanceMetric::L2);
        idx.insert(vec![1.0, 0.0]).unwrap();
        let result = idx.search_filtered(&[1.0, 0.0], 1, &[0b1100_1100]);
        assert!(
            matches!(result, Err(VectorError::InvalidFilterBitmap { .. })),
            "{result:?}"
        );
    }
}
