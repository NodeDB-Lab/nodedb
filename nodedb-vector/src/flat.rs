//! Flat (brute-force) vector index for small collections.
//!
//! Simple linear scan over all stored vectors. No graph overhead, exact
//! results. Automatically used when a collection has fewer than
//! `DEFAULT_FLAT_INDEX_THRESHOLD` vectors (default 10K). Also serves as the
//! search method for growing segments before HNSW construction.
//!
//! Complexity: O(N × D) per query where N = vectors, D = dimensions.

use crate::distance::{DistanceMetric, distance};
use crate::hnsw::SearchResult;

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

    /// Insert a vector. Returns the assigned vector ID.
    pub fn insert(&mut self, vector: Vec<f32>) -> u32 {
        assert_eq!(
            vector.len(),
            self.dim,
            "dimension mismatch: expected {}, got {}",
            self.dim,
            vector.len()
        );
        let id = self.len() as u32;
        self.data.extend_from_slice(&vector);
        self.deleted.push(false);
        self.live_count += 1;
        id
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

    /// Brute-force k-NN search. Exact results — no approximation.
    pub fn search(&self, query: &[f32], top_k: usize) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.dim);
        let n = self.len();
        if n == 0 || top_k == 0 {
            return Vec::new();
        }

        let mut candidates: Vec<SearchResult> = Vec::with_capacity(n.min(top_k * 2));
        for i in 0..n {
            if self.deleted[i] {
                continue;
            }
            let start = i * self.dim;
            let vec_slice = &self.data[start..start + self.dim];
            let dist = distance(query, vec_slice, self.metric);
            candidates.push(SearchResult {
                id: i as u32,
                distance: dist,
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
        candidates
    }

    /// Search with a pre-filter bitmap (byte-array format).
    pub fn search_filtered(&self, query: &[f32], top_k: usize, bitmap: &[u8]) -> Vec<SearchResult> {
        self.search_filtered_offset(query, top_k, bitmap, 0)
    }

    /// Search with a pre-filter bitmap applying a global id offset.
    ///
    /// The bitmap is interpreted in a shifted id space: bit `i + id_offset`
    /// tests local id `i`. Used by multi-segment collections where the
    /// bitmap holds GLOBAL vector ids.
    pub fn search_filtered_offset(
        &self,
        query: &[f32],
        top_k: usize,
        bitmap: &[u8],
        id_offset: u32,
    ) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.dim);
        let n = self.len();
        if n == 0 || top_k == 0 {
            return Vec::new();
        }

        let mut candidates: Vec<SearchResult> = Vec::with_capacity(top_k * 2);
        for i in 0..n {
            if self.deleted[i] {
                continue;
            }
            let global = i + id_offset as usize;
            let byte_idx = global / 8;
            let bit_idx = global % 8;
            if byte_idx >= bitmap.len() || (bitmap[byte_idx] & (1 << bit_idx)) == 0 {
                continue;
            }
            let start = i * self.dim;
            let vec_slice = &self.data[start..start + self.dim];
            let dist = distance(query, vec_slice, self.metric);
            candidates.push(SearchResult {
                id: i as u32,
                distance: dist,
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
        candidates
    }

    pub fn len(&self) -> usize {
        self.deleted.len()
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
    pub fn insert_tombstoned(&mut self, vector: Vec<f32>) -> u32 {
        assert_eq!(
            vector.len(),
            self.dim,
            "dimension mismatch: expected {}, got {}",
            self.dim,
            vector.len()
        );
        let id = self.len() as u32;
        self.data.extend_from_slice(&vector);
        self.deleted.push(true);
        // No live_count increment — it's dead on arrival.
        id
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
            idx.insert(vec![i as f32, 0.0, 0.0]);
        }
        assert_eq!(idx.len(), 100);
        assert_eq!(idx.live_count(), 100);

        let results = idx.search(&[50.0, 0.0, 0.0], 3);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, 50);
        assert!(results[0].distance < 0.01);
    }

    #[test]
    fn delete_excludes_from_search() {
        let mut idx = FlatIndex::new(2, DistanceMetric::L2);
        idx.insert(vec![0.0, 0.0]);
        idx.insert(vec![1.0, 0.0]);
        idx.insert(vec![2.0, 0.0]);

        assert!(idx.delete(1));
        assert_eq!(idx.live_count(), 2);

        let results = idx.search(&[1.0, 0.0], 3);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.id != 1));
    }

    #[test]
    fn exact_results() {
        let mut idx = FlatIndex::new(2, DistanceMetric::Cosine);
        idx.insert(vec![1.0, 0.0]);
        idx.insert(vec![0.0, 1.0]);
        idx.insert(vec![1.0, 1.0]);

        let results = idx.search(&[1.0, 0.0], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 0);
    }

    #[test]
    fn empty_search() {
        let idx = FlatIndex::new(3, DistanceMetric::L2);
        let results = idx.search(&[1.0, 0.0, 0.0], 5);
        assert!(results.is_empty());
    }

    #[test]
    fn filtered_search() {
        let mut idx = FlatIndex::new(2, DistanceMetric::L2);
        for i in 0..8u32 {
            idx.insert(vec![i as f32, 0.0]);
        }
        let bitmap = vec![0b11001100u8];
        let results = idx.search_filtered(&[3.0, 0.0], 2, &bitmap);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, 3);
        assert_eq!(results[1].id, 2);
    }
}
