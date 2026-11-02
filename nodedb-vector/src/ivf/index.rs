// SPDX-License-Identifier: Apache-2.0

//! IVF-PQ index: inverted file with product quantization.
//!
//! k-means centroids partition the vectors into Voronoi cells. Each entry
//! holds a PQ code of its residual against the cell centroid, plus the FP32
//! vector. A search probes the nearest cells, ranks their entries by PQ
//! distance, and reranks the best of them by exact distance.
//!
//! Entries carry caller-assigned ids, so a collection can move its vectors
//! into the index under the ids they already have.

use std::collections::HashMap;

use nodedb_mem::ScopedMemory;
use roaring::RoaringBitmap;

use crate::distance::distance;
use crate::error::{VectorError, check_dim};
use crate::quantize::pq::PqCodec;

use super::kmeans::kmeans_centroids;
use super::params::IvfPqParams;

/// k-means iterations for the coarse centroids and the PQ codebooks.
pub(super) const TRAIN_ITERATIONS: usize = 20;

/// The entries of one Voronoi cell, stored column-wise. Entry `i` has id
/// `ids[i]`, PQ code `codes[i * m .. (i + 1) * m]`, and FP32 vector
/// `vectors[i * dim .. (i + 1) * dim]`.
#[derive(Debug, Clone, Default, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct IvfCell {
    pub(super) ids: Vec<u32>,
    pub(super) codes: Vec<u8>,
    pub(super) vectors: Vec<f32>,
}

/// IVF-PQ index: inverted file with product quantization.
pub struct IvfPqIndex {
    pub(super) dim: usize,
    pub(super) params: IvfPqParams,
    /// Coarse centroids: `n_cells` × `dim` FP32 vectors.
    pub(super) centroids: Vec<Vec<f32>>,
    /// PQ codec trained on the residuals. `None` until [`Self::train`].
    pub(super) pq: Option<PqCodec>,
    pub(super) cells: Vec<IvfCell>,
    /// Entry id → (cell, position in the cell).
    pub(super) slots: HashMap<u32, (u32, u32)>,
    /// Soft-deleted entry ids.
    pub(super) deleted: RoaringBitmap,
    /// Id [`Self::add`] assigns next: one past the highest id ever held.
    pub(super) next_id: u32,
    /// Vectors the codebooks were trained on.
    pub(super) trained_on: usize,
    /// Unix milliseconds of the training, as the caller stamped it. `0` when
    /// never stamped.
    pub(super) trained_at_ms: u64,
}

impl IvfPqIndex {
    /// Create an empty, untrained IVF-PQ index.
    pub fn new(dim: usize, params: IvfPqParams) -> Self {
        Self {
            dim,
            params,
            centroids: Vec::new(),
            pq: None,
            cells: Vec::new(),
            slots: HashMap::new(),
            deleted: RoaringBitmap::new(),
            next_id: 0,
            trained_on: 0,
            trained_at_ms: 0,
        }
    }

    /// Train the coarse centroids and the PQ codebooks on `vectors`, tracking
    /// PQ codebook allocations against `memory`.
    ///
    /// Fails with [`VectorError::InvalidInput`] when the set is empty, the
    /// dimension is zero or not divisible by `pq_m`, the set is smaller than
    /// `pq_k`, or the index already holds vectors (their codes would not
    /// match new codebooks). A vector without the index dimension fails with
    /// [`VectorError::DimensionMismatch`]. A failed training changes nothing.
    pub fn train(&mut self, vectors: &[&[f32]], memory: ScopedMemory) -> Result<(), VectorError> {
        if vectors.is_empty() {
            return Err(VectorError::InvalidInput {
                detail: "IVF-PQ training needs at least one vector".into(),
            });
        }
        if !self.slots.is_empty() {
            return Err(VectorError::InvalidInput {
                detail: format!(
                    "IVF-PQ index already holds {} vectors; train an empty index",
                    self.slots.len()
                ),
            });
        }
        if self.dim == 0 || self.params.pq_m == 0 || !self.dim.is_multiple_of(self.params.pq_m) {
            return Err(VectorError::InvalidInput {
                detail: format!(
                    "IVF-PQ dimension {} must be non-zero and divisible by pq_m {}",
                    self.dim, self.params.pq_m
                ),
            });
        }
        for v in vectors {
            check_dim(self.dim, v.len())?;
        }

        let n_cells = self.params.n_cells.min(vectors.len());
        let centroids = kmeans_centroids(vectors, self.dim, n_cells, TRAIN_ITERATIONS);
        let residuals: Vec<Vec<f32>> = vectors
            .iter()
            .map(|v| residual(v, &centroids[nearest(&centroids, v, &self.params)]))
            .collect();
        let res_refs: Vec<&[f32]> = residuals.iter().map(|r| r.as_slice()).collect();
        let pq = PqCodec::train(
            &res_refs,
            self.dim,
            self.params.pq_m,
            self.params.pq_k,
            TRAIN_ITERATIONS,
            memory,
        )?;

        self.cells = vec![IvfCell::default(); centroids.len()];
        self.centroids = centroids;
        self.pq = Some(pq);
        self.trained_on = vectors.len();
        Ok(())
    }

    /// Add `vector` under the caller-assigned `id`.
    ///
    /// Fails with [`VectorError::DimensionMismatch`] for a vector without the
    /// index dimension, and with [`VectorError::InvalidInput`] when the index
    /// is untrained or already holds `id`. A failed add changes nothing.
    pub fn insert_with_id(&mut self, id: u32, vector: Vec<f32>) -> Result<(), VectorError> {
        check_dim(self.dim, vector.len())?;
        let Some(pq) = self.pq.as_ref() else {
            return Err(VectorError::InvalidInput {
                detail: "IVF-PQ index must be trained before add".into(),
            });
        };
        if self.slots.contains_key(&id) {
            return Err(VectorError::InvalidInput {
                detail: format!("IVF-PQ index already holds vector id {id}"),
            });
        }
        let cell_idx = nearest(&self.centroids, &vector, &self.params);
        let code = pq.encode(&residual(&vector, &self.centroids[cell_idx]));
        let cell = &mut self.cells[cell_idx];
        let pos = cell.ids.len() as u32;
        cell.ids.push(id);
        cell.codes.extend_from_slice(&code);
        cell.vectors.extend_from_slice(&vector);
        self.slots.insert(id, (cell_idx as u32, pos));
        self.next_id = self.next_id.max(id.saturating_add(1));
        Ok(())
    }

    /// Add a vector under the next free id. Returns the id.
    ///
    /// Fails as [`Self::insert_with_id`] does.
    pub fn add(&mut self, vector: &[f32]) -> Result<u32, VectorError> {
        let id = self.next_id;
        self.insert_with_id(id, vector.to_vec())?;
        Ok(id)
    }

    /// Add vectors under consecutive free ids. Stops at the first vector
    /// that fails to add.
    pub fn add_batch(&mut self, vectors: &[&[f32]]) -> Result<(), VectorError> {
        for v in vectors {
            self.add(v)?;
        }
        Ok(())
    }

    /// Whether the index holds a trained codebook.
    pub fn is_trained(&self) -> bool {
        self.pq.is_some()
    }

    /// Whether the index holds `id`, live or soft-deleted.
    pub fn contains(&self, id: u32) -> bool {
        self.slots.contains_key(&id)
    }

    /// Whether `id` is held and soft-deleted.
    pub fn is_deleted(&self, id: u32) -> bool {
        self.deleted.contains(id)
    }

    /// Soft-delete `id`. `false` when the index does not hold it live.
    pub fn delete(&mut self, id: u32) -> bool {
        self.contains(id) && self.deleted.insert(id)
    }

    /// Reverse a soft delete of `id`. `false` when `id` was not deleted.
    pub fn undelete(&mut self, id: u32) -> bool {
        self.deleted.remove(id)
    }

    /// The FP32 vector of a live `id`.
    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        if self.deleted.contains(id) {
            return None;
        }
        self.get_vector_raw(id)
    }

    /// The FP32 vector of `id`, live or soft-deleted.
    pub fn get_vector_raw(&self, id: u32) -> Option<&[f32]> {
        let &(cell, pos) = self.slots.get(&id)?;
        let start = pos as usize * self.dim;
        self.cells
            .get(cell as usize)?
            .vectors
            .get(start..start + self.dim)
    }

    /// Every live entry as `(id, vector)`, ordered by id.
    pub fn live_vectors(&self) -> Vec<(u32, Vec<f32>)> {
        let mut out: Vec<(u32, Vec<f32>)> = Vec::with_capacity(self.live_count());
        for cell in &self.cells {
            for (pos, &id) in cell.ids.iter().enumerate() {
                if !self.deleted.contains(id) {
                    let start = pos * self.dim;
                    out.push((id, cell.vectors[start..start + self.dim].to_vec()));
                }
            }
        }
        out.sort_unstable_by_key(|(id, _)| *id);
        out
    }

    /// Drop every entry with id `next_id` or later, so the next
    /// [`Self::add`] takes `next_id` again. The training stays. A rollback
    /// uses it to withdraw the newest adds.
    pub fn roll_back_to(&mut self, next_id: u32) {
        self.retain_entries(|id| id < next_id);
        self.deleted.remove_range(next_id..);
        self.next_id = self.next_id.min(next_id);
    }

    /// Remove every soft-deleted entry. Returns the number removed. Ids of
    /// the remaining entries do not change.
    pub fn compact(&mut self) -> usize {
        self.compact_retaining(&RoaringBitmap::new())
    }

    /// [`Self::compact`] that keeps every soft-deleted entry in `retain`. A
    /// kept entry stays soft-deleted: a search at a past system-time cut
    /// still reads it.
    pub fn compact_retaining(&mut self, retain: &RoaringBitmap) -> usize {
        let deleted = std::mem::take(&mut self.deleted);
        let removed = self.retain_entries(|id| !deleted.contains(id) || retain.contains(id));
        self.deleted = &deleted & retain;
        removed
    }

    /// Keep the entries whose id satisfies `keep` and rebuild the slot map.
    /// Returns the number of entries dropped.
    fn retain_entries(&mut self, keep: impl Fn(u32) -> bool) -> usize {
        let m = self.pq.as_ref().map_or(0, |pq| pq.m);
        let dim = self.dim;
        let mut removed = 0;
        self.slots.clear();
        for (cell_idx, cell) in self.cells.iter_mut().enumerate() {
            let mut kept = IvfCell::default();
            for (pos, &id) in cell.ids.iter().enumerate() {
                if !keep(id) {
                    removed += 1;
                    continue;
                }
                self.slots
                    .insert(id, (cell_idx as u32, kept.ids.len() as u32));
                kept.ids.push(id);
                kept.codes
                    .extend_from_slice(&cell.codes[pos * m..(pos + 1) * m]);
                kept.vectors
                    .extend_from_slice(&cell.vectors[pos * dim..(pos + 1) * dim]);
            }
            *cell = kept;
        }
        removed
    }

    /// Entries held, live or soft-deleted.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Entries held and not soft-deleted.
    pub fn live_count(&self) -> usize {
        self.slots.len() - self.deleted.len() as usize
    }

    /// Soft-deleted entries held.
    pub fn tombstone_count(&self) -> usize {
        self.deleted.len() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn n_cells(&self) -> usize {
        self.centroids.len()
    }

    pub fn params(&self) -> &IvfPqParams {
        &self.params
    }

    /// Vectors the codebooks were trained on. `0` when untrained.
    pub fn trained_on(&self) -> usize {
        self.trained_on
    }

    /// Unix milliseconds the caller stamped the training with.
    pub fn trained_at_ms(&self) -> u64 {
        self.trained_at_ms
    }

    /// Stamp the training time, in Unix milliseconds.
    pub fn set_trained_at_ms(&mut self, ms: u64) {
        self.trained_at_ms = ms;
    }

    /// Approximate heap bytes of the centroids, codes and vectors.
    pub fn memory_bytes(&self) -> usize {
        let f32_size = std::mem::size_of::<f32>();
        let centroids = self.centroids.len() * self.dim * f32_size;
        let entries: usize = self
            .cells
            .iter()
            .map(|c| {
                c.ids.len() * std::mem::size_of::<u32>()
                    + c.codes.len()
                    + c.vectors.len() * f32_size
            })
            .sum();
        centroids + entries
    }
}

/// Index of the centroid in `centroids` nearest to `vector`. `0` when there
/// are none.
fn nearest(centroids: &[Vec<f32>], vector: &[f32], params: &IvfPqParams) -> usize {
    let mut best = 0;
    let mut best_dist = f32::MAX;
    for (i, c) in centroids.iter().enumerate() {
        let d = distance(vector, c, params.metric);
        if d < best_dist {
            best_dist = d;
            best = i;
        }
    }
    best
}

/// `vector - centroid`, component-wise.
pub(super) fn residual(vector: &[f32], centroid: &[f32]) -> Vec<f32> {
    vector.iter().zip(centroid).map(|(a, b)| a - b).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::DistanceMetric;
    use crate::test_support::test_memory;

    fn make_vectors(n: usize, dim: usize) -> Vec<Vec<f32>> {
        (0..n)
            .map(|i| (0..dim).map(|d| ((i * dim + d) as f32) * 0.01).collect())
            .collect()
    }

    fn small_params() -> IvfPqParams {
        IvfPqParams {
            n_cells: 4,
            pq_m: 4,
            pq_k: 8,
            nprobe: 4,
            metric: DistanceMetric::L2,
        }
    }

    fn trained(vecs: &[Vec<f32>]) -> IvfPqIndex {
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut idx = IvfPqIndex::new(8, small_params());
        idx.train(&refs, test_memory()).unwrap();
        idx
    }

    #[test]
    fn rolling_back_withdraws_every_vector_added_after_the_mark() {
        let vecs = make_vectors(64, 8);
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut idx = trained(&vecs);
        idx.add_batch(&refs[..40]).unwrap();
        let mark = idx.len() as u32;
        let before: Vec<u32> = idx
            .search(&vecs[5], 40)
            .unwrap()
            .iter()
            .map(|r| r.id)
            .collect();

        idx.add_batch(&refs[40..]).unwrap();
        idx.roll_back_to(mark);

        assert_eq!(idx.len(), 40);
        assert!(idx.is_trained(), "the training the index held stays");
        let after_ids: Vec<u32> = idx
            .search(&vecs[5], 64)
            .unwrap()
            .iter()
            .map(|r| r.id)
            .collect();
        assert!(after_ids.iter().all(|id| *id < mark));
        assert_eq!(after_ids, before, "the search reads as before the adds");

        // The next add takes the first id past the mark again.
        assert_eq!(idx.add(&vecs[63]).unwrap(), mark);
    }

    #[test]
    fn caller_ids_survive_delete_compact_and_reads() {
        let vecs = make_vectors(16, 8);
        let mut idx = trained(&vecs);
        for (i, v) in vecs.iter().enumerate() {
            idx.insert_with_id(100 + i as u32, v.clone()).unwrap();
        }
        assert!(matches!(
            idx.insert_with_id(100, vecs[0].clone()),
            Err(VectorError::InvalidInput { .. })
        ));
        assert!(idx.delete(103));
        assert!(!idx.delete(103), "a second delete finds nothing live");
        assert!(idx.get_vector(103).is_none());
        assert_eq!(idx.live_count(), 15);

        assert_eq!(idx.compact(), 1);
        assert_eq!(idx.len(), 15);
        assert!(!idx.contains(103));
        assert_eq!(idx.get_vector(104), Some(vecs[4].as_slice()));
        assert_eq!(idx.add(&vecs[0]).unwrap(), 116);
    }

    #[test]
    fn a_trained_index_holding_vectors_refuses_to_retrain() {
        let vecs = make_vectors(16, 8);
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut idx = trained(&vecs);
        idx.add(&vecs[0]).unwrap();
        assert!(matches!(
            idx.train(&refs, test_memory()),
            Err(VectorError::InvalidInput { .. })
        ));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn wrong_dimension_is_a_typed_error() {
        let vecs: Vec<Vec<f32>> = (0..32)
            .map(|i| (0..8).map(|d| ((i * 8 + d) % 17) as f32).collect())
            .collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut idx = IvfPqIndex::new(8, small_params());
        assert!(matches!(
            idx.add(&[0.0; 8]),
            Err(VectorError::InvalidInput { .. })
        ));
        idx.train(&refs, test_memory()).unwrap();
        idx.add_batch(&refs).unwrap();
        assert!(matches!(
            idx.search(&[0.0; 3], 5),
            Err(VectorError::DimensionMismatch {
                expected: 8,
                got: 3
            })
        ));
        assert!(matches!(
            idx.add(&[0.0; 3]),
            Err(VectorError::DimensionMismatch {
                expected: 8,
                got: 3
            })
        ));
        let short = [0.0_f32; 3];
        let mut untrained = IvfPqIndex::new(8, IvfPqParams::default());
        assert!(matches!(
            untrained.train(&[&short], test_memory()),
            Err(VectorError::DimensionMismatch {
                expected: 8,
                got: 3
            })
        ));
    }
}
