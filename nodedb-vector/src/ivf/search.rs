// SPDX-License-Identifier: Apache-2.0

//! IVF-PQ search: probe the nearest cells, rank their entries by PQ
//! distance, and rerank the best of them by exact FP32 distance.

use roaring::RoaringBitmap;

use crate::distance::{DistanceMetric, distance};
use crate::error::{VectorError, check_dim};
use crate::hnsw::SearchResult;

use super::index::{IvfPqIndex, residual};

/// PQ-ranked candidates reranked exactly, per result asked for.
const RERANK_PER_RESULT: usize = 3;
/// Fewest PQ-ranked candidates reranked exactly.
const MIN_RERANK: usize = 20;

/// One PQ-ranked entry: id, PQ distance, cell, position in the cell.
struct Candidate {
    id: u32,
    pq_distance: f32,
    cell: usize,
    pos: usize,
}

impl IvfPqIndex {
    /// The `top_k` live entries nearest to `query` under the index metric.
    ///
    /// A query without the index dimension fails with
    /// [`VectorError::DimensionMismatch`]. A distance table over the memory
    /// budget fails the search: skipping its cell would drop results.
    pub fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<SearchResult>, VectorError> {
        self.search_with(query, top_k, self.params.metric, None)
    }

    /// The `top_k` live entries nearest to `query` under `metric`, keeping
    /// only ids in `filter` when one is given. Fails as [`Self::search`].
    pub fn search_with(
        &self,
        query: &[f32],
        top_k: usize,
        metric: DistanceMetric,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.search_admit(query, top_k, metric, &|id: u32, deleted: bool| {
            !deleted && filter.is_none_or(|f| f.contains(id))
        })
    }

    /// The `top_k` entries `filter` admits nearest to `query` under
    /// `metric`. A soft-deleted entry is scored only when `filter` revives
    /// it. Fails as [`Self::search`].
    pub fn search_admitting(
        &self,
        query: &[f32],
        top_k: usize,
        metric: DistanceMetric,
        filter: &crate::visibility::NodeFilter<'_>,
    ) -> Result<Vec<SearchResult>, VectorError> {
        self.search_admit(query, top_k, metric, &|id: u32, deleted: bool| {
            filter.admits(id, deleted)
        })
    }

    /// The `top_k` entries `admit` accepts nearest to `query` under
    /// `metric`. `admit` takes an entry id and whether it is soft-deleted.
    fn search_admit<A: Fn(u32, bool) -> bool>(
        &self,
        query: &[f32],
        top_k: usize,
        metric: DistanceMetric,
        admit: &A,
    ) -> Result<Vec<SearchResult>, VectorError> {
        check_dim(self.dim, query.len())?;
        let Some(pq) = &self.pq else {
            return Ok(Vec::new());
        };
        if top_k == 0 || self.slots.is_empty() {
            return Ok(Vec::new());
        }

        let mut probe: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, distance(query, c, self.params.metric)))
            .collect();
        probe.sort_by(|a, b| a.1.total_cmp(&b.1));
        probe.truncate(self.params.nprobe.max(1));

        let m = pq.m;
        let mut candidates: Vec<Candidate> = Vec::new();
        for &(cell_idx, _) in &probe {
            let cell = &self.cells[cell_idx];
            if cell.ids.is_empty() {
                continue;
            }
            let table = pq.build_distance_table(&residual(query, &self.centroids[cell_idx]))?;
            for (pos, &id) in cell.ids.iter().enumerate() {
                if !admit(id, self.deleted.contains(id)) {
                    continue;
                }
                let code = &cell.codes[pos * m..(pos + 1) * m];
                candidates.push(Candidate {
                    id,
                    pq_distance: pq.asymmetric_distance(&table, code),
                    cell: cell_idx,
                    pos,
                });
            }
        }

        let pool = top_k.saturating_mul(RERANK_PER_RESULT).max(MIN_RERANK);
        if candidates.len() > pool {
            candidates.select_nth_unstable_by(pool, |a, b| a.pq_distance.total_cmp(&b.pq_distance));
            candidates.truncate(pool);
        }

        let dim = self.dim;
        let mut results: Vec<SearchResult> = candidates
            .into_iter()
            .map(|c| {
                let start = c.pos * dim;
                let vector = &self.cells[c.cell].vectors[start..start + dim];
                SearchResult {
                    id: c.id,
                    distance: distance(query, vector, metric),
                }
            })
            .collect();
        results.sort_by(|a, b| a.distance.total_cmp(&b.distance));
        results.truncate(top_k);
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivf::IvfPqParams;
    use crate::test_support::test_memory;

    fn make_vectors(n: usize, dim: usize) -> Vec<Vec<f32>> {
        (0..n)
            .map(|i| (0..dim).map(|d| ((i * dim + d) as f32) * 0.01).collect())
            .collect()
    }

    fn index_with(vecs: &[Vec<f32>], params: IvfPqParams) -> IvfPqIndex {
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut idx = IvfPqIndex::new(vecs[0].len(), params);
        idx.train(&refs, test_memory()).unwrap();
        idx.add_batch(&refs).unwrap();
        idx
    }

    #[test]
    fn train_and_search_finds_the_exact_match_first() {
        let vecs = make_vectors(1000, 16);
        let idx = index_with(
            &vecs,
            IvfPqParams {
                n_cells: 32,
                pq_m: 4,
                pq_k: 32,
                nprobe: 8,
                metric: DistanceMetric::L2,
            },
        );
        assert_eq!(idx.len(), 1000);
        let results = idx.search(&vecs[500], 5).unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].id, 500, "the exact rerank puts the match first");
        assert_eq!(results[0].distance, 0.0);
    }

    #[test]
    fn probing_every_cell_returns_every_live_entry_once() {
        let vecs = make_vectors(64, 8);
        let mut idx = index_with(
            &vecs,
            IvfPqParams {
                n_cells: 4,
                pq_m: 4,
                pq_k: 8,
                nprobe: 4,
                metric: DistanceMetric::L2,
            },
        );
        idx.delete(7);
        let mut ids: Vec<u32> = idx
            .search(&vecs[0], 100)
            .unwrap()
            .iter()
            .map(|r| r.id)
            .collect();
        ids.sort_unstable();
        let expected: Vec<u32> = (0..64).filter(|id| *id != 7).collect();
        assert_eq!(ids, expected);
    }

    #[test]
    fn a_filter_keeps_only_its_ids() {
        let vecs = make_vectors(64, 8);
        let idx = index_with(
            &vecs,
            IvfPqParams {
                n_cells: 4,
                pq_m: 4,
                pq_k: 8,
                nprobe: 4,
                metric: DistanceMetric::L2,
            },
        );
        let filter: RoaringBitmap = [3u32, 40, 41].into_iter().collect();
        let mut ids: Vec<u32> = idx
            .search_with(&vecs[0], 10, DistanceMetric::L2, Some(&filter))
            .unwrap()
            .iter()
            .map(|r| r.id)
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![3, 40, 41]);
    }

    #[test]
    fn an_untrained_index_finds_nothing() {
        let idx = IvfPqIndex::new(8, IvfPqParams::default());
        assert!(idx.search(&[0.0; 8], 5).unwrap().is_empty());
    }
}
