//! HNSW insert algorithm (Malkov & Yashunin, Algorithm 1).

use crate::error::VectorError;
use crate::hnsw::graph::{Candidate, HnswIndex, Node};
use crate::hnsw::search::search_layer;

impl HnswIndex {
    /// Insert a vector into the index.
    ///
    /// 1. Assign a random layer using the exponential distribution
    /// 2. Greedily descend from the entry point to the new node's layer + 1
    /// 3. At each layer from the node's layer down to 0, search for nearest
    ///    neighbors, select via the diversity heuristic, and add bidirectional edges
    /// 4. Prune over-connected nodes to maintain the M/M0 invariant
    pub fn insert(&mut self, vector: Vec<f32>) -> Result<(), VectorError> {
        // Materialize flat neighbor storage on first mutation.
        self.ensure_mutable_neighbors();

        if vector.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }

        let new_id = self.nodes.len() as u32;
        let new_layer = self.random_layer();

        let node = Node {
            vector,
            neighbors: (0..=new_layer).map(|_| Vec::new()).collect(),
            deleted: false,
        };
        self.nodes.push(node);

        let Some(ep) = self.entry_point else {
            self.entry_point = Some(new_id);
            self.max_layer = new_layer;
            return Ok(());
        };

        // Clone the query vector to avoid aliasing self.nodes during mutation.
        let query = self.nodes[new_id as usize].vector.clone();

        let mut current_ep = ep;

        // Phase 1: Greedy descent from top layer to new_layer + 1.
        if self.max_layer > new_layer {
            for layer in (new_layer + 1..=self.max_layer).rev() {
                let results = search_layer(self, &query, current_ep, 1, layer, None, 0);
                if let Some(nearest) = results.first() {
                    current_ep = nearest.id;
                }
            }
        }

        // Phase 2: Insert at each layer from min(new_layer, max_layer) down to 0.
        let insert_top = new_layer.min(self.max_layer);
        for layer in (0..=insert_top).rev() {
            let ef = self.params.ef_construction;
            let candidates = search_layer(self, &query, current_ep, ef, layer, None, 0);

            let m = self.max_neighbors(layer);
            let selected = select_neighbors_heuristic(self, &candidates, m);

            self.nodes[new_id as usize].neighbors[layer] = selected.iter().map(|c| c.id).collect();

            for neighbor in &selected {
                let nid = neighbor.id as usize;
                self.nodes[nid].neighbors[layer].push(new_id);

                if self.nodes[nid].neighbors[layer].len() > m {
                    let node_vec = self.nodes[nid].vector.clone();
                    self.prune_neighbors(nid, layer, &node_vec, m);
                }
            }

            if let Some(nearest) = candidates.first() {
                current_ep = nearest.id;
            }
        }

        if new_layer > self.max_layer {
            self.entry_point = Some(new_id);
            self.max_layer = new_layer;
        }

        Ok(())
    }

    /// Prune a node's neighbor list using the diversity heuristic.
    fn prune_neighbors(&mut self, node_idx: usize, layer: usize, node_vec: &[f32], m: usize) {
        let neighbor_ids: Vec<u32> = self.nodes[node_idx].neighbors[layer].clone();

        let mut candidates: Vec<Candidate> = neighbor_ids
            .iter()
            .map(|&nid| Candidate {
                id: nid,
                dist: self.dist_to_node(node_vec, nid),
            })
            .collect();
        candidates.sort_unstable_by(|a, b| a.dist.total_cmp(&b.dist));

        let selected = select_neighbors_heuristic(self, &candidates, m);
        self.nodes[node_idx].neighbors[layer] = selected.iter().map(|c| c.id).collect();
    }
}

/// Heuristic neighbor selection (Malkov & Yashunin, Algorithm 4).
fn select_neighbors_heuristic(
    index: &HnswIndex,
    candidates: &[Candidate],
    m: usize,
) -> Vec<Candidate> {
    let mut selected: Vec<Candidate> = Vec::with_capacity(m);

    for candidate in candidates {
        if selected.len() >= m {
            break;
        }

        let candidate_vec = &index.nodes[candidate.id as usize].vector;
        let selected_vecs: Vec<&[f32]> = selected
            .iter()
            .map(|s| index.nodes[s.id as usize].vector.as_slice())
            .collect();

        let is_diverse = crate::batch_distance::is_diverse_batched(
            candidate_vec,
            candidate.dist,
            &selected_vecs,
            index.params.metric,
        );

        if is_diverse {
            selected.push(*candidate);
        }
    }

    // Backfill if heuristic was too aggressive.
    if selected.len() < m {
        let selected_ids: std::collections::HashSet<u32> = selected.iter().map(|c| c.id).collect();
        for candidate in candidates {
            if selected.len() >= m {
                break;
            }
            if !selected_ids.contains(&candidate.id) {
                selected.push(*candidate);
            }
        }
    }

    selected
}

#[cfg(test)]
mod tests {
    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};

    fn make_index() -> HnswIndex {
        HnswIndex::with_seed(
            3,
            HnswParams {
                m: 4,
                m0: 8,
                ef_construction: 32,
                metric: DistanceMetric::L2,
            },
            12345,
        )
    }

    #[test]
    fn insert_single() {
        let mut idx = make_index();
        idx.insert(vec![1.0, 0.0, 0.0]).unwrap();
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.entry_point(), Some(0));
    }

    #[test]
    fn insert_many_maintains_invariants() {
        let mut idx = make_index();
        for i in 0..100 {
            let v = vec![(i as f32) * 0.1, (i as f32) * 0.2, (i as f32) * 0.3];
            idx.insert(v).unwrap();
        }
        assert_eq!(idx.len(), 100);
        assert!(idx.entry_point().is_some());

        for node in &idx.nodes {
            assert!(node.neighbors[0].len() <= idx.params.m0);
        }
        for node in &idx.nodes {
            for (layer, neighbors) in node.neighbors.iter().enumerate().skip(1) {
                assert!(
                    neighbors.len() <= idx.params.m,
                    "layer {layer} neighbor count {} exceeds m={}",
                    neighbors.len(),
                    idx.params.m
                );
            }
        }
    }

    #[test]
    fn all_nodes_reachable_from_entry() {
        let mut idx = make_index();
        for i in 0..20 {
            idx.insert(vec![i as f32, 0.0, 0.0]).unwrap();
        }

        for target in 0..20u32 {
            let query = idx.get_vector(target).unwrap().to_vec();
            let results = idx.search(&query, 1, 32);
            assert_eq!(results[0].id, target, "node {target} not reachable");
        }
    }

    #[test]
    fn compact_removes_tombstones() {
        let mut idx = make_index();
        for i in 0..20u32 {
            idx.insert(vec![i as f32, 0.0, 0.0]).unwrap();
        }

        for i in (0..20u32).step_by(2) {
            assert!(idx.delete(i));
        }
        assert_eq!(idx.compact(), 10);
        assert_eq!(idx.len(), 10);

        for target_old_id in (1..20u32).step_by(2) {
            let query = vec![target_old_id as f32, 0.0, 0.0];
            let results = idx.search(&query, 1, 32);
            assert!(!results.is_empty());
            let found_vec = idx.get_vector(results[0].id).unwrap();
            assert_eq!(found_vec[0], target_old_id as f32);
        }
    }
}
