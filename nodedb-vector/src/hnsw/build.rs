// SPDX-License-Identifier: Apache-2.0

//! HNSW insert algorithm (Malkov & Yashunin, Algorithm 1).

use crate::dtype::cast_from_f32;
use crate::error::VectorError;
use crate::hnsw::graph::{Candidate, HnswIndex, Node, NodeStorage};
use crate::hnsw::search::search_layer;
use nodedb_types::vector_dtype::VectorStorageDtype;

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

        let storage = match self.params.dtype {
            VectorStorageDtype::F32 => NodeStorage::F32(vector.clone()),
            dtype => NodeStorage::Bytes {
                dtype,
                bytes: cast_from_f32(&vector, dtype),
            },
        };
        let node = Node {
            storage,
            neighbors: (0..=new_layer).map(|_| Vec::new()).collect(),
            deleted: false,
        };
        self.nodes.push(node);

        let Some(ep) = self.entry_point else {
            self.entry_point = Some(new_id);
            self.max_layer = new_layer;
            return Ok(());
        };

        // One probe serves every distance against the new vector below.
        let probe = self.probe(&vector);

        let mut current_ep = ep;

        // Phase 1: Greedy descent from top layer to new_layer + 1.
        if self.max_layer > new_layer {
            for layer in (new_layer + 1..=self.max_layer).rev() {
                let results = search_layer(self, &probe, current_ep, 1, layer, None, 0);
                if let Some(nearest) = results.first() {
                    current_ep = nearest.id;
                }
            }
        }

        // Phase 2: Insert at each layer from min(new_layer, max_layer) down to 0.
        let insert_top = new_layer.min(self.max_layer);
        let mut reached_live = false;
        for layer in (0..=insert_top).rev() {
            let ef = self.params.ef_construction;
            let candidates = search_layer(self, &probe, current_ep, ef, layer, None, 0);
            if layer == 0 {
                reached_live = !candidates.is_empty();
            }

            let m = self.max_neighbors(layer);
            let selected = select_neighbors_heuristic(self, &candidates, m);

            self.nodes[new_id as usize].neighbors[layer] = selected.iter().map(|c| c.id).collect();

            for neighbor in &selected {
                let nid = neighbor.id as usize;
                self.nodes[nid].neighbors[layer].push(new_id);

                if self.nodes[nid].neighbors[layer].len() > m {
                    self.prune_neighbors(neighbor.id, layer, m);
                }
            }

            if let Some(nearest) = candidates.first() {
                current_ep = nearest.id;
            }
        }

        // `search_layer` admits only live nodes, and with fewer than `ef`
        // results it visits every node it can reach. No layer-0 candidate
        // means no live node is reachable from the entry point, so the new
        // node becomes the entry point.
        if !reached_live || new_layer > self.max_layer {
            self.entry_point = Some(new_id);
            self.max_layer = new_layer;
        }

        Ok(())
    }

    /// Prune a node's neighbor list using the diversity heuristic.
    fn prune_neighbors(&mut self, node_id: u32, layer: usize, m: usize) {
        let metric = self.params.metric;
        let node_idx = node_id as usize;
        let selected: Vec<u32> = {
            let probe = self.node_probe(node_id);
            let mut candidates: Vec<Candidate> = self.nodes[node_idx].neighbors[layer]
                .iter()
                .map(|&nid| Candidate {
                    id: nid,
                    dist: probe
                        .as_ref()
                        .and_then(|p| self.dist_to_node(p, nid, metric))
                        .unwrap_or(f32::INFINITY),
                })
                .collect();
            candidates.sort_unstable_by(|a, b| a.dist.total_cmp(&b.dist));
            select_neighbors_heuristic(self, &candidates, m)
                .iter()
                .map(|c| c.id)
                .collect()
        };
        self.nodes[node_idx].neighbors[layer] = selected;
    }
}

/// Heuristic neighbor selection (Malkov & Yashunin, Algorithm 4).
pub(crate) fn select_neighbors_heuristic(
    index: &HnswIndex,
    candidates: &[Candidate],
    m: usize,
) -> Vec<Candidate> {
    let metric = index.params.metric;
    let mut selected: Vec<Candidate> = Vec::with_capacity(m);

    for candidate in candidates {
        if selected.len() >= m {
            break;
        }

        // Borrowed from node storage: the pairwise loop below allocates nothing.
        let candidate_probe = index.node_probe(candidate.id);
        let is_diverse = selected.iter().all(|s| {
            let dist_to_selected = candidate_probe
                .as_ref()
                .and_then(|p| index.dist_to_node(p, s.id, metric))
                .unwrap_or(f32::MAX);
            candidate.dist <= dist_to_selected
        });

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
    use nodedb_types::vector_dtype::VectorStorageDtype;

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
                dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
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
            let results = idx.search(&query, 1, 32).unwrap();
            assert_eq!(results[0].id, target, "node {target} not reachable");
        }
    }

    /// F16/BF16 inserts score node pairs through the borrowed encoded bytes.
    /// Every node must stay reachable by an exact query.
    #[test]
    fn narrow_dtype_inserts_reach_every_node() {
        use crate::test_support::{seeded_index, seeded_vectors};

        let vectors = seeded_vectors(120, 8, 0x2545_F491_4F6C_DD1D);
        for dtype in [VectorStorageDtype::F16, VectorStorageDtype::BF16] {
            for metric in [DistanceMetric::L2, DistanceMetric::Cosine] {
                let idx = seeded_index(&vectors, metric, dtype);
                for (id, v) in vectors.iter().enumerate() {
                    // `ef` covers every node, so a miss means a node is unreachable.
                    let hits = idx.search(v, 1, 128).unwrap();
                    assert_eq!(
                        hits.first().map(|h| h.id),
                        Some(id as u32),
                        "dtype={dtype:?} metric={metric:?}: exact query misses node {id}"
                    );
                }
                for node in &idx.nodes {
                    assert!(node.neighbors[0].len() <= idx.params.m0);
                }
            }
        }
    }

    /// A graph-only restore with an attached backing must insert exactly like a
    /// full restore. Restored nodes hold no local vector, so neighbor pruning
    /// and the diversity heuristic read them through the backing. A new node
    /// must gain incoming edges, or search can never reach it.
    ///
    /// `segment_backing` is gated off on wasm32, so the test is too.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn inserts_after_graph_only_restore_match_a_full_restore() {
        use crate::test_support::{graph_only_restore_with_backing, seeded_index, seeded_vectors};

        const DIM: usize = 8;
        let base = seeded_vectors(200, DIM, 0x9E37_79B9_7F4A_7C15);
        let extra = seeded_vectors(5, DIM, 0xD1B5_4A32_D192_ED03);

        let src = seeded_index(&base, DistanceMetric::L2, VectorStorageDtype::F32);
        let mut full = HnswIndex::from_checkpoint(&src.checkpoint_to_bytes().unwrap())
            .unwrap()
            .expect("full checkpoint must be recognized");
        let mut graph_only = graph_only_restore_with_backing(&src, &base);

        for v in &extra {
            full.insert(v.clone()).unwrap();
            graph_only.insert(v.clone()).unwrap();
        }

        let neighbors = graph_only.export_neighbors();
        assert_eq!(
            full.export_neighbors(),
            neighbors,
            "a graph-only restore must build the same graph as a full restore"
        );

        for (offset, v) in extra.iter().enumerate() {
            let new_id = (base.len() + offset) as u32;
            let hits = graph_only.search(v, 1, 64).unwrap();
            assert_eq!(
                hits.first().map(|h| h.id),
                Some(new_id),
                "exact query for inserted node {new_id} returns it, got {hits:?}"
            );
            assert!(
                hits[0].distance < 1e-6,
                "exact query for node {new_id} scores ~0, got {}",
                hits[0].distance
            );
            assert!(
                neighbors
                    .iter()
                    .any(|layers| layers.first().is_some_and(|l0| l0.contains(&new_id))),
                "node {new_id} has no incoming layer-0 edge"
            );
        }

        let m = graph_only.params().m;
        let m0 = graph_only.params().m0;
        for (id, layers) in neighbors.iter().enumerate() {
            for (layer, list) in layers.iter().enumerate() {
                let cap = if layer == 0 { m0 } else { m };
                assert!(
                    list.len() <= cap,
                    "node {id} layer {layer} has {} neighbors, cap {cap}",
                    list.len()
                );
            }
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
            let results = idx.search(&query, 1, 32).unwrap();
            assert!(!results.is_empty());
            let found_vec = idx.get_vector(results[0].id).unwrap();
            assert_eq!(found_vec[0], target_old_id as f32);
        }
    }

    /// An index whose every node is deleted keeps serving vectors inserted
    /// afterwards. Many seeds, because one seed can give a new node a layer
    /// above the deleted entry point, which hides the defect.
    #[test]
    fn insert_after_every_node_is_deleted_stays_searchable() {
        for seed in 1..=32u64 {
            let mut idx = HnswIndex::with_seed(
                3,
                HnswParams {
                    m: 4,
                    m0: 8,
                    ef_construction: 32,
                    metric: DistanceMetric::L2,
                    dtype: VectorStorageDtype::F32,
                },
                seed,
            );
            idx.insert(vec![1.0, 0.0, 0.0]).unwrap();
            assert!(idx.delete(0));
            let fresh = [[0.0, 1.0, 0.0], [0.0, 0.0, 1.0], [0.5, 0.5, 0.0]];
            for v in fresh {
                idx.insert(v.to_vec()).unwrap();
            }
            let ep = idx.entry_point().unwrap();
            assert!(!idx.is_deleted(ep), "seed {seed}: entry point is deleted");
            for (i, v) in fresh.iter().enumerate() {
                let hits = idx.search(v, 1, 32).unwrap();
                assert_eq!(
                    hits.first().map(|h| h.id),
                    Some(i as u32 + 1),
                    "seed {seed}: node {} unreachable",
                    i + 1
                );
            }
        }
    }
}
