// SPDX-License-Identifier: Apache-2.0

//! Tombstone compaction for [`HnswIndex`]: drops deleted nodes and remaps ids.

use super::HnswIndex;
use crate::hnsw::graph::types::Node;

impl HnswIndex {
    /// Compact the index by removing all tombstoned nodes.
    ///
    /// Returns the number of removed nodes. See `compact_with_map` for the
    /// variant that also returns the old→new id remapping.
    pub fn compact(&mut self) -> usize {
        self.compact_with_map().0
    }

    /// Compact and return both the removed count and the old→new id map.
    ///
    /// `id_map[old_local]` = new_local, or `u32::MAX` if the node was
    /// tombstoned (removed). Repairs edges so the entry point reaches every
    /// remaining node.
    pub fn compact_with_map(&mut self) -> (usize, Vec<u32>) {
        let tombstone_count = self.tombstone_count();
        if tombstone_count == 0 {
            let identity: Vec<u32> = (0..self.nodes.len() as u32).collect();
            return (0, identity);
        }
        self.ensure_mutable_neighbors();
        self.bridge_deleted_neighbors();

        let mut id_map: Vec<u32> = Vec::with_capacity(self.nodes.len());
        let mut new_id = 0u32;
        for node in &self.nodes {
            if node.deleted {
                id_map.push(u32::MAX);
            } else {
                id_map.push(new_id);
                new_id += 1;
            }
        }

        let mut new_nodes: Vec<Node> = Vec::with_capacity(new_id as usize);
        for node in self.nodes.drain(..) {
            if node.deleted {
                continue;
            }
            let remapped_neighbors: Vec<Vec<u32>> = node
                .neighbors
                .into_iter()
                .map(|layer_neighbors| {
                    layer_neighbors
                        .into_iter()
                        .filter_map(|old_nid| {
                            let new_nid = id_map[old_nid as usize];
                            if new_nid == u32::MAX {
                                None
                            } else {
                                Some(new_nid)
                            }
                        })
                        .collect()
                })
                .collect();
            new_nodes.push(Node {
                storage: node.storage,
                neighbors: remapped_neighbors,
                deleted: false,
            });
        }

        self.entry_point = if let Some(old_ep) = self.entry_point {
            let new_ep = id_map[old_ep as usize];
            if new_ep == u32::MAX {
                new_nodes
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, n)| n.neighbors.len())
                    .map(|(i, _)| i as u32)
            } else {
                Some(new_ep)
            }
        } else {
            None
        };

        self.max_layer = new_nodes
            .iter()
            .map(|n| n.neighbors.len().saturating_sub(1))
            .max()
            .unwrap_or(0);

        self.nodes = new_nodes;
        self.connect_unreachable();
        (tombstone_count, id_map)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use nodedb_types::vector_dtype::VectorStorageDtype;

    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};

    fn params(m: usize, m0: usize) -> HnswParams {
        HnswParams {
            m,
            m0,
            ef_construction: 32,
            metric: DistanceMetric::L2,
            dtype: VectorStorageDtype::F32,
        }
    }

    /// Nodes the entry point reaches over layer-0 edges.
    fn reached_at_layer0(idx: &HnswIndex) -> Vec<bool> {
        let mut reached = vec![false; idx.nodes.len()];
        let Some(ep) = idx.entry_point() else {
            return reached;
        };
        reached[ep as usize] = true;
        let mut queue = VecDeque::from([ep]);
        while let Some(cur) = queue.pop_front() {
            for &next in &idx.nodes[cur as usize].neighbors[0] {
                if !reached[next as usize] {
                    reached[next as usize] = true;
                    queue.push_back(next);
                }
            }
        }
        reached
    }

    /// Points 0, 1, 2, 3 on a line with `m0 = 2`. Node 1 prunes its edge to
    /// node 3, so node 2 holds the only edge into node 3. Search walks
    /// through deleted node 2 before compaction. Compaction drops node 2 and
    /// must not strand node 3.
    #[test]
    fn compaction_keeps_node_whose_only_in_edge_came_from_a_deleted_node() {
        // `m = 2^40` puts every node on layer 0, so the graph is fixed.
        let mut idx = HnswIndex::with_seed(3, params(1 << 40, 2), 1);
        for x in 0..4u8 {
            idx.insert(vec![f32::from(x), 0.0, 0.0]).unwrap();
        }
        assert_eq!(idx.max_layer(), 0);
        let into_3: Vec<usize> = idx
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.neighbors[0].contains(&3))
            .map(|(id, _)| id)
            .collect();
        assert_eq!(
            into_3,
            vec![2],
            "fixture: node 2 holds the only edge into node 3"
        );

        assert!(idx.delete(2));
        let query = [3.0, 0.0, 0.0];
        assert_eq!(idx.search(&query, 1, 32).unwrap()[0].id, 3);

        let (removed, id_map) = idx.compact_with_map();
        assert_eq!(removed, 1);
        let hits = idx.search(&query, 1, 32).unwrap();
        assert_eq!(
            hits.first().map(|h| h.id),
            Some(id_map[3]),
            "node 3 is unreachable after compaction, got {hits:?}"
        );
        assert!(reached_at_layer0(&idx).iter().all(|&r| r));
    }

    /// Heavy deletes cut many paths. After compaction the entry point reaches
    /// every node, an exact query finds every node, and no list exceeds its cap.
    #[test]
    fn compaction_after_heavy_deletes_keeps_every_node_reachable() {
        use crate::test_support::seeded_vectors;

        let vectors = seeded_vectors(400, 8, 0x9E37_79B9_7F4A_7C15);
        let mut idx = HnswIndex::with_seed(
            8,
            HnswParams {
                ef_construction: 64,
                ..params(4, 8)
            },
            7,
        );
        for v in &vectors {
            idx.insert(v.clone()).unwrap();
        }
        for id in 0..vectors.len() as u32 {
            if id % 5 != 0 {
                assert!(idx.delete(id));
            }
        }
        let (_, id_map) = idx.compact_with_map();

        let reached = reached_at_layer0(&idx);
        let lost = reached.iter().filter(|&&r| !r).count();
        assert_eq!(lost, 0, "{lost} of {} nodes unreachable", reached.len());
        for old in (0..vectors.len()).step_by(5) {
            let hits = idx.search(&vectors[old], 1, idx.len()).unwrap();
            assert_eq!(
                hits.first().map(|h| h.id),
                Some(id_map[old]),
                "old node {old} not found"
            );
        }
        let (m, m0) = (idx.params().m, idx.params().m0);
        for node in &idx.nodes {
            for (layer, list) in node.neighbors.iter().enumerate() {
                assert!(list.len() <= if layer == 0 { m0 } else { m });
            }
        }
    }
}
