// SPDX-License-Identifier: Apache-2.0

//! Subgraph materialization over CSR adjacency.

use std::collections::HashSet;

#[cfg(test)]
use super::DEFAULT_MAX_VISITED;
use crate::csr::{CsrIndex, Direction};
use crate::overlay_delta::GraphOverlayDelta;

impl CsrIndex {
    /// Materialize a subgraph as `(src, label, dst)` edge tuples within
    /// max_depth, expanding in `direction`.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`crate::traversal::DEFAULT_MAX_VISITED`] for the standard limit.
    ///
    /// `overlay`: when `Some` and non-empty, staged edges are included and
    /// staged tombstones subtract durable edges (read-your-own-writes),
    /// including through staged-only intermediate nodes. When `None` or
    /// empty, the durable-only dense path runs unchanged.
    pub fn subgraph(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
        overlay: Option<&GraphOverlayDelta>,
    ) -> Vec<(String, String, String)> {
        match overlay {
            Some(ov) if !ov.is_empty() => self.subgraph_overlay(
                start_nodes,
                label_filter,
                direction,
                max_depth,
                max_visited,
                ov,
            ),
            _ => self.subgraph_dense(start_nodes, label_filter, direction, max_depth, max_visited),
        }
    }

    /// Durable-only subgraph materialization over the dense u32 CSR ids.
    fn subgraph_dense(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> Vec<(String, String, String)> {
        let labels = self.label_filter(label_filter);
        let mut visited: HashSet<u32> = HashSet::new();
        let mut frontier: Vec<u32> = Vec::new();
        let mut edges = Vec::new();

        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node)
                && visited.insert(id)
            {
                frontier.push(id);
            }
        }

        // Level by level, as `traverse_bfs_dense`: every frontier node's edges
        // are recorded, then the level's new nodes are admitted in name order.
        for _depth in 0..max_depth {
            if frontier.is_empty() || visited.len() >= max_visited {
                break;
            }
            let mut candidates: Vec<u32> = Vec::new();
            for &node_id in &frontier {
                self.record_access(node_id);
                if matches!(direction, Direction::Out | Direction::Both) {
                    for (lid, dst) in self.dense_iter_out(node_id) {
                        if labels.keeps(lid) {
                            edges.push((
                                self.id_to_node[node_id as usize].clone(),
                                self.label_name(lid).to_string(),
                                self.id_to_node[dst as usize].clone(),
                            ));
                            if !visited.contains(&dst) {
                                candidates.push(dst);
                            }
                        }
                    }
                }
                if matches!(direction, Direction::In | Direction::Both) {
                    for (lid, src) in self.dense_iter_in(node_id) {
                        if labels.keeps(lid) {
                            edges.push((
                                self.id_to_node[src as usize].clone(),
                                self.label_name(lid).to_string(),
                                self.id_to_node[node_id as usize].clone(),
                            ));
                            if !visited.contains(&src) {
                                candidates.push(src);
                            }
                        }
                    }
                }
            }
            frontier = self.admit_by_name(candidates, &mut visited, max_visited);
        }

        edges
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_memory;

    fn make_csr() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "b").unwrap();
        csr.add_edge("b", "KNOWS", "c").unwrap();
        csr.add_edge("c", "KNOWS", "d").unwrap();
        csr.add_edge("a", "WORKS", "e").unwrap();
        csr
    }

    #[test]
    fn subgraph_materialization() {
        let csr = make_csr();
        let edges = csr.subgraph(&["a"], None, Direction::Out, 2, DEFAULT_MAX_VISITED, None);
        assert_eq!(edges.len(), 3);
        assert!(edges.contains(&("a".into(), "KNOWS".into(), "b".into())));
        assert!(edges.contains(&("a".into(), "WORKS".into(), "e".into())));
        assert!(edges.contains(&("b".into(), "KNOWS".into(), "c".into())));
    }
    #[test]
    fn a_capped_subgraph_expands_only_admitted_levels() {
        // `a` points at `z`, `m` and `b`, stored in that order.
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "z").unwrap();
        csr.add_edge("a", "L", "m").unwrap();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("z", "L", "y").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        let mut edges = csr.subgraph(&["a"], None, Direction::Out, 3, 3, None);
        edges.sort();
        // Level 1 fills the cap, so no level-1 node expands.
        let expected: Vec<(String, String, String)> = ["b", "m", "z"]
            .iter()
            .map(|dst| ("a".to_string(), "L".to_string(), dst.to_string()))
            .collect();
        assert_eq!(edges, expected);
    }
}
