// SPDX-License-Identifier: Apache-2.0

//! Subgraph materialization over CSR adjacency.

use std::collections::{HashSet, VecDeque};

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
        let label_id = label_filter.and_then(|l| self.label_id(l));
        let mut visited: HashSet<u32> = HashSet::new();
        let mut queue: VecDeque<(u32, usize)> = VecDeque::new();
        let mut edges = Vec::new();

        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node)
                && visited.insert(id)
            {
                queue.push_back((id, 0));
            }
        }

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth >= max_depth || visited.len() >= max_visited {
                continue;
            }
            self.record_access(node_id);
            if matches!(direction, Direction::Out | Direction::Both) {
                for (lid, dst) in self.dense_iter_out(node_id) {
                    if label_id.is_none_or(|f| f == lid) {
                        edges.push((
                            self.id_to_node[node_id as usize].clone(),
                            self.label_name(lid).to_string(),
                            self.id_to_node[dst as usize].clone(),
                        ));
                        if visited.len() < max_visited && visited.insert(dst) {
                            queue.push_back((dst, depth + 1));
                        }
                    }
                }
            }
            if matches!(direction, Direction::In | Direction::Both) {
                for (lid, src) in self.dense_iter_in(node_id) {
                    if label_id.is_none_or(|f| f == lid) {
                        edges.push((
                            self.id_to_node[src as usize].clone(),
                            self.label_name(lid).to_string(),
                            self.id_to_node[node_id as usize].clone(),
                        ));
                        if visited.len() < max_visited && visited.insert(src) {
                            queue.push_back((src, depth + 1));
                        }
                    }
                }
            }
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
}
