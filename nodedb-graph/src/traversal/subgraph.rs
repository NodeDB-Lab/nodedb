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
    /// Every expanded node records each of its edges in `direction`. The last
    /// admitted level is not expanded: it records only its edges in
    /// `direction` whose other endpoint is an admitted node, and admits no
    /// node.
    ///
    /// An empty `label_filter` keeps every edge. Otherwise an edge whose label
    /// is any listed label passes.
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
        label_filter: &[&str],
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
        label_filter: &[&str],
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> Vec<(String, String, String)> {
        let labels = self.label_filter(label_filter);
        let mut visited: HashSet<u32> = HashSet::new();
        let mut frontier: Vec<u32> = Vec::new();
        let mut edges = Vec::new();
        // Each physical edge once: `Both` reaches an edge from both ends, and
        // one triple can be stored under several collections.
        let mut seen: HashSet<(u32, u32, u32)> = HashSet::new();

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
                            if seen.insert((node_id, lid, dst)) {
                                edges.push((
                                    self.id_to_node[node_id as usize].clone(),
                                    self.label_name(lid).to_string(),
                                    self.id_to_node[dst as usize].clone(),
                                ));
                            }
                            if !visited.contains(&dst) {
                                candidates.push(dst);
                            }
                        }
                    }
                }
                if matches!(direction, Direction::In | Direction::Both) {
                    for (lid, src) in self.dense_iter_in(node_id) {
                        if labels.keeps(lid) {
                            if seen.insert((src, lid, node_id)) {
                                edges.push((
                                    self.id_to_node[src as usize].clone(),
                                    self.label_name(lid).to_string(),
                                    self.id_to_node[node_id as usize].clone(),
                                ));
                            }
                            if !visited.contains(&src) {
                                candidates.push(src);
                            }
                        }
                    }
                }
            }
            frontier = self.admit_by_name(candidates, &mut visited, max_visited);
        }

        // The last admitted level is never expanded. Its edges to admitted
        // nodes, itself included, are still part of the subgraph.
        for &node_id in &frontier {
            if matches!(direction, Direction::Out | Direction::Both) {
                for (lid, dst) in self.dense_iter_out(node_id) {
                    if labels.keeps(lid)
                        && visited.contains(&dst)
                        && seen.insert((node_id, lid, dst))
                    {
                        edges.push((
                            self.id_to_node[node_id as usize].clone(),
                            self.label_name(lid).to_string(),
                            self.id_to_node[dst as usize].clone(),
                        ));
                    }
                }
            }
            if matches!(direction, Direction::In | Direction::Both) {
                for (lid, src) in self.dense_iter_in(node_id) {
                    if labels.keeps(lid)
                        && visited.contains(&src)
                        && seen.insert((src, lid, node_id))
                    {
                        edges.push((
                            self.id_to_node[src as usize].clone(),
                            self.label_name(lid).to_string(),
                            self.id_to_node[node_id as usize].clone(),
                        ));
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
    use crate::test_support::{chain_csr, test_memory};

    #[test]
    fn subgraph_materialization() {
        let csr = chain_csr();
        let edges = csr.subgraph(&["a"], &[], Direction::Out, 2, DEFAULT_MAX_VISITED, None);
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
        let mut edges = csr.subgraph(&["a"], &[], Direction::Out, 3, 3, None);
        edges.sort();
        // Level 1 fills the cap, so no level-1 node expands.
        let expected: Vec<(String, String, String)> = ["b", "m", "z"]
            .iter()
            .map(|dst| ("a".to_string(), "L".to_string(), dst.to_string()))
            .collect();
        assert_eq!(edges, expected);
    }

    #[test]
    fn both_directions_return_each_physical_edge_once() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "a").unwrap();
        csr.add_edge("a", "SELF", "a").unwrap();
        csr.add_edge_in_collection("a", "L", "c", "first").unwrap();
        csr.add_edge_in_collection("a", "L", "c", "second").unwrap();
        let mut edges = csr.subgraph(&["a"], &[], Direction::Both, 3, DEFAULT_MAX_VISITED, None);
        edges.sort();
        let expected: Vec<(String, String, String)> = [
            ("a", "L", "b"),
            ("a", "L", "c"),
            ("a", "SELF", "a"),
            ("b", "L", "a"),
        ]
        .iter()
        .map(|(s, l, d)| (s.to_string(), l.to_string(), d.to_string()))
        .collect();
        assert_eq!(edges, expected);
    }

    fn triples(edges: &[(&str, &str, &str)]) -> Vec<(String, String, String)> {
        let mut out: Vec<(String, String, String)> = edges
            .iter()
            .map(|(s, l, d)| (s.to_string(), l.to_string(), d.to_string()))
            .collect();
        out.sort();
        out
    }

    fn boundary_csr() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
        for (src, dst) in [
            ("a", "b"),
            ("a", "c"),
            ("b", "c"),
            ("c", "b"),
            ("c", "a"),
            ("c", "x"),
        ] {
            csr.add_edge(src, "L", dst).unwrap();
        }
        csr
    }

    #[test]
    fn boundary_nodes_record_edges_among_admitted_nodes() {
        let csr = boundary_csr();
        // A non-empty overlay runs the string-keyed path.
        let mut staged = GraphOverlayDelta::new();
        staged.stage_tombstone("q", "L", "r");
        for overlay in [None, Some(&staged)] {
            let mut edges =
                csr.subgraph(&["a"], &[], Direction::Out, 1, DEFAULT_MAX_VISITED, overlay);
            edges.sort();
            assert_eq!(
                edges,
                triples(&[
                    ("a", "L", "b"),
                    ("a", "L", "c"),
                    ("b", "L", "c"),
                    ("c", "L", "a"),
                    ("c", "L", "b"),
                ]),
                "c -> x leaves the admitted set"
            );
        }
    }

    #[test]
    fn boundary_edges_follow_the_walk_direction() {
        let mut csr = CsrIndex::new(test_memory());
        for (src, dst) in [("b", "a"), ("c", "a"), ("b", "c"), ("a", "z")] {
            csr.add_edge(src, "L", dst).unwrap();
        }
        let mut edges = csr.subgraph(&["a"], &[], Direction::In, 1, DEFAULT_MAX_VISITED, None);
        edges.sort();
        assert_eq!(
            edges,
            triples(&[("b", "L", "a"), ("b", "L", "c"), ("c", "L", "a")])
        );
    }

    #[test]
    fn depth_zero_records_only_the_start_nodes_edges_among_themselves() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "a").unwrap();
        csr.add_edge("a", "L", "b").unwrap();
        let edges = csr.subgraph(&["a"], &[], Direction::Out, 0, DEFAULT_MAX_VISITED, None);
        assert_eq!(edges, triples(&[("a", "L", "a")]));
    }

    #[test]
    fn a_label_set_returns_parallel_edges_of_both_labels() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "K", "b").unwrap();
        csr.add_edge("a", "W", "b").unwrap();
        csr.add_edge("a", "X", "c").unwrap();
        let mut edges = csr.subgraph(
            &["a"],
            &["K", "W"],
            Direction::Out,
            2,
            DEFAULT_MAX_VISITED,
            None,
        );
        edges.sort();
        let expected: Vec<(String, String, String)> = [("a", "K", "b"), ("a", "W", "b")]
            .iter()
            .map(|(s, l, d)| (s.to_string(), l.to_string(), d.to_string()))
            .collect();
        assert_eq!(edges, expected);
    }
}
