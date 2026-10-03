// SPDX-License-Identifier: Apache-2.0

//! Bidirectional shortest paths over CSR adjacency.

use std::collections::{HashMap, hash_map::Entry};

#[cfg(test)]
use super::DEFAULT_MAX_VISITED;
use crate::csr::CsrIndex;
use crate::overlay_delta::GraphOverlayDelta;
use crate::path_params::ShortestPathParams;

impl CsrIndex {
    /// Shortest path via bidirectional BFS.
    ///
    /// `max_visited` caps the combined forward+backward visited set to prevent
    /// supernode fan-out explosion. Pass [`crate::traversal::DEFAULT_MAX_VISITED`]
    /// for the standard limit.
    ///
    /// `frontier_bitmap`: when `Some`, only nodes whose surrogate is present in
    /// the bitmap are eligible for expansion. The start and end nodes are not
    /// gated.
    ///
    /// `overlay`: when `Some` and non-empty, the search observes the
    /// transaction's staged edge writes/deletes (read-your-own-writes),
    /// including a path that must pass through a node reachable only via a
    /// staged edge. When `None` or empty, the durable-only dense bidirectional
    /// fast path runs unchanged.
    pub fn shortest_path(
        &self,
        params: ShortestPathParams<'_>,
        overlay: Option<&GraphOverlayDelta>,
    ) -> Option<Vec<String>> {
        match overlay {
            Some(ov) if !ov.is_empty() => self.shortest_path_overlay(params, ov),
            _ => self.shortest_path_dense(params),
        }
    }

    /// Durable-only bidirectional BFS over the dense u32 CSR ids.
    ///
    /// Each step expands one forward level, then one backward level. A level
    /// relaxes its edges in `(neighbour, frontier node)` name order, and the
    /// search stops at the first node both sides reached. The cap is checked
    /// before each step.
    ///
    /// Round `k` meets on a path of `2k - 1` edges (forward level) or `2k`
    /// edges (backward level), so the first meeting is a shortest path, and
    /// `max_depth.div_ceil(2)` rounds reach every path of at most `max_depth`
    /// edges. A meeting on a longer path returns `None`.
    fn shortest_path_dense(&self, params: ShortestPathParams<'_>) -> Option<Vec<String>> {
        let ShortestPathParams {
            src,
            dst,
            label_filter,
            max_depth,
            max_visited,
            frontier_bitmap,
        } = params;
        let src_id = *self.node_to_id.get(src)?;
        let dst_id = *self.node_to_id.get(dst)?;
        if src_id == dst_id {
            return Some(vec![src.to_string()]);
        }

        let labels = self.label_filter(label_filter);
        // The endpoints are never gated: a bitmap that leaves them out still
        // admits the edges that reach them.
        let in_bitmap = |id: u32| {
            id == src_id
                || id == dst_id
                || frontier_bitmap.is_none_or(|bm| {
                    bm.contains(nodedb_types::Surrogate::new(self.node_surrogate_raw(id)))
                })
        };
        let within_depth =
            |path: Vec<String>| (path.len().saturating_sub(1) <= max_depth).then_some(path);
        let mut fwd_parent: HashMap<u32, u32> = HashMap::new();
        let mut bwd_parent: HashMap<u32, u32> = HashMap::new();
        fwd_parent.insert(src_id, src_id);
        bwd_parent.insert(dst_id, dst_id);

        let mut fwd_frontier: Vec<u32> = vec![src_id];
        let mut bwd_frontier: Vec<u32> = vec![dst_id];

        for _round in 0..max_depth.div_ceil(2) {
            if fwd_parent.len() + bwd_parent.len() >= max_visited {
                break;
            }

            // Each level's edges are relaxed in (neighbour, frontier node)
            // name order, so the parent a node gets, and the meeting point,
            // do not depend on how the edges are stored. A cluster coordinator
            // relaxes cross-shard hops in the same order.
            let mut candidates: Vec<(u32, u32)> = Vec::new();
            for &node in &fwd_frontier {
                self.record_access(node);
                for (lid, neighbor) in self.dense_iter_out(node) {
                    if labels.keeps(lid) && in_bitmap(neighbor) {
                        candidates.push((neighbor, node));
                    }
                }
            }
            self.sort_edges_by_name(&mut candidates);
            let mut next_fwd = Vec::new();
            for (neighbor, node) in candidates {
                if let Entry::Vacant(e) = fwd_parent.entry(neighbor) {
                    e.insert(node);
                    next_fwd.push(neighbor);
                }
                if bwd_parent.contains_key(&neighbor) {
                    return within_depth(self.reconstruct_path(neighbor, &fwd_parent, &bwd_parent));
                }
            }
            fwd_frontier = next_fwd;

            let mut candidates: Vec<(u32, u32)> = Vec::new();
            for &node in &bwd_frontier {
                self.record_access(node);
                for (lid, neighbor) in self.dense_iter_in(node) {
                    if labels.keeps(lid) && in_bitmap(neighbor) {
                        candidates.push((neighbor, node));
                    }
                }
            }
            self.sort_edges_by_name(&mut candidates);
            let mut next_bwd = Vec::new();
            for (neighbor, node) in candidates {
                if let Entry::Vacant(e) = bwd_parent.entry(neighbor) {
                    e.insert(node);
                    next_bwd.push(neighbor);
                }
                if fwd_parent.contains_key(&neighbor) {
                    return within_depth(self.reconstruct_path(neighbor, &fwd_parent, &bwd_parent));
                }
            }
            bwd_frontier = next_bwd;

            if fwd_frontier.is_empty() && bwd_frontier.is_empty() {
                break;
            }
        }
        None
    }

    /// Order `(neighbour, frontier node)` edges by the two node names.
    fn sort_edges_by_name(&self, edges: &mut [(u32, u32)]) {
        edges.sort_by(|a, b| {
            let name = |id: u32| self.node_name_checked(id);
            (name(a.0), name(a.1)).cmp(&(name(b.0), name(b.1)))
        });
    }

    fn reconstruct_path(
        &self,
        meeting: u32,
        fwd_parent: &HashMap<u32, u32>,
        bwd_parent: &HashMap<u32, u32>,
    ) -> Vec<String> {
        let mut fwd_path = Vec::new();
        let mut current = meeting;
        loop {
            fwd_path.push(current);
            let parent = fwd_parent[&current];
            if parent == current {
                break;
            }
            current = parent;
        }
        fwd_path.reverse();

        current = bwd_parent[&meeting];
        if current != meeting {
            loop {
                fwd_path.push(current);
                let parent = bwd_parent[&current];
                if parent == current {
                    break;
                }
                current = parent;
            }
        }

        fwd_path
            .into_iter()
            .map(|id| self.id_to_node[id as usize].clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{chain_csr, long_chain_csr, test_memory};

    fn path_params<'a>(
        src: &'a str,
        dst: &'a str,
        label_filter: &'a [&'a str],
        max_depth: usize,
        frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    ) -> ShortestPathParams<'a> {
        ShortestPathParams {
            src,
            dst,
            label_filter,
            max_depth,
            max_visited: DEFAULT_MAX_VISITED,
            frontier_bitmap,
        }
    }

    #[test]
    fn shortest_path_direct() {
        let csr = chain_csr();
        let path = csr
            .shortest_path(path_params("a", "c", &["KNOWS"], 5, None), None)
            .unwrap();
        assert_eq!(path, vec!["a", "b", "c"]);
    }

    #[test]
    fn shortest_path_takes_the_smallest_named_tie() {
        // Two paths of equal length, the `z` one stored first.
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "z").unwrap();
        csr.add_edge("z", "L", "d").unwrap();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "d").unwrap();
        let path = csr
            .shortest_path(path_params("a", "d", &[], 5, None), None)
            .unwrap();
        assert_eq!(path, vec!["a", "b", "d"]);
    }

    #[test]
    fn shortest_path_over_a_long_chain() {
        let csr = long_chain_csr();
        let path = csr
            .shortest_path(path_params("n0", "n50", &["NEXT"], 100, None), None)
            .unwrap();
        assert_eq!(path.len(), 51);
        // 50 edges: a depth of 49 finds none, 50 finds it.
        assert!(
            csr.shortest_path(path_params("n0", "n50", &["NEXT"], 49, None), None)
                .is_none()
        );
        assert!(
            csr.shortest_path(path_params("n0", "n50", &["NEXT"], 50, None), None)
                .is_some()
        );
    }

    #[test]
    fn shortest_path_same_node() {
        let csr = chain_csr();
        let path = csr
            .shortest_path(path_params("a", "a", &[], 5, None), None)
            .unwrap();
        assert_eq!(path, vec!["a"]);
    }

    #[test]
    fn shortest_path_unreachable() {
        let csr = chain_csr();
        let path = csr.shortest_path(path_params("d", "a", &["KNOWS"], 5, None), None);
        assert!(path.is_none());
    }

    #[test]
    fn shortest_path_depth_limit() {
        let csr = chain_csr();
        let path = csr.shortest_path(path_params("a", "d", &["KNOWS"], 1, None), None);
        assert!(path.is_none());
    }

    /// shortest_path with a bitmap that excludes the only intermediate node.
    /// "b" is the only path from "a" to "c" via KNOWS edges; if "b" is blocked
    /// then no path exists.
    #[test]
    fn shortest_path_frontier_bitmap_blocks_intermediate() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let mut csr = chain_csr();
        csr.set_node_surrogate("b", Surrogate::new(10));
        csr.set_node_surrogate("c", Surrogate::new(20));

        // Bitmap that does NOT contain "b".
        let mut bm = SurrogateBitmap::new();
        bm.insert(Surrogate::new(20)); // only "c" is in the bitmap

        let path = csr.shortest_path(path_params("a", "c", &["KNOWS"], 5, Some(&bm)), None);
        // "b" (surrogate 10) is not in the bitmap so expansion through it is
        // blocked, making the path from "a" to "c" unreachable.
        assert!(path.is_none());
    }

    /// A bitmap that holds neither endpoint still admits the edge between
    /// them: the endpoints are never gated.
    #[test]
    fn the_frontier_bitmap_never_gates_the_endpoints() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "c").unwrap();
        csr.set_node_surrogate("a", Surrogate::new(1));
        csr.set_node_surrogate("c", Surrogate::new(3));
        let mut bm = SurrogateBitmap::new();
        bm.insert(Surrogate::new(99));

        let path = csr.shortest_path(path_params("a", "c", &[], 1, Some(&bm)), None);
        assert_eq!(path, Some(vec!["a".to_string(), "c".to_string()]));
    }

    fn params<'a>(labels: &'a [&'a str], depth: usize) -> ShortestPathParams<'a> {
        ShortestPathParams {
            src: "a",
            dst: "d",
            label_filter: labels,
            max_depth: depth,
            max_visited: DEFAULT_MAX_VISITED,
            frontier_bitmap: None,
        }
    }

    #[test]
    fn dense_path_matches_any_listed_label() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "FIRST", "b").unwrap();
        csr.add_edge("b", "SECOND", "d").unwrap();
        assert_eq!(
            csr.shortest_path(params(&["FIRST", "SECOND"], 2), None),
            Some(vec!["a".into(), "b".into(), "d".into()])
        );
        assert!(
            csr.shortest_path(params(&["FIRST", "SECOND"], 1), None)
                .is_none()
        );
        assert!(csr.shortest_path(params(&["FIRST"], 2), None).is_none());
        assert!(csr.shortest_path(params(&["unknown"], 2), None).is_none());
        assert!(csr.shortest_path(params(&[], 2), None).is_some());
        assert!(
            csr.shortest_path(params(&["unknown", "FIRST", "SECOND"], 2), None)
                .is_some()
        );
        assert!(
            csr.shortest_path(params(&["FIRST", "SECOND"], 0), None)
                .is_none()
        );
    }

    /// `a -FIRST-> b -SECOND-> d` plus a shortcut `a -OTHER-> d`. An unknown
    /// label in the set must not widen it to the shortcut.
    #[test]
    fn an_unknown_label_in_the_set_does_not_widen_it() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "FIRST", "b").unwrap();
        csr.add_edge("b", "SECOND", "d").unwrap();
        csr.add_edge("a", "OTHER", "d").unwrap();
        assert_eq!(
            csr.shortest_path(params(&["FIRST", "SECOND", "unknown"], 2), None),
            Some(vec!["a".into(), "b".into(), "d".into()])
        );
        assert!(
            csr.shortest_path(params(&["FIRST", "unknown"], 2), None)
                .is_none()
        );
        assert!(
            csr.shortest_path(params(&["unknown", "never"], 2), None)
                .is_none()
        );
    }

    #[test]
    fn overlay_path_matches_labels_without_durable_ids() {
        let csr = CsrIndex::new(test_memory());
        let mut overlay = GraphOverlayDelta::new();
        overlay.stage_edge("a", "FIRST", "x");
        overlay.stage_edge("x", "SECOND", "d");
        assert_eq!(
            csr.shortest_path(params(&["FIRST", "SECOND"], 2), Some(&overlay)),
            Some(vec!["a".into(), "x".into(), "d".into()])
        );
        assert!(
            csr.shortest_path(params(&["FIRST", "SECOND"], 1), Some(&overlay))
                .is_none()
        );
        assert!(
            csr.shortest_path(params(&["unknown"], 2), Some(&overlay))
                .is_none()
        );
        assert!(
            csr.shortest_path(params(&["FIRST"], 2), Some(&overlay))
                .is_none()
        );
        assert!(csr.shortest_path(params(&[], 2), Some(&overlay)).is_some());
        assert!(
            csr.shortest_path(params(&["FIRST", "SECOND"], 0), Some(&overlay))
                .is_none()
        );
    }

    #[test]
    fn mixed_labels_preserve_forward_and_backward_tombstones() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "FIRST", "b").unwrap();
        csr.add_edge("b", "SECOND", "d").unwrap();
        let labels = ["FIRST", "SECOND"];
        for (src, label, dst) in [("a", "FIRST", "b"), ("b", "SECOND", "d")] {
            let mut overlay = GraphOverlayDelta::new();
            overlay.stage_tombstone(src, label, dst);
            assert!(
                csr.shortest_path(params(&labels, 2), Some(&overlay))
                    .is_none()
            );
        }
    }

    #[test]
    fn odd_length_paths_count_every_edge() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "FIRST", "b").unwrap();
        csr.add_edge("b", "SECOND", "c").unwrap();
        csr.add_edge("c", "FIRST", "d").unwrap();
        let labels = ["FIRST", "SECOND"];
        assert!(csr.shortest_path(params(&labels, 2), None).is_none());
        assert_eq!(
            csr.shortest_path(params(&labels, 3), None),
            Some(vec!["a".into(), "b".into(), "c".into(), "d".into()])
        );

        let staged_csr = CsrIndex::new(test_memory());
        let mut overlay = GraphOverlayDelta::new();
        overlay.stage_edge("a", "FIRST", "b");
        overlay.stage_edge("b", "SECOND", "c");
        overlay.stage_edge("c", "FIRST", "d");
        assert!(
            staged_csr
                .shortest_path(params(&labels, 2), Some(&overlay))
                .is_none()
        );
        assert_eq!(
            staged_csr.shortest_path(params(&labels, 3), Some(&overlay)),
            Some(vec!["a".into(), "b".into(), "c".into(), "d".into()])
        );
    }
}
