// SPDX-License-Identifier: Apache-2.0

//! Breadth-first traversal over CSR adjacency.

use std::collections::HashSet;

#[cfg(test)]
use super::DEFAULT_MAX_VISITED;
use crate::bfs_params::BfsParams;
use crate::csr::index::LabelFilter;
use crate::csr::{CsrIndex, Direction};
use crate::overlay_delta::GraphOverlayDelta;

impl CsrIndex {
    /// BFS traversal. Returns all reachable node IDs within max_depth hops.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`crate::traversal::DEFAULT_MAX_VISITED`] for the standard limit.
    ///
    /// `frontier_bitmap`: when `Some`, only nodes whose surrogate is present in the
    /// bitmap are eligible as traversal targets. Start nodes are not gated — only
    /// newly discovered frontier nodes are checked.
    ///
    /// `overlay`: when `Some` and non-empty, the traversal observes the
    /// transaction's staged edge writes/deletes (read-your-own-writes),
    /// including through nodes reachable only via a staged edge. When `None`
    /// or empty, the durable-only dense fast path runs unchanged.
    pub fn traverse_bfs(
        &self,
        params: BfsParams<'_>,
        overlay: Option<&GraphOverlayDelta>,
    ) -> Vec<String> {
        match overlay {
            Some(ov) if !ov.is_empty() => self.traverse_bfs_overlay(params, ov),
            _ => self.traverse_bfs_dense(params),
        }
    }

    /// Durable-only BFS over the dense u32 CSR ids.
    ///
    /// The walk runs level by level. Each level's new nodes are admitted in
    /// node-name order until `max_visited` nodes are visited, so a capped walk
    /// admits the same nodes however the edges are stored. A cluster
    /// coordinator walking the same edges across partitions admits the same
    /// nodes.
    fn traverse_bfs_dense(&self, params: BfsParams<'_>) -> Vec<String> {
        let BfsParams {
            start_nodes,
            label_filter,
            direction,
            max_depth,
            max_visited,
            frontier_bitmap,
        } = params;
        let labels = self.label_filter(label_filter);
        let in_bitmap = |id: u32| {
            frontier_bitmap.is_none_or(|bm| {
                bm.contains(nodedb_types::Surrogate::new(self.node_surrogate_raw(id)))
            })
        };
        let mut visited: HashSet<u32> = HashSet::new();
        let mut frontier: Vec<u32> = Vec::new();
        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node)
                && visited.insert(id)
            {
                frontier.push(id);
            }
        }

        for _depth in 0..max_depth {
            if frontier.is_empty() || visited.len() >= max_visited {
                break;
            }
            let candidates =
                self.level_candidates(&frontier, &labels, direction, &visited, in_bitmap);
            frontier = self.admit_by_name(candidates, &mut visited, max_visited);
        }

        visited
            .into_iter()
            .map(|id| self.id_to_node[id as usize].clone())
            .collect()
    }

    /// Collect the unvisited neighbors of `frontier` that pass `labels` and
    /// `eligible`. Records an access on every frontier node.
    fn level_candidates(
        &self,
        frontier: &[u32],
        labels: &LabelFilter,
        direction: Direction,
        visited: &HashSet<u32>,
        eligible: impl Fn(u32) -> bool,
    ) -> Vec<u32> {
        let mut candidates: Vec<u32> = Vec::new();
        for &node_id in frontier {
            // Track access for hot/cold partition decisions.
            self.record_access(node_id);
            if matches!(direction, Direction::Out | Direction::Both) {
                for (lid, dst) in self.dense_iter_out(node_id) {
                    if labels.keeps(lid) && !visited.contains(&dst) && eligible(dst) {
                        candidates.push(dst);
                    }
                }
            }
            if matches!(direction, Direction::In | Direction::Both) {
                for (lid, src) in self.dense_iter_in(node_id) {
                    if labels.keeps(lid) && !visited.contains(&src) && eligible(src) {
                        candidates.push(src);
                    }
                }
            }
        }
        candidates
    }

    /// Admit `candidates` into `visited` in node-name order, until `visited`
    /// holds `max_visited` nodes. Returns the nodes admitted, in name order.
    pub(crate) fn admit_by_name(
        &self,
        mut candidates: Vec<u32>,
        visited: &mut HashSet<u32>,
        max_visited: usize,
    ) -> Vec<u32> {
        self.sort_by_name(&mut candidates);
        candidates.dedup();
        let mut admitted = Vec::with_capacity(candidates.len());
        for id in candidates {
            if visited.len() >= max_visited {
                break;
            }
            if visited.insert(id) {
                self.prefetch_node(id);
                admitted.push(id);
            }
        }
        admitted
    }

    /// BFS traversal returning nodes with their hop depth.
    ///
    /// An empty `label_filter` keeps every edge. Otherwise an edge whose label
    /// is any listed label passes. The walk runs level by level and admits
    /// each level's new nodes in node-name order until `max_visited` nodes are
    /// visited, like [`Self::traverse_bfs`]. The depth tag saturates at
    /// `u8::MAX`.
    ///
    /// `max_visited` caps the number of nodes visited to prevent supernode fan-out
    /// explosion. Pass [`crate::traversal::DEFAULT_MAX_VISITED`] for the standard limit.
    pub fn traverse_bfs_with_depth(
        &self,
        start_nodes: &[&str],
        label_filter: &[&str],
        direction: Direction,
        max_depth: usize,
        max_visited: usize,
    ) -> Vec<(String, u8)> {
        let labels = self.label_filter(label_filter);
        let mut visited: HashSet<u32> = HashSet::new();
        let mut depths: Vec<(u32, u8)> = Vec::new();
        let mut frontier: Vec<u32> = Vec::new();
        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node)
                && visited.insert(id)
            {
                frontier.push(id);
                depths.push((id, 0));
            }
        }

        for depth in 0..max_depth {
            if frontier.is_empty() || visited.len() >= max_visited {
                break;
            }
            let tag = u8::try_from(depth.saturating_add(1)).unwrap_or(u8::MAX);
            let candidates =
                self.level_candidates(&frontier, &labels, direction, &visited, |_| true);
            frontier = self.admit_by_name(candidates, &mut visited, max_visited);
            depths.extend(frontier.iter().map(|&id| (id, tag)));
        }

        depths
            .into_iter()
            .map(|(id, depth)| (self.id_to_node[id as usize].clone(), depth))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::test_support::{chain_csr, long_chain_csr, test_memory};

    fn bfs(csr: &CsrIndex, labels: &[&str], max_depth: usize, max_visited: usize) -> Vec<String> {
        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: labels,
                direction: Direction::Out,
                max_depth,
                max_visited,
                frontier_bitmap: None,
            },
            None,
        );
        result.sort();
        result
    }

    #[test]
    fn a_label_set_reaches_both_labels() {
        let csr = chain_csr();
        assert_eq!(
            bfs(&csr, &["KNOWS", "WORKS"], 1, DEFAULT_MAX_VISITED),
            vec!["a", "b", "e"]
        );
    }

    #[test]
    fn an_unknown_label_in_a_set_adds_nothing() {
        let csr = chain_csr();
        assert_eq!(
            bfs(&csr, &["KNOWS", "NOPE"], 3, DEFAULT_MAX_VISITED),
            bfs(&csr, &["KNOWS"], 3, DEFAULT_MAX_VISITED)
        );
    }

    #[test]
    fn a_set_of_unknown_labels_returns_the_start_only() {
        let csr = chain_csr();
        assert_eq!(bfs(&csr, &["NOPE"], 3, DEFAULT_MAX_VISITED), vec!["a"]);
    }

    /// The same edges, inserted in two orders, give the same capped walk.
    #[test]
    fn a_capped_walk_over_a_set_ignores_insertion_order() {
        let edges = [
            ("a", "K", "z"),
            ("a", "W", "m"),
            ("a", "K", "b"),
            ("a", "W", "c"),
            ("a", "X", "a0"),
        ];
        let mut forward = CsrIndex::new(test_memory());
        for (src, label, dst) in edges {
            forward.add_edge(src, label, dst).unwrap();
        }
        let mut reverse = CsrIndex::new(test_memory());
        for (src, label, dst) in edges.iter().rev() {
            reverse.add_edge(src, label, dst).unwrap();
        }
        let forward_walk = bfs(&forward, &["K", "W"], 2, 3);
        assert_eq!(forward_walk, vec!["a", "b", "c"]);
        assert_eq!(bfs(&reverse, &["K", "W"], 2, 3), forward_walk);
    }

    /// `a` points at `z`, `m` and `b`, stored in that order. A cap of 3 leaves
    /// room for two of them, and name order picks `b` and `m`.
    #[test]
    fn a_capped_depth_walk_admits_in_name_order() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "z").unwrap();
        csr.add_edge("a", "L", "m").unwrap();
        csr.add_edge("a", "L", "b").unwrap();
        let map: HashMap<String, u8> = csr
            .traverse_bfs_with_depth(&["a"], &[], Direction::Out, 2, 3)
            .into_iter()
            .collect();
        assert_eq!(map.len(), 3);
        assert_eq!(map["a"], 0);
        assert_eq!(map["b"], 1);
        assert_eq!(map["m"], 1);
    }

    /// A walk deeper than 255 hops tags every node past 255 with `u8::MAX`.
    #[test]
    fn the_depth_tag_saturates_past_255() {
        let mut csr = CsrIndex::new(test_memory());
        for i in 0..270 {
            csr.add_edge(&format!("n{i}"), "NEXT", &format!("n{}", i + 1))
                .unwrap();
        }
        let map: HashMap<String, u8> = csr
            .traverse_bfs_with_depth(&["n0"], &["NEXT"], Direction::Out, 300, DEFAULT_MAX_VISITED)
            .into_iter()
            .collect();
        assert_eq!(map.len(), 271);
        assert_eq!(map["n0"], 0);
        assert_eq!(map["n200"], 200);
        assert_eq!(map["n255"], 255);
        assert_eq!(map["n256"], u8::MAX);
        assert_eq!(map["n270"], u8::MAX);
    }

    #[test]
    fn bfs_traversal() {
        let csr = chain_csr();
        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: &["KNOWS"],
                direction: Direction::Out,
                max_depth: 2,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            None,
        );
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn bfs_all_labels() {
        let csr = chain_csr();
        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: &[],
                direction: Direction::Out,
                max_depth: 1,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            None,
        );
        result.sort();
        assert_eq!(result, vec!["a", "b", "e"]);
    }

    /// `a` points at `z`, `m` and `b`, stored in that order. A cap of 3 leaves
    /// room for two of them, and name order picks `b` and `m`.
    #[test]
    fn a_capped_bfs_admits_each_level_in_name_order() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "z").unwrap();
        csr.add_edge("a", "L", "m").unwrap();
        csr.add_edge("a", "L", "b").unwrap();
        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: &[],
                direction: Direction::Out,
                max_depth: 2,
                max_visited: 3,
                frontier_bitmap: None,
            },
            None,
        );
        result.sort();
        assert_eq!(result, vec!["a", "b", "m"]);
    }

    #[test]
    fn bfs_cycle() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.add_edge("c", "L", "a").unwrap();
        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: &[],
                direction: Direction::Out,
                max_depth: 10,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            None,
        );
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn bfs_with_depth() {
        let csr = chain_csr();
        let result =
            csr.traverse_bfs_with_depth(&["a"], &["KNOWS"], Direction::Out, 3, DEFAULT_MAX_VISITED);
        let map: HashMap<String, u8> = result.into_iter().collect();
        assert_eq!(map["a"], 0);
        assert_eq!(map["b"], 1);
        assert_eq!(map["c"], 2);
        assert_eq!(map["d"], 3);
    }

    #[test]
    fn large_graph_bfs() {
        let csr = long_chain_csr();
        let result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["n0"],
                label_filter: &["NEXT"],
                direction: Direction::Out,
                max_depth: 100,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: None,
            },
            None,
        );
        assert_eq!(result.len(), 101);
    }

    /// BFS with a frontier bitmap that includes only "b". Starting from "a",
    /// "b" is reachable but "c" is blocked (its surrogate is not in the bitmap).
    #[test]
    fn bfs_frontier_bitmap_excludes_non_members() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let mut csr = chain_csr();
        // Assign surrogates: b=10, c=20, d=30. "a" and "e" get no surrogate.
        csr.set_node_surrogate("b", Surrogate::new(10));
        csr.set_node_surrogate("c", Surrogate::new(20));
        csr.set_node_surrogate("d", Surrogate::new(30));

        // Bitmap contains only "b" (surrogate 10).
        let mut bm = SurrogateBitmap::new();
        bm.insert(Surrogate::new(10));

        let mut result = csr.traverse_bfs(
            BfsParams {
                start_nodes: &["a"],
                label_filter: &["KNOWS"],
                direction: Direction::Out,
                max_depth: 10,
                max_visited: DEFAULT_MAX_VISITED,
                frontier_bitmap: Some(&bm),
            },
            None,
        );
        result.sort();
        // "a" is the start node (not gated). "b" passes the bitmap. "c" is
        // excluded (surrogate 20 not in bitmap) so traversal stops there.
        assert_eq!(result, vec!["a", "b"]);
    }
}
