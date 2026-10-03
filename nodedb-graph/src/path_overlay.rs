// SPDX-License-Identifier: Apache-2.0

//! In-transaction (read-your-own-writes) bidirectional shortest path.
//!
//! Runs only when a non-empty [`GraphOverlayDelta`] is supplied. Like the
//! string-keyed BFS in [`crate::traversal_overlay`], the two frontiers are
//! keyed on the node *string* rather than the u32 CSR id. That choice is what
//! makes bidirectional search correct across staged edges: a node reachable
//! only via a staged edge has no durable surrogate, so the durable dense
//! bidirectional path — which detects a meeting by comparing u32 ids — cannot
//! represent it. With string keys, a staged-only meeting node is simply a
//! string present in both the forward and backward parent maps; the
//! meeting point is a plain string-set intersection and no synthetic
//! (ephemeral) u32 id is ever minted.
//!
//! Each forward frontier node expands its OUT edges (durable CSR out via
//! `node_to_id`, skipping staged tombstones and bitmap-excluded durable
//! targets, unioned with `overlay.out_neighbors`). Each backward frontier
//! node expands its IN edges symmetrically. Staged edges bypass the frontier
//! bitmap: the transaction's own writes have no durable surrogate to gate on.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::csr::CsrIndex;
use crate::csr::index::LabelFilter;
use crate::overlay_delta::GraphOverlayDelta;
use crate::path_params::ShortestPathParams;

impl CsrIndex {
    /// String-keyed bidirectional BFS that merges the transaction's staged
    /// edges/tombstones. Mirrors the durable `shortest_path_dense` loop
    /// structure (alternating one forward then one backward level per depth
    /// step) so that, when the overlay contributes nothing, the result and
    /// path shape match the durable search.
    pub(crate) fn shortest_path_overlay(
        &self,
        params: ShortestPathParams<'_>,
        overlay: &GraphOverlayDelta,
    ) -> Option<Vec<String>> {
        let ShortestPathParams {
            src,
            dst,
            label_filter,
            max_depth,
            max_visited,
            frontier_bitmap,
        } = params;
        if src == dst {
            return Some(vec![src.to_string()]);
        }

        // parent maps: node -> the neighbour it was reached from. The endpoint
        // maps to itself, marking the reconstruction terminus.
        let mut fwd_parent: HashMap<String, String> = HashMap::new();
        let mut bwd_parent: HashMap<String, String> = HashMap::new();
        fwd_parent.insert(src.to_string(), src.to_string());
        bwd_parent.insert(dst.to_string(), dst.to_string());

        let mut fwd_frontier: Vec<String> = vec![src.to_string()];
        let mut bwd_frontier: Vec<String> = vec![dst.to_string()];

        let gate = PathGate::new(self, label_filter, frontier_bitmap, [src, dst]);
        let within_depth =
            |path: Vec<String>| (path.len().saturating_sub(1) <= max_depth).then_some(path);

        // Round `k` meets on a path of `2k - 1` or `2k` edges, so
        // `max_depth.div_ceil(2)` rounds reach every path within `max_depth`.
        for _round in 0..max_depth.div_ceil(2) {
            if fwd_parent.len() + bwd_parent.len() >= max_visited {
                break;
            }

            // Each level's edges are relaxed in (neighbour, frontier node)
            // name order, as the durable search relaxes them.
            let mut candidates: Vec<(String, String)> = Vec::new();
            for node in std::mem::take(&mut fwd_frontier) {
                for neighbor in self.forward_neighbors(&node, &gate, overlay) {
                    candidates.push((neighbor, node.clone()));
                }
            }
            candidates.sort();
            let mut next_fwd = Vec::new();
            for (neighbor, node) in candidates {
                if let Some(meeting) = relax(
                    &neighbor,
                    &node,
                    &mut fwd_parent,
                    &bwd_parent,
                    &mut next_fwd,
                ) {
                    return within_depth(reconstruct(&meeting, &fwd_parent, &bwd_parent));
                }
            }
            fwd_frontier = next_fwd;

            let mut candidates: Vec<(String, String)> = Vec::new();
            for node in std::mem::take(&mut bwd_frontier) {
                for neighbor in self.backward_neighbors(&node, &gate, overlay) {
                    candidates.push((neighbor, node.clone()));
                }
            }
            candidates.sort();
            let mut next_bwd = Vec::new();
            for (neighbor, node) in candidates {
                if let Some(meeting) = relax(
                    &neighbor,
                    &node,
                    &mut bwd_parent,
                    &fwd_parent,
                    &mut next_bwd,
                ) {
                    return within_depth(reconstruct(&meeting, &fwd_parent, &bwd_parent));
                }
            }
            bwd_frontier = next_bwd;

            if fwd_frontier.is_empty() && bwd_frontier.is_empty() {
                break;
            }
        }
        None
    }

    /// OUT neighbours of `node`: durable CSR out edges (skipping staged
    /// tombstones and gated durable targets) unioned with staged out edges.
    fn forward_neighbors(
        &self,
        node: &str,
        gate: &PathGate<'_>,
        overlay: &GraphOverlayDelta,
    ) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(&node_id) = self.node_to_id.get(node) {
            self.record_access(node_id);
            for (lid, dst) in self.dense_iter_out(node_id) {
                if !gate.durable_labels.keeps(lid) {
                    continue;
                }
                let dst_name = &self.id_to_node[dst as usize];
                if overlay.is_tombstoned(node, self.label_name(lid), dst_name) {
                    continue;
                }
                if !gate.admits_durable(self, dst, dst_name) {
                    continue;
                }
                out.push(dst_name.clone());
            }
        }
        out.extend(
            overlay
                .out_neighbors(node, gate.labels)
                .map(|(_, dst)| dst.to_string()),
        );
        out
    }

    /// IN neighbours of `node`: durable CSR in edges (skipping staged
    /// tombstones and gated durable sources) unioned with staged in edges.
    fn backward_neighbors(
        &self,
        node: &str,
        gate: &PathGate<'_>,
        overlay: &GraphOverlayDelta,
    ) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(&node_id) = self.node_to_id.get(node) {
            self.record_access(node_id);
            for (lid, src) in self.dense_iter_in(node_id) {
                if !gate.durable_labels.keeps(lid) {
                    continue;
                }
                let src_name = &self.id_to_node[src as usize];
                if overlay.is_tombstoned(src_name, self.label_name(lid), node) {
                    continue;
                }
                if !gate.admits_durable(self, src, src_name) {
                    continue;
                }
                out.push(src_name.clone());
            }
        }
        out.extend(
            overlay
                .in_neighbors(node, gate.labels)
                .map(|(_, src)| src.to_string()),
        );
        out
    }
}

/// Which edges and nodes one overlay path search may cross.
struct PathGate<'a> {
    /// Staged-edge label names. Empty keeps every label.
    labels: &'a [&'a str],
    /// `labels` resolved against this partition. A label it has never seen
    /// keeps no durable edge, so it never widens the filter.
    durable_labels: LabelFilter,
    frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    /// The source and destination, which the bitmap never gates.
    endpoints: [&'a str; 2],
}

impl<'a> PathGate<'a> {
    fn new(
        csr: &CsrIndex,
        label_filter: &'a [&'a str],
        frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
        endpoints: [&'a str; 2],
    ) -> Self {
        Self {
            labels: label_filter,
            durable_labels: csr.label_filter(label_filter),
            frontier_bitmap,
            endpoints,
        }
    }

    /// Staged edges bypass the bitmap: the transaction's own writes have no
    /// durable surrogate to gate on.
    fn admits_durable(&self, csr: &CsrIndex, id: u32, name: &str) -> bool {
        self.endpoints.contains(&name)
            || self.frontier_bitmap.is_none_or(|bm| {
                bm.contains(nodedb_types::Surrogate::new(csr.node_surrogate_raw(id)))
            })
    }
}

/// Record `neighbor` (reached from `parent`) in `this_parent` if unseen and
/// queue it for the next level. Returns `Some(neighbor)` when the two searches
/// have met — i.e. `neighbor` is already present in `other_parent`. Meeting
/// detection is independent of whether `neighbor` was newly inserted, matching
/// the durable dense search. The caller reconstructs the path with the fixed
/// forward/backward maps, so orientation is never ambiguous.
fn relax(
    neighbor: &str,
    parent: &str,
    this_parent: &mut HashMap<String, String>,
    other_parent: &HashMap<String, String>,
    next_frontier: &mut Vec<String>,
) -> Option<String> {
    if let Entry::Vacant(e) = this_parent.entry(neighbor.to_string()) {
        e.insert(parent.to_string());
        next_frontier.push(neighbor.to_string());
    }
    if other_parent.contains_key(neighbor) {
        return Some(neighbor.to_string());
    }
    None
}

/// Reconstruct the ordered `src -> dst` path through `meeting`. `fwd_parent`
/// roots at `src`, `bwd_parent` roots at `dst`; `meeting` is present in both
/// (a caller invariant). Mirrors the durable dense `reconstruct_path`.
fn reconstruct(
    meeting: &str,
    fwd_parent: &HashMap<String, String>,
    bwd_parent: &HashMap<String, String>,
) -> Vec<String> {
    let mut path = walk_to_root(meeting, fwd_parent);
    path.reverse();

    let suffix_root = &bwd_parent[meeting];
    if suffix_root != meeting {
        let mut cur = suffix_root.clone();
        loop {
            path.push(cur.clone());
            let parent = &bwd_parent[&cur];
            if *parent == cur {
                break;
            }
            cur = parent.clone();
        }
    }
    path
}

/// Walk the parent chain from `start` to its self-parent root, collecting each
/// node (root last). Presence is a caller invariant.
fn walk_to_root(start: &str, parents: &HashMap<String, String>) -> Vec<String> {
    let mut chain = Vec::new();
    let mut cur = start.to_string();
    loop {
        chain.push(cur.clone());
        let parent = &parents[&cur];
        if *parent == cur {
            break;
        }
        cur = parent.clone();
    }
    chain
}

#[cfg(test)]
mod tests {
    use crate::csr::CsrIndex;
    use crate::overlay_delta::GraphOverlayDelta;
    use crate::path_params::ShortestPathParams;
    use crate::test_support::test_memory;
    use crate::traversal::DEFAULT_MAX_VISITED;

    fn params<'a>(
        src: &'a str,
        dst: &'a str,
        label_filter: &'a [&'a str],
        max_depth: usize,
    ) -> ShortestPathParams<'a> {
        ShortestPathParams {
            src,
            dst,
            label_filter,
            max_depth,
            max_visited: DEFAULT_MAX_VISITED,
            frontier_bitmap: None,
        }
    }

    /// A bitmap that holds neither endpoint still admits the durable edge
    /// between them while a staged edge forces the overlay search.
    #[test]
    fn the_frontier_bitmap_never_gates_the_endpoints() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "c").unwrap();
        csr.set_node_surrogate("a", Surrogate::new(1));
        csr.set_node_surrogate("c", Surrogate::new(3));
        let mut bm = SurrogateBitmap::new();
        bm.insert(Surrogate::new(99));
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("x", "KNOWS", "y");

        let mut p = params("a", "c", &["KNOWS"], 1);
        p.frontier_bitmap = Some(&bm);
        assert_eq!(
            csr.shortest_path(p, Some(&ov)),
            Some(vec!["a".to_string(), "c".to_string()])
        );
    }

    /// A staged edge completes a path the durable graph lacks: durable A->B,
    /// staged B->C, so A->B->C must be found only with the overlay.
    #[test]
    fn staged_edge_completes_path() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "b").unwrap();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("b", "KNOWS", "c");

        let path = csr
            .shortest_path(params("a", "c", &["KNOWS"], 5), Some(&ov))
            .expect("staged edge should complete the path");
        assert_eq!(path, vec!["a", "b", "c"]);

        // Without the overlay the path does not exist.
        assert!(
            csr.shortest_path(params("a", "c", &["KNOWS"], 5), None)
                .is_none()
        );
    }

    /// A path entirely through staged-only intermediate nodes: no durable
    /// edges touch A, X, or D. String-keyed frontiers let X (no surrogate)
    /// participate as the meeting point.
    #[test]
    fn path_through_staged_only_node() {
        let csr = CsrIndex::new(test_memory());
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("a", "KNOWS", "x");
        ov.stage_edge("x", "KNOWS", "d");

        let path = csr
            .shortest_path(params("a", "d", &["KNOWS"], 5), Some(&ov))
            .expect("staged-only path should be found");
        assert_eq!(path, vec!["a", "x", "d"]);
    }

    /// A staged tombstone removes the only durable direct edge, forcing a
    /// longer detour that exists only via other durable edges.
    #[test]
    fn tombstone_forces_detour() {
        // Durable: a->d (direct), a->b, b->d. Tombstone a->d. Detour a->b->d.
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "d").unwrap();
        csr.add_edge("a", "KNOWS", "b").unwrap();
        csr.add_edge("b", "KNOWS", "d").unwrap();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_tombstone("a", "KNOWS", "d");

        let path = csr
            .shortest_path(params("a", "d", &["KNOWS"], 5), Some(&ov))
            .expect("detour path should be found");
        assert_eq!(path, vec!["a", "b", "d"]);
    }

    /// A staged tombstone that removes the only path yields None.
    #[test]
    fn tombstone_removes_only_path() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "d").unwrap();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_tombstone("a", "KNOWS", "d");

        assert!(
            csr.shortest_path(params("a", "d", &["KNOWS"], 5), Some(&ov))
                .is_none()
        );
    }

    /// Empty overlay yields the same result as the durable dense search on a
    /// small fixed graph (parity check).
    #[test]
    fn empty_overlay_matches_dense() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "b").unwrap();
        csr.add_edge("b", "KNOWS", "c").unwrap();
        csr.add_edge("c", "KNOWS", "d").unwrap();
        let ov = GraphOverlayDelta::new();

        // Empty overlay dispatches to dense; force the overlay code path by
        // calling it directly, then compare with dense.
        let dense = csr
            .shortest_path(params("a", "d", &["KNOWS"], 10), None)
            .unwrap();
        let overlaid = csr.shortest_path_overlay(params("a", "d", &["KNOWS"], 10), &ov);
        assert_eq!(overlaid, Some(dense));
    }

    /// Durable `a -FIRST-> b`, staged `b -SECOND-> d` and `b -OTHER-> d`. The
    /// set follows the staged edge under its second label only.
    #[test]
    fn a_staged_edge_under_the_second_label_of_a_set_is_followed() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "FIRST", "b").unwrap();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("b", "SECOND", "d");
        ov.stage_edge("b", "OTHER", "x");

        assert_eq!(
            csr.shortest_path(params("a", "d", &["FIRST", "SECOND"], 5), Some(&ov)),
            Some(vec!["a".to_string(), "b".to_string(), "d".to_string()])
        );
        assert!(
            csr.shortest_path(params("a", "x", &["FIRST", "SECOND"], 5), Some(&ov))
                .is_none()
        );
    }

    #[test]
    fn src_equals_dst() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "b").unwrap();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("a", "KNOWS", "x");
        let path = csr
            .shortest_path(params("a", "a", &[], 5), Some(&ov))
            .unwrap();
        assert_eq!(path, vec!["a"]);
    }

    #[test]
    fn unreachable_is_none() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "b").unwrap();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("m", "KNOWS", "n");
        assert!(
            csr.shortest_path(params("a", "n", &["KNOWS"], 5), Some(&ov))
                .is_none()
        );
    }
}
