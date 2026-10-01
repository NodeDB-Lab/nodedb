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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use crate::csr::CsrIndex;
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

        let label_ids: HashSet<u32> = label_filter
            .iter()
            .filter_map(|label| self.label_id(label))
            .collect();
        let label_filter: HashSet<&str> = label_filter.iter().copied().collect();

        // Alternating complete BFS levels makes the first meeting shortest.
        for _depth in 0..max_depth {
            if fwd_parent.len() + bwd_parent.len() >= max_visited {
                break;
            }

            let mut next_fwd = Vec::new();
            for node in std::mem::take(&mut fwd_frontier) {
                let neighbors = self.forward_neighbors(
                    &node,
                    &label_ids,
                    &label_filter,
                    frontier_bitmap,
                    overlay,
                );
                for neighbor in neighbors {
                    if let Some(meeting) = relax(
                        &neighbor,
                        &node,
                        &mut fwd_parent,
                        &bwd_parent,
                        &mut next_fwd,
                    ) {
                        let path = reconstruct(&meeting, &fwd_parent, &bwd_parent);
                        return (path.len().saturating_sub(1) <= max_depth).then_some(path);
                    }
                }
            }
            fwd_frontier = next_fwd;

            let mut next_bwd = Vec::new();
            for node in std::mem::take(&mut bwd_frontier) {
                let neighbors = self.backward_neighbors(
                    &node,
                    &label_ids,
                    &label_filter,
                    frontier_bitmap,
                    overlay,
                );
                for neighbor in neighbors {
                    if let Some(meeting) = relax(
                        &neighbor,
                        &node,
                        &mut bwd_parent,
                        &fwd_parent,
                        &mut next_bwd,
                    ) {
                        let path = reconstruct(&meeting, &fwd_parent, &bwd_parent);
                        return (path.len().saturating_sub(1) <= max_depth).then_some(path);
                    }
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
    /// tombstones and bitmap-excluded durable targets) unioned with staged
    /// out edges.
    fn forward_neighbors(
        &self,
        node: &str,
        label_ids: &HashSet<u32>,
        label_filter: &HashSet<&str>,
        frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
        overlay: &GraphOverlayDelta,
    ) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(&node_id) = self.node_to_id.get(node) {
            self.record_access(node_id);
            for (lid, dst) in self.dense_iter_out(node_id) {
                if !label_filter.is_empty() && !label_ids.contains(&lid) {
                    continue;
                }
                let dst_name = &self.id_to_node[dst as usize];
                if overlay.is_tombstoned(node, self.label_name(lid), dst_name) {
                    continue;
                }
                if !frontier_bitmap.is_none_or(|bm| {
                    bm.contains(nodedb_types::Surrogate::new(self.node_surrogate_raw(dst)))
                }) {
                    continue;
                }
                out.push(dst_name.clone());
            }
        }
        for (label, dst) in overlay.out_neighbors(node, None) {
            if !label_filter.is_empty() && !label_filter.contains(&label) {
                continue;
            }
            out.push(dst.to_string());
        }
        out
    }

    /// IN neighbours of `node`: durable CSR in edges (skipping staged
    /// tombstones and bitmap-excluded durable sources) unioned with staged
    /// in edges.
    fn backward_neighbors(
        &self,
        node: &str,
        label_ids: &HashSet<u32>,
        label_filter: &HashSet<&str>,
        frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
        overlay: &GraphOverlayDelta,
    ) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(&node_id) = self.node_to_id.get(node) {
            self.record_access(node_id);
            for (lid, src) in self.dense_iter_in(node_id) {
                if !label_filter.is_empty() && !label_ids.contains(&lid) {
                    continue;
                }
                let src_name = &self.id_to_node[src as usize];
                if overlay.is_tombstoned(src_name, self.label_name(lid), node) {
                    continue;
                }
                if !frontier_bitmap.is_none_or(|bm| {
                    bm.contains(nodedb_types::Surrogate::new(self.node_surrogate_raw(src)))
                }) {
                    continue;
                }
                out.push(src_name.clone());
            }
        }
        for (label, src) in overlay.in_neighbors(node, None) {
            if !label_filter.is_empty() && !label_filter.contains(&label) {
                continue;
            }
            out.push(src.to_string());
        }
        out
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
