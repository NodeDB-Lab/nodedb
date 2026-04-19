//! Variable-length path expansion and neighbor collection.

use std::collections::HashSet;

use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::Direction;

/// Hard cap on results returned from a single variable-length expansion.
/// Defends the Control Plane against pathological queries even when the
/// DSL layer's depth cap is set high.
const MAX_VARLEN_RESULTS: usize = 100_000;

/// Hard cap on live frontier size at any hop. Prevents a single wide hop
/// from blowing up intermediate allocation even when global node dedup
/// is in place (dense multigraphs, bidirectional traversal on large |V|).
const MAX_VARLEN_FRONTIER: usize = 100_000;

/// Variable-length path expansion via iterative BFS with **global** per-node
/// dedup.
///
/// Returns `(dst_node_id, path_description)` for every node reachable in
/// `min_hops..=max_hops` hops from `source`. Each destination is emitted
/// at most once — along the first (shortest) path BFS finds. This is the
/// openCypher semantics for `(a)-[*min..max]->(b)` and the only safe
/// contract on dense graphs: without global dedup, result size grows as
/// `b^max_hops` and the query allocates itself out of the process.
///
/// Path-string construction is gated on `want_path`. Callers that don't
/// bind the edge variable (i.e. `MATCH (a)-[*1..k]->(b)` with no
/// `-[e*1..k]-`) pass `false` and skip all `format!`/`String` work in
/// the hot loop.
///
/// Return: `VarLenExpansion { results, truncated }`. When `truncated` is
/// `true` one of the hard caps (`MAX_VARLEN_RESULTS`, `MAX_VARLEN_FRONTIER`)
/// fired and the result set is incomplete — the caller MUST surface this
/// to the client (as `partial = true` on the response envelope, or as an
/// explicit `truncated` row in response metadata) so silent partial
/// results are impossible.
pub(super) struct VarLenExpansion {
    pub results: Vec<(u32, String)>,
    pub truncated: bool,
}

pub(super) fn expand_variable_length(
    csr: &CsrIndex,
    source: u32,
    label_filter: Option<&str>,
    direction: Direction,
    min_hops: usize,
    max_hops: usize,
    want_path: bool,
) -> VarLenExpansion {
    let mut results: Vec<(u32, String)> = Vec::new();
    if max_hops == 0 {
        if min_hops == 0 {
            let src_name = if want_path {
                csr.node_name_raw(source).to_string()
            } else {
                String::new()
            };
            results.push((source, src_name));
        }
        return VarLenExpansion {
            results,
            truncated: false,
        };
    }

    let src_name = if want_path {
        csr.node_name_raw(source).to_string()
    } else {
        String::new()
    };

    // Global visited set — each dst id is emitted and expanded at most once.
    let mut visited: HashSet<u32> = HashSet::new();
    visited.insert(source);

    // `*0..k` includes the source at depth 0.
    if min_hops == 0 {
        results.push((source, src_name.clone()));
    }

    let mut frontier: Vec<(u32, String)> = vec![(source, src_name)];
    let mut truncated = false;

    'outer: for depth in 1..=max_hops {
        if frontier.is_empty() {
            break;
        }

        let mut next_frontier: Vec<(u32, String)> = Vec::new();

        for (node, path) in &frontier {
            let neighbors = collect_neighbors(csr, *node, label_filter, direction);
            for (_, dst) in neighbors {
                if !visited.insert(dst) {
                    continue;
                }

                let new_path = if want_path {
                    let dst_name = csr.node_name_raw(dst).to_string();
                    format!("{path}->{dst_name}")
                } else {
                    String::new()
                };

                if depth >= min_hops {
                    results.push((dst, new_path.clone()));
                    if results.len() >= MAX_VARLEN_RESULTS {
                        truncated = true;
                        break 'outer;
                    }
                }

                if depth < max_hops {
                    next_frontier.push((dst, new_path));
                    if next_frontier.len() >= MAX_VARLEN_FRONTIER {
                        truncated = true;
                        break 'outer;
                    }
                }
            }
        }

        frontier = next_frontier;
    }

    VarLenExpansion { results, truncated }
}

/// Collect neighbor (label_id, node_id) pairs from CSR.
pub(super) fn collect_neighbors(
    csr: &CsrIndex,
    node: u32,
    label_filter: Option<&str>,
    direction: Direction,
) -> Vec<(u32, u32)> {
    let mut neighbors = Vec::new();
    match direction {
        Direction::Out => {
            for (lid, dst) in csr.iter_out_edges_raw(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, dst));
                }
            }
        }
        Direction::In => {
            for (lid, src) in csr.iter_in_edges_raw(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, src));
                }
            }
        }
        Direction::Both => {
            for (lid, dst) in csr.iter_out_edges_raw(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, dst));
                }
            }
            for (lid, src) in csr.iter_in_edges_raw(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, src));
                }
            }
        }
    }
    neighbors
}

fn csr_label_matches(csr: &CsrIndex, label_id: u32, filter: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(f) => csr.label_name(label_id) == f,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::csr::CsrIndex;
    use crate::engine::graph::edge_store::Direction;

    /// Spec: variable-length expansion MUST apply global per-node dedup.
    ///
    /// On a densely connected graph the number of paths of length ≤ d grows
    /// as b^d, but the number of distinct (dst, min-path) pairs is bounded
    /// by |V| × (d - min + 1). The fix must enforce that bound; without it,
    /// a graph with branching factor b = 6 and max_hops = 8 allocates 6^8 =
    /// 1.6M paths, which is a DoS vector.
    ///
    /// Regression guard: result count must stay sublinear in b^max_hops,
    /// with a hard cap proportional to |V| × (max_hops - min_hops + 1).
    #[test]
    fn variable_length_expansion_dedups_nodes_across_paths() {
        // Build a near-complete directed graph on 6 nodes (branching 5 per
        // node, 30 edges). With max_hops = 8 and no dedup the BFS explores
        // 5^8 = 390,625 distinct paths. With dedup it explores ≤ 6 nodes
        // per depth level, i.e. ≤ 48 results over 8 hops.
        let mut csr = CsrIndex::new();
        let nodes = ["a", "b", "c", "d", "e", "f"];
        for &src in &nodes {
            for &dst in &nodes {
                if src != dst {
                    csr.add_edge(src, "l", dst).unwrap();
                }
            }
        }

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("a").unwrap(),
            Some("l"),
            Direction::Out,
            1,
            8,
            false,
        );
        let results = expansion.results;

        // Spec: distinct destinations are bounded by (|V| - 1) = 5.
        let distinct_dsts: std::collections::HashSet<u32> =
            results.iter().map(|(d, _)| *d).collect();
        assert!(
            distinct_dsts.len() <= nodes.len(),
            "distinct dst count must be <= |V| ({}); got {}",
            nodes.len(),
            distinct_dsts.len()
        );

        // Regression guard against exponential fan-out: the total result
        // count must not approach b^max_hops. Cap at |V| × max_hops = 48.
        // Current buggy code returns hundreds of thousands of rows.
        assert!(
            results.len() <= nodes.len() * 8,
            "variable-length expansion must not allocate b^d paths; \
             got {} results on a 6-node graph with max_hops=8 \
             (expected ≤ {})",
            results.len(),
            nodes.len() * 8
        );
    }

    /// Spec: `*0..k` is openCypher-style "match the source itself plus
    /// paths up to length k". At depth 0 the source node must be in the
    /// result set. The current BFS starts `depth` at 1 and never emits
    /// the source even when `min_hops == 0`.
    #[test]
    fn variable_length_expansion_includes_source_at_zero_hops() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "l", "b").unwrap();
        csr.add_edge("b", "l", "c").unwrap();

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("a").unwrap(),
            Some("l"),
            Direction::Out,
            0,
            2,
            false,
        );
        let results = expansion.results;

        let dsts: std::collections::HashSet<u32> = results.iter().map(|(d, _)| *d).collect();
        assert!(
            dsts.contains(&csr.node_id_raw("a").unwrap()),
            "*0..k must include the source node at depth 0; got dsts {dsts:?}"
        );
    }

    /// Spec: `*k..k` (exact length) returns only destinations reachable
    /// in exactly k hops — not the union of 1..=k. The current BFS does
    /// gate emission with `if depth >= min_hops`, but the expansion must
    /// remain correct once global dedup prunes shorter paths.
    #[test]
    fn variable_length_expansion_exact_length_returns_only_that_depth() {
        let mut csr = CsrIndex::new();
        // Chain a → b → c → d. At exactly 2 hops from `a` only `c` is
        // reachable, not `b` (1 hop) or `d` (3 hops).
        csr.add_edge("a", "l", "b").unwrap();
        csr.add_edge("b", "l", "c").unwrap();
        csr.add_edge("c", "l", "d").unwrap();

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("a").unwrap(),
            Some("l"),
            Direction::Out,
            2,
            2,
            false,
        );
        let results = expansion.results;

        let dsts: std::collections::HashSet<u32> = results.iter().map(|(d, _)| *d).collect();
        let c = csr.node_id_raw("c").unwrap();
        let expected: std::collections::HashSet<u32> = [c].into_iter().collect();
        assert_eq!(
            dsts, expected,
            "*2..2 must return exactly the depth-2 reachable set {{c}}; got {dsts:?}"
        );
    }

    /// Spec: even with global node dedup in place, a single hop must
    /// not allow the live frontier to grow unboundedly. A pathological
    /// graph with many distinct nodes all reachable from the source in
    /// one hop should respect a per-hop frontier cap so subsequent hops
    /// cannot snowball.
    ///
    /// Regression guard: on a star with `N` leaves and `max_hops` large,
    /// the result set is bounded by `N`; a buggy no-cap implementation
    /// that forgets to cap the per-hop frontier under dedup can still
    /// allocate O(N × max_hops) in intermediate state. We assert result
    /// size is bounded.
    #[test]
    fn variable_length_expansion_caps_frontier_per_hop() {
        let mut csr = CsrIndex::new();
        const LEAVES: usize = 5_000;
        for i in 0..LEAVES {
            csr.add_edge("root", "l", &format!("leaf_{i}")).unwrap();
        }

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("root").unwrap(),
            Some("l"),
            Direction::Out,
            1,
            5,
            false,
        );
        let results = expansion.results;

        // With global dedup every leaf appears exactly once across the
        // whole traversal — subsequent hops have no outgoing edges.
        assert!(
            results.len() <= LEAVES,
            "star with {LEAVES} leaves must return at most {LEAVES} results; \
             got {}",
            results.len()
        );
    }
}
