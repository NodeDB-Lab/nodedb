// SPDX-License-Identifier: BUSL-1.1

//! Variable-length path expansion and neighbor collection.

use std::collections::HashSet;

use super::varlen_named::{self, NameOrId};
use crate::engine::graph::csr::{CsrIndex, GraphOverlayDelta};
use crate::engine::graph::edge_store::Direction;

/// Hard cap on results returned from a single variable-length expansion.
/// Defends the Control Plane against pathological queries even when the
/// DSL layer's depth cap is set high.
pub(super) const MAX_VARLEN_RESULTS: usize = 100_000;

/// Hard cap on live frontier size at any hop. Prevents a single wide hop
/// from blowing up intermediate allocation even when global node dedup
/// is in place (dense multigraphs, bidirectional traversal on large |V|).
pub(super) const MAX_VARLEN_FRONTIER: usize = 100_000;

/// Tunable caps for a single variable-length expansion.
///
/// Production constructs this from node `GraphTuning` via
/// [`VarLenCaps::from_graph_tuning`], whose fields default to the historical
/// `100_000` hard caps verbatim — so an operator who sets nothing gets
/// byte-identical behaviour. [`VarLenCaps::default`] preserves the same
/// `100_000` ceilings for callers (and tests) that do not thread tuning. The
/// caps are a struct field rather than a module const so an operator (via the
/// `[tuning.graph]` config knobs) — and tests — can drive truncation
/// deterministically on small graphs without mutating any compile-time ceiling.
#[derive(Debug, Clone, Copy)]
pub struct VarLenCaps {
    /// Max emitted results before truncation fires.
    pub max_results: usize,
    /// Max live frontier (per-hop) before truncation fires.
    pub max_frontier: usize,
}

impl VarLenCaps {
    /// Build the caps from node graph tuning. The two `varlen_*` fields default
    /// to `100_000`, so the production path is identical to the prior
    /// hardcoded ceilings unless an operator overrides them in config.
    pub fn from_graph_tuning(tuning: &nodedb_types::config::tuning::GraphTuning) -> Self {
        Self {
            max_results: tuning.varlen_max_results,
            max_frontier: tuning.varlen_max_frontier,
        }
    }
}

impl Default for VarLenCaps {
    fn default() -> Self {
        Self {
            max_results: MAX_VARLEN_RESULTS,
            max_frontier: MAX_VARLEN_FRONTIER,
        }
    }
}

/// Where a capped expansion should resume from on the next round.
///
/// Carries the **surviving un-expanded frontier** at a single hop boundary
/// (`frontier`, all reached at `depth - 1` and awaiting expansion AT `depth`)
/// so a follow-up call can continue the BFS from exactly that point. There is
/// deliberately **no `visited` set**: termination relies on the `min..max`
/// depth bound plus downstream coordinator row-dedup, so re-running a node
/// already emitted on the first pass yields a duplicate that is collapsed
/// later — never a skipped or mis-depthed row.
#[derive(Debug, Clone)]
pub(super) struct VarLenCursor {
    /// Un-expanded frontier entries to resume the BFS from, each a
    /// `(node_name, path_so_far)` pair. The node is keyed by its GLOBAL name,
    /// not the CSR-local dense id used during live traversal: local ids are
    /// per-core and overlap across cores, so a captured cursor must be
    /// core-agnostic to be safely fanned to all cores on resume. The path
    /// string carries the accumulated route from the original source (empty
    /// when the edge variable is unbound), so resumed `RETURN p` rows render
    /// full paths instead of bare node names.
    pub frontier: Vec<(String, String)>,
    /// Hop depth at which `frontier` is to be expanded (`resume_depth`).
    pub depth: usize,
}

/// Result of a variable-length expansion.
///
/// `cursor` is `Some` iff one of the hard caps (`max_results`,
/// `max_frontier`) fired: the result set for this round is incomplete and the
/// cursor records the live frontier/depth needed to resume next round. `None`
/// means the expansion ran to its natural completion.
///
/// A single call populates EITHER `results` (the durable u32-keyed path) OR
/// `named_results` (the overlay name-keyed path), never both — the other stays
/// empty. `boundary` lists frontier nodes with zero local merged out-degree
/// (`(node_name, path_so_far, resume_depth)`) whose remaining edges may be
/// homed on another shard; the caller emits a cross-shard continuation for each
/// (gated on the remote-node predicate) so a boundary edge is reached without
/// depending on a result cap firing.
pub(super) struct VarLenExpansion {
    /// Durable destinations `(dst_node_id, path)` — the u32-keyed path.
    pub results: Vec<(u32, String)>,
    /// Overlay destinations `(NameOrId, path)` — the name-keyed path. A durable
    /// destination keeps its id; a staged-only destination is carried by name.
    pub named_results: Vec<(NameOrId, String)>,
    pub cursor: Option<VarLenCursor>,
    /// Frontier nodes dropped for zero local merged out-degree, as
    /// `(node_name, path_so_far, resume_depth)`. `resume_depth` is the hop depth
    /// at which the node would have been expanded, i.e. the depth to resume its
    /// continuation from on the owning shard.
    pub boundary: Vec<(String, String, usize)>,
}

/// Pattern-shape parameters for a variable-length BFS expansion.
///
/// Bundles the immutable per-query shape (label filter, direction, depth
/// bounds, path-string flag) into a single borrowed struct so `run_bfs`,
/// `expand_variable_length`, and `resume_variable_length` each stay within
/// the 7-argument clippy limit without needing `#[allow]`.
pub(super) struct VarLenPattern<'a> {
    pub label_filter: Option<&'a str>,
    pub direction: Direction,
    pub min_hops: usize,
    pub max_hops: usize,
    /// `true` iff the edge variable is bound in the query (e.g. `[e*1..k]`).
    /// When `false` all `format!`/`String` path work in the hot loop is skipped.
    pub want_path: bool,
    /// Collection scoping for edge traversal (resolved from the query's
    /// `IN '<collection>'` clause). See [`CollectionFilter`].
    pub collection_filter: CollectionFilter,
}

/// Collection scoping applied to graph edge traversal.
///
/// A CSR partition holds every collection's edges under one shared node space;
/// this selects which edges a collection-scoped read (`MATCH ... IN '<c>'`) may
/// traverse, so a query in collection A never sees collection B's edges.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum CollectionFilter {
    /// No `IN '<collection>'` clause — traverse edges of every collection
    /// (tenant-wide graph). This preserves the collection-less behavior.
    #[default]
    Unscoped,
    /// Traverse only edges tagged with this collection id.
    Only(u32),
    /// An `IN '<collection>'` clause naming a collection that has no edges in
    /// this partition — match nothing (never falls back to unscoped, which
    /// would re-introduce the cross-collection leak).
    Empty,
}

/// Resolve a query's `IN '<collection>'` clause to a [`CollectionFilter`]
/// against the partition's collection interning.
pub(super) fn resolve_collection_filter(
    collection: Option<&str>,
    csr: &CsrIndex,
) -> CollectionFilter {
    match collection {
        None => CollectionFilter::Unscoped,
        Some(c) => match csr.collection_id(c) {
            Some(id) => CollectionFilter::Only(id),
            None => CollectionFilter::Empty,
        },
    }
}

/// Return `csr.node_name_raw(node).to_string()` when `want_path`, else `""`.
///
/// Centralises the three identical `if want_path { … } else { String::new() }`
/// sites in the BFS hot paths.
#[inline]
fn node_name_or_empty(csr: &CsrIndex, node: u32, want_path: bool) -> String {
    if want_path {
        csr.node_name_raw(node).to_string()
    } else {
        String::new()
    }
}

/// Variable-length path expansion via iterative BFS with **global** per-node
/// dedup — the from-scratch entry point.
///
/// Returns `(dst_node_id, path_description)` for every node reachable in
/// `min_hops..=max_hops` hops from `source`. Each destination is emitted
/// at most once — along the first (shortest) path BFS finds. This is the
/// openCypher semantics for `(a)-[*min..max]->(b)` and the only safe
/// contract on dense graphs: without global dedup, result size grows as
/// `b^max_hops` and the query allocates itself out of the process.
///
/// Path-string construction is gated on `pattern.want_path`. Callers that
/// don't bind the edge variable (i.e. `MATCH (a)-[*1..k]->(b)` with no
/// `-[e*1..k]-`) pass `false` and skip all `format!`/`String` work in
/// the hot loop.
///
/// On a cap hit the returned [`VarLenExpansion::cursor`] is `Some`; callers
/// MUST surface that (as `partial = true` on the response envelope) and may
/// resume via [`resume_variable_length`] so silent partial results are
/// impossible.
pub(super) fn expand_variable_length(
    csr: &CsrIndex,
    source: u32,
    pattern: &VarLenPattern<'_>,
    caps: VarLenCaps,
    overlay: Option<&GraphOverlayDelta>,
) -> VarLenExpansion {
    let mut results: Vec<(u32, String)> = Vec::new();
    if pattern.max_hops == 0 {
        if pattern.min_hops == 0 {
            results.push((source, node_name_or_empty(csr, source, pattern.want_path)));
        }
        return VarLenExpansion {
            results,
            named_results: Vec::new(),
            cursor: None,
            boundary: Vec::new(),
        };
    }

    // Inside a transaction with staged edges, run the name-keyed merge BFS so
    // staged edges/tombstones and staged-only nodes are observed. An absent or
    // empty overlay takes the durable u32-keyed path below, byte-identical to
    // committed-CSR-only execution.
    if let Some(ov) = overlay
        && !ov.is_empty()
    {
        return varlen_named::expand_named(csr, source, pattern, caps, ov);
    }

    let src_name = node_name_or_empty(csr, source, pattern.want_path);

    // Global visited set — each dst id is emitted and expanded at most once.
    let mut visited: HashSet<u32> = HashSet::new();
    visited.insert(source);

    // `*0..k` includes the source at depth 0.
    if pattern.min_hops == 0 {
        results.push((source, src_name.clone()));
    }

    let frontier: Vec<(u32, String)> = vec![(source, src_name)];
    run_bfs(csr, results, visited, frontier, 1, pattern, caps)
}

/// Resume a previously-capped variable-length expansion from a [`VarLenCursor`].
///
/// `cursor.frontier` are `(node_name, path_so_far)` pairs reached at
/// `cursor.depth - 1` on the prior round and awaiting expansion AT
/// `cursor.depth`. Each name is resolved against THIS core's CSR via
/// `node_id_raw`: a name the CSR owns seeds the BFS at its local id; a name the
/// CSR does NOT own is silently skipped (not an error). This name-keyed
/// self-scoping is what lets the resume plan be broadcast to all cores — only
/// the owning core resolves the frontier and resumes; every other core skips
/// every name and yields an empty expansion. The carried path prefix from the
/// original source is preserved, so resumed `RETURN p` rows render the full
/// path, not just the resume node's own name. The BFS continues with a
/// **fresh** `visited` set (seeded only with the resolved resume frontier ids so
/// a node is not expanded twice within this round): per the cross-shard contract,
/// dedup of rows already emitted on the prior round is the coordinator's job,
/// never the executor's. The `min_hops..=max_hops` bound is honored across the
/// resume boundary because the loop continues at `cursor.depth`, so a node
/// reached at depth `d` here behaves exactly as a node reached at depth `d` in
/// one pass.
//
// Consumed by `execute_varlen_resume` (the cross-plane resume path).
pub(super) fn resume_variable_length(
    csr: &CsrIndex,
    cursor: &VarLenCursor,
    pattern: &VarLenPattern<'_>,
    caps: VarLenCaps,
    overlay: Option<&GraphOverlayDelta>,
) -> VarLenExpansion {
    // Inside a transaction with staged edges, resume via the name-keyed merge
    // BFS so a staged-only frontier node (or staged tail) is walked. An absent
    // or empty overlay takes the durable resume path below.
    if let Some(ov) = overlay
        && !ov.is_empty()
    {
        return varlen_named::resume_named(csr, cursor, pattern, caps, ov);
    }

    // Resolve each frontier NAME against this core's CSR. A name the CSR owns
    // becomes a local id and seeds the BFS; a name it does NOT own is skipped —
    // this is what lets the resume plan be fanned to all cores and self-scope to
    // the owning one. Seed visited with the resolved ids so an intra-round
    // revisit of a resume node is suppressed; rows already emitted in the PRIOR
    // round are intentionally NOT tracked (coordinator dedups across rounds).
    // The carried `path_so_far` strings are the accumulated route from the
    // original source, so resumed paths stay continuous instead of being rebuilt
    // from the resume node's own name.
    let mut visited: HashSet<u32> = HashSet::new();
    let mut frontier: Vec<(u32, String)> = Vec::with_capacity(cursor.frontier.len());
    for (name, path) in &cursor.frontier {
        let Some(local_id) = csr.node_id_raw(name) else {
            continue;
        };
        if !visited.insert(local_id) {
            continue;
        }
        frontier.push((local_id, path.clone()));
    }

    run_bfs(
        csr,
        Vec::new(),
        visited,
        frontier,
        cursor.depth,
        pattern,
        caps,
    )
}

/// Shared BFS driver for both the from-scratch and resume paths.
///
/// Expands `frontier` hop-by-hop from `start_depth` through `pattern.max_hops`,
/// emitting destinations at `depth >= pattern.min_hops`. Caps are honored at
/// **hop boundaries**: a depth level is processed to completion, then the cap is
/// checked. This keeps the resume cursor depth-exact — the surviving
/// `next_frontier` is a single set all reached at the same depth, awaiting
/// expansion at `depth + 1`.
fn run_bfs(
    csr: &CsrIndex,
    mut results: Vec<(u32, String)>,
    mut visited: HashSet<u32>,
    mut frontier: Vec<(u32, String)>,
    start_depth: usize,
    pattern: &VarLenPattern<'_>,
    caps: VarLenCaps,
) -> VarLenExpansion {
    let mut cursor: Option<VarLenCursor> = None;
    let mut boundary: Vec<(String, String, usize)> = Vec::new();

    for depth in start_depth..=pattern.max_hops {
        if frontier.is_empty() {
            break;
        }

        let mut next_frontier: Vec<(u32, String)> = Vec::new();

        for (node, path) in &frontier {
            let neighbors = collect_neighbors(
                csr,
                *node,
                pattern.label_filter,
                pattern.direction,
                pattern.collection_filter,
            );
            // Zero local out-degree: this node's remaining edges (if any) may be
            // homed on another shard. Capture it (keyed by GLOBAL name) so the
            // caller can ship a cross-shard continuation instead of dropping the
            // partial match — a boundary edge is then reached without waiting for
            // a result cap to fire.
            if neighbors.is_empty() {
                boundary.push((csr.node_name_raw(*node).to_string(), path.clone(), depth));
                continue;
            }
            for (_, dst) in neighbors {
                if !visited.insert(dst) {
                    continue;
                }

                let new_path = if pattern.want_path {
                    let dst_name = csr.node_name_raw(dst).to_string();
                    format!("{path}->{dst_name}")
                } else {
                    String::new()
                };

                if depth >= pattern.min_hops {
                    results.push((dst, new_path.clone()));
                }

                if depth < pattern.max_hops {
                    next_frontier.push((dst, new_path));
                }
            }
        }

        // Honor caps at the hop boundary so the resume cursor is depth-exact:
        // `next_frontier` is a single set all reached at `depth`, awaiting
        // expansion at `depth + 1`. A node here behaves on resume exactly as
        // if reached at `depth` in one uninterrupted pass.
        let cap_hit = results.len() >= caps.max_results || next_frontier.len() >= caps.max_frontier;
        if cap_hit {
            if depth < pattern.max_hops && !next_frontier.is_empty() {
                // Convert the live `(local_id, path_so_far)` frontier to a
                // `(node_name, path_so_far)` cursor. The live BFS keys on dense
                // CSR-local ids for traversal speed, but the captured cursor must
                // key on GLOBAL names so it is core-agnostic and safe to fan to
                // all cores on resume (local ids overlap across cores). The
                // carried path strings keep path-returning queries continuous
                // across the cap.
                let named_frontier: Vec<(String, String)> = next_frontier
                    .into_iter()
                    .map(|(local_id, path)| (csr.node_name_raw(local_id).to_string(), path))
                    .collect();
                cursor = Some(VarLenCursor {
                    frontier: named_frontier,
                    depth: depth + 1,
                });
            }
            break;
        }

        frontier = next_frontier;
    }

    VarLenExpansion {
        results,
        named_results: Vec::new(),
        cursor,
        boundary,
    }
}

/// Collect neighbor (label_id, node_id) pairs from CSR. A `Both` read
/// returns each physical edge once, a self-loop included.
pub(super) fn collect_neighbors(
    csr: &CsrIndex,
    node: u32,
    label_filter: Option<&str>,
    direction: Direction,
    collection_filter: CollectionFilter,
) -> Vec<(u32, u32)> {
    let mut neighbors = Vec::new();
    // A collection clause naming an unknown collection matches nothing —
    // never fall through to the unscoped iterators.
    if collection_filter == CollectionFilter::Empty {
        return neighbors;
    }
    let keep = |lid: u32| label_filter.is_none() || csr_label_matches(csr, lid, label_filter);
    if matches!(direction, Direction::Out | Direction::Both) {
        match collection_filter {
            CollectionFilter::Only(cid) => {
                for (lid, dst) in csr.iter_out_edges_raw_in(node, cid) {
                    if keep(lid) {
                        neighbors.push((lid, dst));
                    }
                }
            }
            _ => {
                for (lid, dst) in csr.iter_out_edges_raw(node) {
                    if keep(lid) {
                        neighbors.push((lid, dst));
                    }
                }
            }
        }
    }
    if matches!(direction, Direction::In | Direction::Both) {
        match collection_filter {
            CollectionFilter::Only(cid) => {
                for (lid, src) in csr.iter_in_edges_raw_in(node, cid) {
                    if keep(lid) && direction.in_pass_keeps(&src, &node) {
                        neighbors.push((lid, src));
                    }
                }
            }
            _ => {
                for (lid, src) in csr.iter_in_edges_raw(node) {
                    if keep(lid) && direction.in_pass_keeps(&src, &node) {
                        neighbors.push((lid, src));
                    }
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
    use crate::engine::graph::test_support::test_scoped_memory;

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
        let mut csr = CsrIndex::new(test_scoped_memory());
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
            &VarLenPattern {
                label_filter: Some("l"),
                direction: Direction::Out,
                min_hops: 1,
                max_hops: 8,
                want_path: false,
                collection_filter: CollectionFilter::Unscoped,
            },
            VarLenCaps::default(),
            None,
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
        let mut csr = CsrIndex::new(test_scoped_memory());
        csr.add_edge("a", "l", "b").unwrap();
        csr.add_edge("b", "l", "c").unwrap();

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("a").unwrap(),
            &VarLenPattern {
                label_filter: Some("l"),
                direction: Direction::Out,
                min_hops: 0,
                max_hops: 2,
                want_path: false,
                collection_filter: CollectionFilter::Unscoped,
            },
            VarLenCaps::default(),
            None,
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
        let mut csr = CsrIndex::new(test_scoped_memory());
        // Chain a → b → c → d. At exactly 2 hops from `a` only `c` is
        // reachable, not `b` (1 hop) or `d` (3 hops).
        csr.add_edge("a", "l", "b").unwrap();
        csr.add_edge("b", "l", "c").unwrap();
        csr.add_edge("c", "l", "d").unwrap();

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("a").unwrap(),
            &VarLenPattern {
                label_filter: Some("l"),
                direction: Direction::Out,
                min_hops: 2,
                max_hops: 2,
                want_path: false,
                collection_filter: CollectionFilter::Unscoped,
            },
            VarLenCaps::default(),
            None,
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
        let mut csr = CsrIndex::new(test_scoped_memory());
        const LEAVES: usize = 5_000;
        for i in 0..LEAVES {
            csr.add_edge("root", "l", &format!("leaf_{i}")).unwrap();
        }

        let expansion = expand_variable_length(
            &csr,
            csr.node_id_raw("root").unwrap(),
            &VarLenPattern {
                label_filter: Some("l"),
                direction: Direction::Out,
                min_hops: 1,
                max_hops: 5,
                want_path: false,
                collection_filter: CollectionFilter::Unscoped,
            },
            VarLenCaps::default(),
            None,
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

    /// Build a simple directed chain `n0 -l-> n1 -l-> ... -l-> n{len}`.
    fn make_chain(len: usize) -> CsrIndex {
        let mut csr = CsrIndex::new(test_scoped_memory());
        for i in 0..len {
            csr.add_edge(&format!("n{i}"), "l", &format!("n{}", i + 1))
                .unwrap();
        }
        csr
    }

    fn dst_set(results: &[(u32, String)]) -> std::collections::HashSet<u32> {
        results.iter().map(|(d, _)| *d).collect()
    }

    /// Spec: a capped expansion resumed from its `VarLenCursor` produces the
    /// SAME destination set as a single uncapped pass. Exact set equality of
    /// (first-pass ∪ resumed) vs (uncapped). The cap is injected via
    /// `VarLenCaps`, NOT by lowering the 100k production const.
    #[test]
    fn varlen_resume_union_equals_uncapped_pass() {
        // Chain n0 -> n1 -> ... -> n6 (6 edges). `*1..6` from n0 reaches
        // {n1..n6} in one pass. A low results cap forces truncation mid-way.
        let csr = make_chain(6);
        let src = csr.node_id_raw("n0").unwrap();

        let pat = VarLenPattern {
            label_filter: Some("l"),
            direction: Direction::Out,
            min_hops: 1,
            max_hops: 6,
            want_path: false,
            collection_filter: CollectionFilter::Unscoped,
        };
        let uncapped = expand_variable_length(&csr, src, &pat, VarLenCaps::default(), None);
        assert!(uncapped.cursor.is_none(), "uncapped pass must not truncate");
        let full = dst_set(&uncapped.results);

        // Inject a low results cap so truncation fires at a hop boundary.
        let caps = VarLenCaps {
            max_results: 2,
            max_frontier: usize::MAX,
        };
        let first = expand_variable_length(&csr, src, &pat, caps, None);
        let cursor = first
            .cursor
            .clone()
            .expect("low cap must produce a resume cursor");
        assert!(cursor.depth >= 2, "resume depth advances past the cap");

        // Resume — possibly more than once — until the BFS completes.
        let mut union: std::collections::HashSet<u32> = dst_set(&first.results);
        let mut next = Some(cursor);
        while let Some(c) = next {
            let resumed = resume_variable_length(&csr, &c, &pat, caps, None);
            union.extend(dst_set(&resumed.results));
            next = resumed.cursor;
        }

        assert_eq!(
            union, full,
            "first-pass ∪ resumed must equal the uncapped destination set"
        );
    }

    /// Spec: under the cap, an expansion completes in one pass with `cursor ==
    /// None` and the same results as before the resume machinery existed.
    #[test]
    fn varlen_no_truncation_path_unchanged() {
        let csr = make_chain(3); // n0 -> n1 -> n2 -> n3
        let src = csr.node_id_raw("n0").unwrap();
        let expansion = expand_variable_length(
            &csr,
            src,
            &VarLenPattern {
                label_filter: Some("l"),
                direction: Direction::Out,
                min_hops: 1,
                max_hops: 3,
                want_path: false,
                collection_filter: CollectionFilter::Unscoped,
            },
            VarLenCaps::default(),
            None,
        );
        assert!(
            expansion.cursor.is_none(),
            "well under the cap → no truncation cursor"
        );
        let dsts = dst_set(&expansion.results);
        let expected: std::collections::HashSet<u32> = ["n1", "n2", "n3"]
            .iter()
            .map(|n| csr.node_id_raw(n).unwrap())
            .collect();
        assert_eq!(dsts, expected, "results identical to a normal pass");
    }

    /// Spec: the `min..max` depth bound is honored ACROSS the resume boundary.
    /// A `*1..2` expansion truncated at depth 1 and resumed at depth 2 must
    /// NOT emit depth-3 nodes.
    #[test]
    fn varlen_resume_honors_depth_bound() {
        let csr = make_chain(3); // n0 -> n1 -> n2 -> n3
        let src = csr.node_id_raw("n0").unwrap();
        let n3 = csr.node_id_raw("n3").unwrap();

        // cap=1 truncates after emitting n1 at depth 1; max_hops=2.
        let caps = VarLenCaps {
            max_results: 1,
            max_frontier: usize::MAX,
        };
        let pat = VarLenPattern {
            label_filter: Some("l"),
            direction: Direction::Out,
            min_hops: 1,
            max_hops: 2,
            want_path: false,
            collection_filter: CollectionFilter::Unscoped,
        };
        let first = expand_variable_length(&csr, src, &pat, caps, None);
        let cursor = first.cursor.clone().expect("cap=1 must truncate");

        let resumed = resume_variable_length(&csr, &cursor, &pat, caps, None);

        let mut union = dst_set(&first.results);
        union.extend(dst_set(&resumed.results));

        assert!(
            !union.contains(&n3),
            "*1..2 must never emit the depth-3 node n3 across the resume boundary; \
             got {union:?}"
        );
        let expected: std::collections::HashSet<u32> = ["n1", "n2"]
            .iter()
            .map(|n| csr.node_id_raw(n).unwrap())
            .collect();
        assert_eq!(
            union, expected,
            "*1..2 resume union must be exactly the depth-1..2 set {{n1,n2}}"
        );
    }

    fn path_set(results: &[(u32, String)]) -> std::collections::HashSet<String> {
        results.iter().map(|(_, p)| p.clone()).collect()
    }

    /// Spec: with `want_path = true`, a capped expansion resumed from its cursor
    /// renders the FULL path string from the original source for every resumed
    /// row — not a truncated suffix starting at the resume frontier. Exact set
    /// equality of (first-pass ∪ resumed) path strings vs a single uncapped
    /// `want_path` pass. This is the path-continuity guarantee: the cursor
    /// carries each frontier node's accumulated path verbatim, so resume seeds
    /// the exact path context instead of rebuilding only the node's own name.
    #[test]
    fn varlen_resume_want_path_strings_are_full_paths() {
        // Chain n0 -> n1 -> ... -> n6. `*1..6` from n0 yields paths
        // "n0->n1", "n0->n1->n2", ... "n0->n1->...->n6".
        let csr = make_chain(6);
        let src = csr.node_id_raw("n0").unwrap();

        let pat = VarLenPattern {
            label_filter: Some("l"),
            direction: Direction::Out,
            min_hops: 1,
            max_hops: 6,
            want_path: true,
            collection_filter: CollectionFilter::Unscoped,
        };

        // Ground truth: a single uncapped want_path pass.
        let uncapped = expand_variable_length(&csr, src, &pat, VarLenCaps::default(), None);
        assert!(uncapped.cursor.is_none(), "uncapped pass must not truncate");
        let full_paths = path_set(&uncapped.results);
        assert!(
            full_paths.contains("n0->n1->n2->n3->n4->n5->n6"),
            "uncapped want_path pass must render the full chain path; got {full_paths:?}"
        );

        // Inject a low results cap so truncation fires mid-chain.
        let caps = VarLenCaps {
            max_results: 2,
            max_frontier: usize::MAX,
        };
        let first = expand_variable_length(&csr, src, &pat, caps, None);
        let cursor = first
            .cursor
            .clone()
            .expect("low cap must produce a resume cursor");

        // Resume — possibly across multiple rounds — unioning path strings.
        let mut union: std::collections::HashSet<String> = path_set(&first.results);
        let mut next = Some(cursor);
        while let Some(c) = next {
            let resumed = resume_variable_length(&csr, &c, &pat, caps, None);
            union.extend(path_set(&resumed.results));
            next = resumed.cursor;
        }

        assert_eq!(
            union, full_paths,
            "first-pass ∪ resumed path strings must equal the uncapped want_path set; \
             a truncated suffix on the resumed tail would fail this"
        );
    }

    /// Spec (multi-core safety): a resume cursor is fanned to ALL cores on a
    /// node, each running `resume_variable_length` against its OWN CSR. A core
    /// that does not own any of the cursor's frontier NAMES must produce ZERO
    /// results — never spurious rows from misinterpreting a foreign core's
    /// dense local id against its own CSR. With name-keyed frontiers, a name
    /// absent from this CSR resolves to `None` via `node_id_raw` and is skipped.
    #[test]
    fn varlen_resume_skips_unowned_names_yields_empty() {
        // This CSR owns only {n0..n6}. The cursor's frontier names belong to a
        // DIFFERENT core's partition and are entirely absent here.
        let csr = make_chain(6);
        let pat = VarLenPattern {
            label_filter: Some("l"),
            direction: Direction::Out,
            min_hops: 1,
            max_hops: 6,
            want_path: false,
            collection_filter: CollectionFilter::Unscoped,
        };
        let cursor = VarLenCursor {
            frontier: vec![
                ("foreign_a".to_string(), "src->foreign_a".to_string()),
                ("foreign_b".to_string(), "src->foreign_b".to_string()),
            ],
            depth: 2,
        };

        let resumed = resume_variable_length(&csr, &cursor, &pat, VarLenCaps::default(), None);

        assert!(
            resumed.results.is_empty(),
            "a core that owns none of the cursor's frontier names must yield no \
             rows; got {:?}",
            resumed.results
        );
        assert!(
            resumed.cursor.is_none(),
            "an empty resume frontier has nothing to truncate"
        );
    }

    /// Spec (positive resolution): a cursor whose frontier NAMES exist in this
    /// CSR resolves each name to its local id and resumes correctly. Pairing
    /// with the skip-unowned test above, this proves name-keying preserves
    /// single-core correctness while making the cursor core-agnostic: the
    /// owning core resolves and resumes, foreign cores skip.
    #[test]
    fn varlen_resume_resolves_owned_names() {
        // Chain n0 -> ... -> n6. Resume at depth 2 from the owned name "n1"
        // (reached at depth 1 on a hypothetical prior round). Expanding "n1"
        // onward at *1..6 reaches {n2..n6}.
        let csr = make_chain(6);
        let pat = VarLenPattern {
            label_filter: Some("l"),
            direction: Direction::Out,
            min_hops: 1,
            max_hops: 6,
            want_path: false,
            collection_filter: CollectionFilter::Unscoped,
        };
        let cursor = VarLenCursor {
            frontier: vec![("n1".to_string(), "n0->n1".to_string())],
            depth: 2,
        };

        let resumed = resume_variable_length(&csr, &cursor, &pat, VarLenCaps::default(), None);

        let dsts = dst_set(&resumed.results);
        let expected: std::collections::HashSet<u32> = ["n2", "n3", "n4", "n5", "n6"]
            .iter()
            .map(|n| csr.node_id_raw(n).unwrap())
            .collect();
        assert_eq!(
            dsts, expected,
            "resuming from owned name n1 must reach {{n2..n6}}; got {dsts:?}"
        );
    }
}
