// SPDX-License-Identifier: BUSL-1.1

//! Name-keyed variable-length path expansion for read-your-own-writes.
//!
//! The durable variable-length BFS in [`super::expansion`] keys its visited set
//! on dense CSR-local ids, so it cannot walk through a node that exists only via
//! a transaction's staged edge (such a node has no durable id). When a MATCH
//! runs inside a transaction with a non-empty [`GraphOverlayDelta`], the
//! `[*min..max]` expansion instead runs here against a NAME-keyed merge of
//! durable CSR adjacency and the staged overlay, so staged edges are traversed,
//! staged tombstones are hidden, and staged-only intermediate nodes participate.
//!
//! This is the variable-length analogue of the fixed-hop merge in
//! [`super::overlay_expand`]; both share the single neighbour-merge helper
//! [`merge_neighbors_named`] so there is exactly one union+tombstone
//! implementation.
//!
//! Beyond the overlay, this module also carries the boundary-continuation
//! machinery used by BOTH the durable and named BFS paths: a frontier node with
//! zero local (merged) out-degree may have its remaining edges homed on another
//! shard, so it is captured as a resume seed rather than silently dropped — the
//! traversal reaches a cross-boundary edge without depending on a result cap
//! firing first.

use std::collections::HashSet;

use super::expansion::{
    self, CollectionFilter, VarLenCaps, VarLenCursor, VarLenExpansion, VarLenPattern,
};
use super::types::{BindingRow, ExecutionState, VarLenResume};
use crate::engine::graph::csr::{CsrIndex, Direction, GraphOverlayDelta};

/// A resolved variable-length destination: a durable node keeps its CSR id (so
/// the caller binds it by id, exactly as the durable path does), while a node
/// reachable only through a staged edge has no durable id and is carried by
/// name (bound by name instead).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NameOrId {
    /// A durable node whose CSR id is known.
    Id(u32),
    /// A staged-only node that has no durable CSR id.
    Name(String),
}

/// Live BFS state threaded into [`run_bfs_named`]: the accumulated results, the
/// name-keyed visited set, the frontier to expand, and the hop depth to start
/// at. Bundled into one value so the driver stays within the argument budget
/// (the from-scratch and resume entry points differ only in how they build it).
struct NamedBfsSeed {
    results: Vec<(NameOrId, String)>,
    visited: HashSet<String>,
    frontier: Vec<(String, String)>,
    start_depth: usize,
}

/// Name-keyed variable-length expansion from a durable `source`, merging the
/// transaction's staged overlay at every hop.
///
/// `max_hops == 0` is handled by the caller ([`expansion::expand_variable_length`])
/// before dispatch, so `pattern.max_hops >= 1` here.
pub(super) fn expand_named(
    csr: &CsrIndex,
    source: u32,
    pattern: &VarLenPattern<'_>,
    caps: VarLenCaps,
    overlay: &GraphOverlayDelta,
) -> VarLenExpansion {
    let src_name = csr.node_name_raw(source).to_string();

    let mut visited: HashSet<String> = HashSet::new();
    visited.insert(src_name.clone());

    let mut results: Vec<(NameOrId, String)> = Vec::new();
    if pattern.min_hops == 0 {
        let path = if pattern.want_path {
            src_name.clone()
        } else {
            String::new()
        };
        results.push((NameOrId::Id(source), path));
    }

    let seed_path = if pattern.want_path {
        src_name.clone()
    } else {
        String::new()
    };
    let seed = NamedBfsSeed {
        results,
        visited,
        frontier: vec![(src_name, seed_path)],
        start_depth: 1,
    };
    run_bfs_named(csr, seed, pattern, caps, overlay)
}

/// Resume a name-keyed variable-length expansion from a [`VarLenCursor`].
///
/// Each frontier name is resolved against THIS core's durable CSR OR the
/// overlay's staged endpoints: a name owned locally (durably or as a staged
/// endpoint) seeds the BFS, a name owned by neither is skipped. This preserves
/// the same cross-core self-scoping the durable resume path relies on — a
/// resume plan fanned to all cores only resumes on the core that owns the
/// frontier.
pub(super) fn resume_named(
    csr: &CsrIndex,
    cursor: &VarLenCursor,
    pattern: &VarLenPattern<'_>,
    caps: VarLenCaps,
    overlay: &GraphOverlayDelta,
) -> VarLenExpansion {
    let mut visited: HashSet<String> = HashSet::new();
    let mut frontier: Vec<(String, String)> = Vec::with_capacity(cursor.frontier.len());
    for (name, path) in &cursor.frontier {
        let owned =
            csr.node_id_raw(name).is_some() || overlay.staged_endpoint_names().any(|n| n == name);
        if !owned {
            continue;
        }
        if !visited.insert(name.clone()) {
            continue;
        }
        frontier.push((name.clone(), path.clone()));
    }
    let seed = NamedBfsSeed {
        results: Vec::new(),
        visited,
        frontier,
        start_depth: cursor.depth,
    };
    run_bfs_named(csr, seed, pattern, caps, overlay)
}

/// Name-keyed BFS driver, parallel to [`expansion`]'s durable `run_bfs`.
///
/// Kept as a separate driver (rather than genericising the durable BFS) so the
/// hot, heavily-tested durable path is untouched. Destinations are emitted as
/// [`NameOrId`] so a durable neighbour binds by id and a staged-only neighbour
/// binds by name. Frontier nodes with zero merged out-degree are captured in
/// `boundary` for cross-shard continuation instead of being dropped.
fn run_bfs_named(
    csr: &CsrIndex,
    seed: NamedBfsSeed,
    pattern: &VarLenPattern<'_>,
    caps: VarLenCaps,
    overlay: &GraphOverlayDelta,
) -> VarLenExpansion {
    let NamedBfsSeed {
        mut results,
        mut visited,
        mut frontier,
        start_depth,
    } = seed;
    let mut cursor: Option<VarLenCursor> = None;
    let mut boundary: Vec<(String, String, usize)> = Vec::new();

    for depth in start_depth..=pattern.max_hops {
        if frontier.is_empty() {
            break;
        }

        let mut next_frontier: Vec<(String, String)> = Vec::new();

        for (node_name, path) in &frontier {
            let src_id = csr.node_id_raw(node_name);
            let neighbors = merge_neighbors_named(
                csr,
                node_name,
                src_id,
                pattern.label_filter,
                pattern.direction,
                pattern.collection_filter,
                overlay,
            );

            // Zero local merged out-degree: the node's remaining edges (if any)
            // are homed elsewhere. Capture it so the traversal can continue on
            // the owning shard instead of dropping the partial match.
            if neighbors.is_empty() {
                boundary.push((node_name.clone(), path.clone(), depth));
                continue;
            }

            for (_label, dst_name) in neighbors {
                if !visited.insert(dst_name.clone()) {
                    continue;
                }

                let new_path = if pattern.want_path {
                    format!("{path}->{dst_name}")
                } else {
                    String::new()
                };

                if depth >= pattern.min_hops {
                    let bound = match csr.node_id_raw(&dst_name) {
                        Some(id) => NameOrId::Id(id),
                        None => NameOrId::Name(dst_name.clone()),
                    };
                    results.push((bound, new_path.clone()));
                }

                if depth < pattern.max_hops {
                    next_frontier.push((dst_name, new_path));
                }
            }
        }

        let cap_hit = results.len() >= caps.max_results || next_frontier.len() >= caps.max_frontier;
        if cap_hit {
            if depth < pattern.max_hops && !next_frontier.is_empty() {
                // The named frontier is already keyed by global node name, so no
                // id→name conversion is needed (unlike the durable path).
                cursor = Some(VarLenCursor {
                    frontier: next_frontier,
                    depth: depth + 1,
                });
            }
            break;
        }

        frontier = next_frontier;
    }

    VarLenExpansion {
        results: Vec::new(),
        named_results: results,
        cursor,
        boundary,
    }
}

/// Merge a source node's durable CSR neighbours with the transaction's staged
/// edges for one hop, as `(label_name, neighbour_name)` pairs.
///
/// Durable neighbours whose backing edge is staged-tombstoned are dropped;
/// staged edges matching the direction and label are added (deduplicated
/// against the durable set). `src_id` is `Some` for a durable source (its CSR
/// adjacency is walked) and `None` for a staged-only source (only its staged
/// edges exist). Out- and in-edges are collected under a single direction each
/// so the tombstone lookup always knows the edge's orientation, even for a
/// `Both` triple.
///
/// This is the single neighbour-merge implementation shared by the fixed-hop
/// path ([`super::overlay_expand`]) and the variable-length path here.
pub(super) fn merge_neighbors_named(
    csr: &CsrIndex,
    src_name: &str,
    src_id: Option<u32>,
    label_filter: Option<&str>,
    direction: Direction,
    collection_filter: CollectionFilter,
    overlay: &GraphOverlayDelta,
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let want_out = matches!(direction, Direction::Out | Direction::Both);
    let want_in = matches!(direction, Direction::In | Direction::Both);

    if let Some(id) = src_id {
        // Durable adjacency, collected per-direction so tombstone orientation
        // is unambiguous even for a `Both` triple.
        if want_out {
            out.extend(durable_named(
                csr,
                id,
                label_filter,
                Direction::Out,
                collection_filter,
                src_name,
                overlay,
            ));
        }
        if want_in {
            out.extend(durable_named(
                csr,
                id,
                label_filter,
                Direction::In,
                collection_filter,
                src_name,
                overlay,
            ));
        }
    }

    if want_out {
        for (label, dst) in overlay.out_neighbors(src_name, label_filter.as_slice()) {
            if !out
                .iter()
                .any(|(l, n)| l.as_str() == label && n.as_str() == dst)
            {
                out.push((label.to_string(), dst.to_string()));
            }
        }
    }
    if want_in {
        for (label, src) in overlay.in_neighbors(src_name, label_filter.as_slice()) {
            if !out
                .iter()
                .any(|(l, n)| l.as_str() == label && n.as_str() == src)
            {
                out.push((label.to_string(), src.to_string()));
            }
        }
    }
    out
}

/// Durable CSR neighbours of `id` in one direction, as `(label, neighbour)`
/// names, with any staged-tombstoned edge removed. Split out from
/// [`merge_neighbors_named`] so out- and in-edges are each collected under a
/// single known direction, keeping the tombstone lookup's `(src, label, dst)`
/// orientation unambiguous.
fn durable_named(
    csr: &CsrIndex,
    id: u32,
    label_filter: Option<&str>,
    dir: Direction,
    collection_filter: CollectionFilter,
    src_name: &str,
    overlay: &GraphOverlayDelta,
) -> Vec<(String, String)> {
    let mut v = Vec::new();
    for (lid, other_id) in
        expansion::collect_neighbors(csr, id, label_filter, dir, collection_filter)
    {
        let label = csr.label_name(lid).to_string();
        let other = csr.node_name_raw(other_id).to_string();
        let tombstoned = if matches!(dir, Direction::In) {
            overlay.is_tombstoned(&other, &label, src_name)
        } else {
            overlay.is_tombstoned(src_name, &label, &other)
        };
        if !tombstoned {
            v.push((label, other));
        }
    }
    v
}

/// Record a [`VarLenResume`] for each boundary node homed on a remote shard.
///
/// `boundary` entries are `(node_name, path_so_far, resume_depth)` produced by
/// either BFS driver for frontier nodes with zero local merged out-degree.
/// Emission is gated on `state.is_remote_node`: in a true single-node
/// deployment the predicate is `None`, so nothing is emitted and behaviour is
/// unchanged; in cluster mode only nodes the predicate marks remote are shipped
/// (mirroring the fixed-hop `UnresolvedExpansion` gating). `source_row` is the
/// binding row to re-seed the resumed rows with, already carrying this
/// expansion's source binding.
pub(super) fn record_boundary_resumes(
    state: &mut ExecutionState,
    triple_idx: usize,
    source_row: &BindingRow,
    boundary: &[(String, String, usize)],
) {
    let Some(pred) = state.is_remote_node else {
        return;
    };
    for (name, path, depth) in boundary {
        if pred(name) {
            state.record_truncation(VarLenResume {
                triple_idx,
                source_row: source_row.clone(),
                frontier: vec![(name.clone(), path.clone())],
                depth: *depth,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::test_support::test_scoped_memory;

    fn pattern(label: &str, min: usize, max: usize) -> VarLenPattern<'_> {
        VarLenPattern {
            label_filter: Some(label),
            direction: Direction::Out,
            min_hops: min,
            max_hops: max,
            want_path: false,
            collection_filter: CollectionFilter::Unscoped,
        }
    }

    fn name_set(exp: &VarLenExpansion, csr: &CsrIndex) -> std::collections::HashSet<String> {
        exp.named_results
            .iter()
            .map(|(b, _)| match b {
                NameOrId::Id(id) => csr.node_name_raw(*id).to_string(),
                NameOrId::Name(n) => n.clone(),
            })
            .collect()
    }

    /// A staged edge is traversed by the name-keyed expansion, reaching a
    /// staged-only node (no durable CSR id) that the durable BFS could not.
    #[test]
    fn staged_edge_traversed_by_name() {
        let mut csr = CsrIndex::new(test_scoped_memory());
        csr.add_edge("a", "R", "b").unwrap();
        let mut ov = GraphOverlayDelta::new();
        // b -> c staged: c has no durable id.
        ov.stage_edge("b", "R", "c");

        let src = csr.node_id_raw("a").unwrap();
        let exp = expand_named(&csr, src, &pattern("R", 1, 3), VarLenCaps::default(), &ov);

        let names = name_set(&exp, &csr);
        assert!(names.contains("b"), "durable hop a->b must be reached");
        assert!(
            names.contains("c"),
            "staged hop b->c must be reached through name-keyed BFS; got {names:?}"
        );
        // `c` is staged-only, so it is carried by name, not id.
        assert!(
            exp.named_results
                .iter()
                .any(|(b, _)| *b == NameOrId::Name("c".to_string())),
            "staged-only node c must be a Name variant"
        );
    }

    /// First-round ∪ resumed equals the uncapped-with-overlay ground truth: a
    /// low results cap truncates the name-keyed BFS, and resuming from the
    /// cursor recovers exactly the missing destinations.
    #[test]
    fn named_resume_union_equals_uncapped() {
        let mut csr = CsrIndex::new(test_scoped_memory());
        for i in 0..3 {
            csr.add_edge(&format!("n{i}"), "R", &format!("n{}", i + 1))
                .unwrap();
        }
        // Staged tail: n3 -> n4 -> n5 (n4, n5 staged-only).
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("n3", "R", "n4");
        ov.stage_edge("n4", "R", "n5");

        let src = csr.node_id_raw("n0").unwrap();
        let pat = pattern("R", 1, 6);

        let uncapped = expand_named(&csr, src, &pat, VarLenCaps::default(), &ov);
        assert!(uncapped.cursor.is_none());
        let full = name_set(&uncapped, &csr);

        let caps = VarLenCaps {
            max_results: 2,
            max_frontier: usize::MAX,
        };
        let first = expand_named(&csr, src, &pat, caps, &ov);
        let mut union = name_set(&first, &csr);
        let mut next = first.cursor;
        while let Some(c) = next {
            let resumed = resume_named(&csr, &c, &pat, caps, &ov);
            union.extend(name_set(&resumed, &csr));
            next = resumed.cursor;
        }

        assert_eq!(
            union, full,
            "first-round ∪ resumed must equal uncapped-with-overlay set"
        );
        assert!(
            full.contains("n5"),
            "staged tail must be reachable uncapped"
        );
    }

    /// A foreign core that owns none of the resume frontier's names (neither
    /// durably nor as a staged endpoint) yields no results.
    #[test]
    fn named_resume_foreign_core_skips_unowned() {
        // This core owns nothing referenced by the cursor: empty CSR, and the
        // only staged endpoints belong to an unrelated local edge.
        let csr = CsrIndex::new(test_scoped_memory());
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("local_only", "R", "x");

        let cursor = VarLenCursor {
            frontier: vec![
                ("foreign_a".to_string(), "s->foreign_a".to_string()),
                ("foreign_b".to_string(), "s->foreign_b".to_string()),
            ],
            depth: 2,
        };
        let resumed = resume_named(
            &csr,
            &cursor,
            &pattern("R", 1, 6),
            VarLenCaps::default(),
            &ov,
        );
        assert!(
            resumed.named_results.is_empty(),
            "unowned frontier names must yield no results; got {:?}",
            resumed.named_results
        );
        assert!(resumed.boundary.is_empty());
    }

    /// Boundary capture: a frontier node with zero local out-degree is recorded
    /// in `boundary` (not dropped) and, when `is_remote_node` marks it remote,
    /// surfaces as a resume via [`record_boundary_resumes`].
    #[test]
    fn boundary_node_captured_and_recorded() {
        let mut csr = CsrIndex::new(test_scoped_memory());
        csr.add_edge("a", "R", "b").unwrap();
        // Non-empty overlay (forces the name-keyed path) that does NOT give `b`
        // any out-edge, so `b` is a genuine zero-out-degree boundary node.
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("unrelated_src", "R", "unrelated_dst");

        let src = csr.node_id_raw("a").unwrap();
        let exp = expand_named(&csr, src, &pattern("R", 1, 4), VarLenCaps::default(), &ov);
        assert!(
            exp.boundary.iter().any(|(n, _, _)| n == "b"),
            "zero-out-degree node b must be captured in boundary; got {:?}",
            exp.boundary
        );

        // is_remote_node marks b remote → a resume is recorded.
        let pred = |n: &str| n == "b";
        let mut state = ExecutionState::new(Some(&pred), VarLenCaps::default());
        let mut source_row = BindingRow::new();
        source_row.insert("a".to_string(), "a".to_string());
        record_boundary_resumes(&mut state, 0, &source_row, &exp.boundary);
        assert!(
            state.truncated(),
            "remote boundary node must record a resume"
        );
    }

    /// A boundary node is NOT captured when it has a staged local out-edge: the
    /// merged out-degree is non-zero, so the BFS continues locally.
    #[test]
    fn boundary_not_captured_with_staged_out_edge() {
        let mut csr = CsrIndex::new(test_scoped_memory());
        csr.add_edge("a", "R", "b").unwrap();
        let mut ov = GraphOverlayDelta::new();
        // b has a staged out-edge, so it is NOT a boundary node.
        ov.stage_edge("b", "R", "c");

        let src = csr.node_id_raw("a").unwrap();
        let exp = expand_named(&csr, src, &pattern("R", 1, 4), VarLenCaps::default(), &ov);
        assert!(
            !exp.boundary.iter().any(|(n, _, _)| n == "b"),
            "b has a staged out-edge and must not be a boundary node; got {:?}",
            exp.boundary
        );
        // c is a boundary node (staged-only leaf, no further edges).
        assert!(exp.boundary.iter().any(|(n, _, _)| n == "c"));
    }

    /// Free-ranging two-anchor correctness at the driver level: two independent
    /// sources each expand their own staged tails without cross-contamination.
    #[test]
    fn two_anchor_named_expansion() {
        let mut csr = CsrIndex::new(test_scoped_memory());
        csr.add_edge("a", "R", "b").unwrap();
        csr.add_edge("p", "R", "q").unwrap();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("b", "R", "c");
        ov.stage_edge("q", "R", "r");

        let a = csr.node_id_raw("a").unwrap();
        let p = csr.node_id_raw("p").unwrap();
        let pat = pattern("R", 1, 3);

        let from_a = name_set(
            &expand_named(&csr, a, &pat, VarLenCaps::default(), &ov),
            &csr,
        );
        let from_p = name_set(
            &expand_named(&csr, p, &pat, VarLenCaps::default(), &ov),
            &csr,
        );

        assert_eq!(
            from_a,
            ["b", "c"]
                .into_iter()
                .map(String::from)
                .collect::<HashSet<String>>(),
            "anchor a reaches only its own tail"
        );
        assert_eq!(
            from_p,
            ["q", "r"]
                .into_iter()
                .map(String::from)
                .collect::<HashSet<String>>(),
            "anchor p reaches only its own tail"
        );
    }
}
