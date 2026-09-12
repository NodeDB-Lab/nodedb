// SPDX-License-Identifier: BUSL-1.1

//! Single-triple `(src)-[edge]->(dst)` evaluation: fixed-hop and
//! variable-length expansion, read-your-own-writes overlay dispatch, and
//! cross-shard frontier recording.

use crate::engine::graph::csr::{CsrIndex, GraphOverlayDelta};
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::pattern::ast::PatternTriple;
use crate::engine::graph::pattern::executor::expansion;
use crate::engine::graph::pattern::executor::overlay_expand;
use crate::engine::graph::pattern::executor::types::{
    BindingRow, ExecutionState, UnresolvedExpansion, VarLenResume,
};
use crate::engine::graph::pattern::executor::varlen_named::{self, NameOrId};

use super::binding::{bind_node, binding_compatible, resolve_binding};

/// Execute a single triple `(src)-[edge]->(dst)` against a binding row.
///
/// `triple_idx` is the 0-based position of this triple within its chain;
/// it is recorded in any `UnresolvedExpansion` emitted.
pub(in crate::engine::graph::pattern::executor) fn execute_triple(
    triple: &PatternTriple,
    triple_idx: usize,
    csr: &CsrIndex,
    input_row: &BindingRow,
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    overlay: Option<&GraphOverlayDelta>,
) -> Result<Vec<BindingRow>, crate::Error> {
    // Read-your-own-writes: inside a transaction with staged graph edges, a
    // fixed-hop triple is expanded against a name-keyed merge of durable CSR
    // adjacency and the staged overlay, so staged edges are visible, staged
    // tombstones are hidden, and staged-only intermediate nodes (which have no
    // durable CSR id) participate. Variable-length edges keep the durable BFS
    // path (its visited set keys on dense CSR ids); autocommit / empty-overlay
    // runs are unaffected and fall through to the durable path below.
    //
    // The merge runs in cluster mode too: a bound source whose merged (durable
    // ∪ staged, minus tombstone) out-degree is zero and which the locality
    // predicate marks remote is emitted as a cross-shard `UnresolvedExpansion`
    // by `expand_triple_overlay`, exactly as the durable fixed-hop path does for
    // a zero-raw-degree bound source, so a resumed pattern's fixed-hop tail
    // continues onto the staged edge's owning core instead of being dropped.
    if let Some(ov) = overlay
        && !ov.is_empty()
        && !triple.edge.is_variable_length()
    {
        return Ok(overlay_expand::expand_triple_overlay(
            triple,
            triple_idx,
            csr,
            input_row,
            state,
            frontier_bitmap,
            ov,
        ));
    }

    let direction = triple.edge.direction.to_csr_direction();
    let label_filter = triple.edge.edge_type.as_deref();
    let src_nodes = resolve_binding(&triple.src, csr, input_row, frontier_bitmap);

    if src_nodes.is_empty() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();

    if triple.edge.is_variable_length() {
        // Path strings are only needed when the edge variable is bound
        // (e.g. `(a)-[e*1..3]->(b) RETURN e`). For anonymous variable
        // expansions skip all `format!`/`String` work in the hot loop.
        let want_path = triple.edge.name.is_some();
        let pattern = expansion::VarLenPattern {
            label_filter,
            direction,
            min_hops: triple.edge.min_hops,
            max_hops: triple.edge.max_hops,
            want_path,
            collection_filter: state.collection_filter,
        };
        for &src_id in &src_nodes {
            let expansion = expansion::expand_variable_length(
                csr,
                src_id,
                &pattern,
                state.varlen_caps,
                overlay,
            );
            if let Some(cursor) = expansion.cursor {
                // Capture the LIVE resume cursor instead of silently dropping
                // the un-expanded frontier. The cursor's `source_row` MUST carry
                // this expansion's source binding (e.g. `a = src_id`): a
                // free-ranging anchor has an empty `input_row`, and the resumed
                // rows are rebuilt from `source_row`, so without binding the
                // source here the resumed rows would lack the anchor variable and
                // be dropped by a `WHERE`/projection that references it.
                let mut source_row = input_row.clone();
                bind_node(&mut source_row, &triple.src, csr, src_id);
                state.record_truncation(VarLenResume {
                    triple_idx,
                    source_row,
                    frontier: cursor.frontier,
                    depth: cursor.depth,
                });
            }

            // Cross-boundary continuations: frontier nodes with zero local
            // out-degree whose remaining edges may be homed on another shard.
            // Shipping them (gated on the remote-node predicate) is what makes a
            // staged/durable cross-boundary edge reachable without depending on
            // the result cap firing. The anchor's binding is carried on the
            // resumed rows via `source_row`, identical to the cap-cursor case.
            if !expansion.boundary.is_empty() && state.is_remote_node.is_some() {
                let mut source_row = input_row.clone();
                bind_node(&mut source_row, &triple.src, csr, src_id);
                varlen_named::record_boundary_resumes(
                    state,
                    triple_idx,
                    &source_row,
                    &expansion.boundary,
                );
            }

            for (dst_id, path) in expansion.results {
                if !binding_compatible(&triple.dst, csr, input_row, dst_id) {
                    continue;
                }
                let mut row = input_row.clone();
                bind_node(&mut row, &triple.src, csr, src_id);
                bind_node(&mut row, &triple.dst, csr, dst_id);
                if let Some(ref edge_name) = triple.edge.name {
                    row.insert(edge_name.clone(), path);
                }
                results.push(row);
            }

            // Overlay path destinations: a durable dst binds by id (as above); a
            // staged-only dst has no CSR id and binds by name.
            for (bound, path) in expansion.named_results {
                match bound {
                    NameOrId::Id(dst_id) => {
                        if !binding_compatible(&triple.dst, csr, input_row, dst_id) {
                            continue;
                        }
                        let mut row = input_row.clone();
                        bind_node(&mut row, &triple.src, csr, src_id);
                        bind_node(&mut row, &triple.dst, csr, dst_id);
                        if let Some(ref edge_name) = triple.edge.name {
                            row.insert(edge_name.clone(), path);
                        }
                        results.push(row);
                    }
                    NameOrId::Name(dst_name) => {
                        if !overlay_expand::dst_compatible(&triple.dst, csr, input_row, &dst_name) {
                            continue;
                        }
                        let mut row = input_row.clone();
                        bind_node(&mut row, &triple.src, csr, src_id);
                        overlay_expand::bind_name(&mut row, &triple.dst, &dst_name);
                        if let Some(ref edge_name) = triple.edge.name {
                            row.insert(edge_name.clone(), path);
                        }
                        results.push(row);
                    }
                }
            }
        }
    } else {
        // Determine whether the source variable was BOUND (resolved from a
        // prior binding in `input_row` or from a literal match) or
        // FREE-RANGING (enumerated over all local nodes because no binding
        // existed yet).  `resolve_binding` takes the BOUND path only when
        // `binding.name` is `Some` AND that name already appears in `input_row`.
        // Everything else — anonymous nodes, unbound variables, and
        // frontier-bitmap-restricted enumeration — is FREE-RANGING.
        //
        // Only BOUND sources can produce a frontier entry: they represent a
        // locally-originated partial match whose continuation must be dispatched
        // to the source node's home shard.  A FREE-RANGING source must NOT emit
        // because every shard will range over the same local nodes during its own
        // pass; emitting here would duplicate work and pollute the frontier with
        // every zero-degree sink.
        let source_is_bound = triple
            .src
            .name
            .as_deref()
            .is_some_and(|n| input_row.contains_key(n));

        for &src_id in &src_nodes {
            // Check raw degree in the queried direction BEFORE any label
            // filter. A source with zero raw adjacency means its edges may
            // live on a remote shard — record it in the frontier so the
            // Control Plane can dispatch a continuation. A source that has
            // edges locally but none pass the label filter is a legitimate
            // empty local result; do NOT add it to the frontier.
            let raw_degree = match direction {
                Direction::Out => csr.out_degree_raw(src_id),
                Direction::In => csr.in_degree_raw(src_id),
                Direction::Both => csr.out_degree_raw(src_id) + csr.in_degree_raw(src_id),
            };
            if raw_degree == 0 {
                // Emit a frontier entry only when ALL four conditions hold:
                // 1. The source variable was BOUND (not free-ranging).
                // 2. The caller supplied a locality predicate.
                // 3. The predicate identifies this node as remote.
                // 4. (implicit) Zero raw adjacency — we are inside this branch.
                //
                // A free-ranging unbound source NEVER emits regardless of
                // degree or predicate.  Without a predicate (None) — the
                // fully-local single-node path — every leaf is a legitimate
                // terminal, not a cross-shard ghost.
                if source_is_bound && let Some(pred) = state.is_remote_node {
                    let node_name = csr.node_name_raw(src_id).to_string();
                    if pred(&node_name) {
                        let binding_var =
                            triple.src.name.clone().unwrap_or_else(|| node_name.clone());
                        state.frontier.push(UnresolvedExpansion {
                            binding_var,
                            node_name,
                            triple_idx,
                            partial_row: input_row.clone(),
                        });
                    }
                }
                // No local edges to produce — continue to next src.
                continue;
            }

            let neighbors = expansion::collect_neighbors(
                csr,
                src_id,
                label_filter,
                direction,
                state.collection_filter,
            );
            for (lid, dst_id) in neighbors {
                if !binding_compatible(&triple.dst, csr, input_row, dst_id) {
                    continue;
                }
                let mut row = input_row.clone();
                bind_node(&mut row, &triple.src, csr, src_id);
                bind_node(&mut row, &triple.dst, csr, dst_id);
                if let Some(ref edge_name) = triple.edge.name {
                    let src_name = csr.node_name_raw(src_id);
                    let dst_name = csr.node_name_raw(dst_id);
                    let label_name = csr.label_name(lid);
                    row.insert(
                        edge_name.clone(),
                        format!("{src_name}|{label_name}|{dst_name}"),
                    );
                }
                results.push(row);
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
pub(in crate::engine::graph::pattern::executor) mod tests {
    use crate::engine::graph::csr::CsrIndex;
    use crate::engine::graph::edge_store::EdgeStore;
    use crate::engine::graph::pattern::ast::ComparisonOp;
    use crate::engine::graph::pattern::compiler::parse;
    use crate::engine::graph::pattern::executor::core::{MatchExecCtx, execute, rows_to_msgpack};
    use crate::engine::graph::pattern::executor::expansion;
    use crate::engine::graph::pattern::executor::predicates::{
        PropertyLookup, check_property_for_test as check,
    };
    use crate::engine::graph::pattern::executor::types::BindingRow;
    use crate::engine::sparse::btree::SparseEngine;

    /// Open a standalone `SparseEngine` in a fresh tempdir for tests that need
    /// to exercise the property-predicate path (or just to satisfy the
    /// `PropertyLookup` borrow for tests that have no property predicates).
    pub(crate) fn make_sparse() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let sparse = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
        (sparse, dir)
    }

    /// Build a `PropertyLookup` over `sparse` + `csr` scoped to the `make_csr`
    /// graph's `(DatabaseId::DEFAULT, TenantId::new(1), "col")`.
    ///
    /// `csr` resolves a bound node name to its surrogate; the document is then
    /// fetched at `StorageKey::for_surrogate(surrogate)`, mirroring the real keying.
    pub(crate) fn props_for<'a>(sparse: &'a SparseEngine, csr: &'a CsrIndex) -> PropertyLookup<'a> {
        PropertyLookup {
            sparse,
            csr,
            database_id: 0,
            tenant_id: 1,
            collection: Some("col"),
        }
    }

    fn make_social_graph() -> (CsrIndex, EdgeStore, tempfile::TempDir) {
        make_csr(&[
            ("alice", "KNOWS", "bob"),
            ("bob", "KNOWS", "carol"),
            ("carol", "KNOWS", "dave"),
            ("alice", "LIKES", "carol"),
            ("bob", "BLOCKED", "dave"),
        ])
    }

    #[test]
    fn execute_simple_one_hop() {
        let (csr, store, _dir) = make_social_graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query = parse("MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "alice");
        assert_eq!(rows[0]["b"], "bob");
    }

    #[test]
    fn execute_two_hops() {
        let (csr, store, _dir) = make_social_graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query = parse("MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a = 'alice' RETURN a, b, c")
            .unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["c"], "carol");
    }

    #[test]
    fn execute_optional_match() {
        let (csr, store, _dir) = make_social_graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query = parse(
            "MATCH (a)-[:KNOWS]->(b) OPTIONAL MATCH (b)-[:LIKES]->(c) WHERE a = 'alice' RETURN a, b, c",
        )
        .unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["c"], "NULL");
    }

    #[test]
    fn execute_anti_join() {
        let (csr, store, _dir) = make_social_graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query = parse(
            "MATCH (a)-[:KNOWS]->(b) WHERE NOT EXISTS { MATCH (a)-[:BLOCKED]->(b) } RETURN a, b",
        )
        .unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn execute_with_limit() {
        let (csr, store, _dir) = make_social_graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query = parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b LIMIT 2").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn execute_empty_result() {
        let (csr, store, _dir) = make_social_graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query = parse("MATCH (a)-[:NONEXISTENT]->(b) RETURN a, b").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert!(rows.is_empty());
    }

    #[test]
    fn execute_with_node_labels() {
        let (mut csr, store, _dir) = make_social_graph();
        let (sparse, _sdir) = make_sparse();

        // Set labels.
        csr.add_node_label("alice", "Person").unwrap();
        csr.add_node_label("bob", "Person").unwrap();
        csr.add_node_label("carol", "Person").unwrap();
        csr.add_node_label("dave", "Bot").unwrap();

        // Build the lookup AFTER mutating the CSR so the immutable borrow does
        // not overlap the `set`/`add` calls above.
        let props = props_for(&sparse, &csr);

        // Without label filter — all KNOWS edges.
        let query = parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 3);

        // With label filter — only Person src.
        let query = parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        // alice->bob, bob->carol, carol->dave — all 3 srcs are Person.
        assert_eq!(rows.len(), 3);

        // With label filter — only Bot dst.
        let query = parse("MATCH (a)-[:KNOWS]->(b:Bot) RETURN a, b").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        // Only carol->dave where dave is Bot.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "carol");
        assert_eq!(rows[0]["b"], "dave");

        // Both labels — Person->Bot.
        let query = parse("MATCH (a:Person)-[:KNOWS]->(b:Bot) RETURN a, b").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "carol");

        // Non-matching labels — should return 0.
        let query = parse("MATCH (a:Bot)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert!(rows.is_empty());
    }

    #[test]
    fn rows_to_msgpack_format() {
        let mut row = BindingRow::new();
        row.insert("a".into(), "alice".into());
        row.insert("b".into(), "bob".into());
        let msgpack = rows_to_msgpack(&[row]).unwrap();
        let json = nodedb_types::json_from_msgpack(&msgpack).unwrap();
        let arr = json.as_array().unwrap();
        assert_eq!(arr[0]["a"], "alice");
    }

    /// Anchor nodes not in the frontier bitmap are never expanded as sources.
    /// alice (surrogate 1) is in the bitmap; bob and carol (surrogates 2, 3)
    /// are not. A free-variable MATCH should only yield rows where the src
    /// anchor is alice.
    #[test]
    fn match_frontier_bitmap_excludes_non_member_anchors() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let (mut csr, store, _dir) = make_social_graph();
        let (sparse, _sdir) = make_sparse();
        // Assign surrogates: alice=1, bob=2, carol=3, dave=4.
        csr.set_node_surrogate("alice", Surrogate::new(1));
        csr.set_node_surrogate("bob", Surrogate::new(2));
        csr.set_node_surrogate("carol", Surrogate::new(3));
        csr.set_node_surrogate("dave", Surrogate::new(4));
        // Build the lookup AFTER mutating the CSR so the immutable borrow does
        // not overlap the surrogate assignments above.
        let props = props_for(&sparse, &csr);

        // Bitmap contains only alice (surrogate 1).
        let bm = SurrogateBitmap::from_iter([Surrogate::new(1)]);

        let query = parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: Some(&bm),
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;

        // Only alice->bob should appear; bob->carol and carol->dave are blocked
        // because the src anchor (bob, carol) is not in the bitmap.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "alice");
        assert_eq!(rows[0]["b"], "bob");
    }

    // ── Unresolved frontier tests ─────────────────────────────────────────

    /// Helper: build a CsrIndex from a list of `(src, label, dst)` edges.
    pub(crate) fn make_csr(
        edges: &[(&str, &str, &str)],
    ) -> (
        CsrIndex,
        crate::engine::graph::edge_store::EdgeStore,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store =
            crate::engine::graph::edge_store::EdgeStore::open(&dir.path().join("graph.redb"))
                .unwrap();

        use crate::engine::graph::edge_store::EdgeRef;
        use nodedb_types::{DatabaseId, TenantId};
        const DB: DatabaseId = DatabaseId::DEFAULT;
        const T: TenantId = TenantId::new(1);
        let mut ord = 0i64;
        for &(src, label, dst) in edges {
            ord += 1;
            store
                .put_edge_versioned(
                    EdgeRef::new(DB, T, "col", src, label, dst),
                    b"",
                    ord,
                    ord,
                    i64::MAX,
                )
                .unwrap();
        }
        let csr = crate::engine::graph::csr::rebuild::rebuild_from_store(&store).unwrap();
        (csr, store, dir)
    }

    /// Frontier emitted for a BOUND multi-hop intermediate: hop-1 binds `b`
    /// to "bob" via the output row; hop-2 receives `b` as a bound source
    /// (present in `input_row`). Bob has zero out-edges AND is flagged remote,
    /// so the executor records it in `unresolved_frontier`.
    ///
    /// The hop-1 source `a` is free-ranging (WHERE a='alice' is a
    /// post-processing predicate, not carried in the initial `input_row={}`),
    /// so it NEVER produces a frontier entry — only the bound intermediate
    /// `b` does.
    ///
    /// Graph: alice --KNOWS--> bob (bob has no out-edges)
    /// Query: MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a = 'alice'
    /// bob is bound from hop-1; hop-2 finds bob has 0 out-edges AND bob is
    /// flagged remote → exactly one frontier entry.
    #[test]
    fn frontier_emitted_when_source_has_zero_out_degree() {
        let (csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query = parse("MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a = 'alice' RETURN a, b, c")
            .unwrap();
        // "bob" is the hop-2 source with zero out-edges; mark it remote.
        let is_remote: &dyn Fn(&str) -> bool = &|name| name == "bob";
        let outcome = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: Some(is_remote),
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap();

        // No local rows — bob has no out-edges locally.
        assert!(
            outcome.rows.is_empty(),
            "expected no rows; got {:?}",
            outcome.rows
        );

        // Exactly one frontier entry for the hop-2 source "bob".
        assert_eq!(
            outcome.unresolved_frontier.len(),
            1,
            "expected 1 frontier entry; got {:?}",
            outcome.unresolved_frontier.len()
        );
        let entry = &outcome.unresolved_frontier[0];
        assert_eq!(entry.node_name, "bob");
        assert_eq!(entry.triple_idx, 1, "hop-2 is triple_idx 1");
        // partial_row carries the hop-1 binding.
        assert_eq!(
            entry.partial_row.get("a").map(String::as_str),
            Some("alice")
        );
        assert_eq!(entry.partial_row.get("b").map(String::as_str), Some("bob"));
    }

    /// No false positive on label miss: a source HAS out-edges but none
    /// match the triple's label filter. The executor must NOT emit a frontier
    /// entry — this is a legitimate empty result, not a cross-shard gap.
    ///
    /// Two independent gates both suppress the frontier here:
    /// (1) The source variable `a` is free-ranging (WHERE a='alice' is a
    ///     post-processing predicate, not a binding carried in `input_row`),
    ///     so the bound gate suppresses emission.
    /// (2) Even if `a` were bound, alice has a LIKES out-edge so raw_degree
    ///     > 0 in the Out direction, which means the degree gate would also
    ///     suppress it — a node with local edges but no matching-label edges
    ///     is a legitimate empty result, not a cross-shard gap.
    ///
    /// We pass an all-remote predicate (`|_| true`) to prove that neither
    /// the degree gate nor the bound gate is defeated by the predicate alone.
    ///
    /// Graph: alice --LIKES--> bob (alice has LIKES out-edge, no KNOWS out-edge)
    /// Query: MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b
    #[test]
    fn no_frontier_when_source_has_edges_but_label_does_not_match() {
        let (csr, store, _dir) = make_csr(&[("alice", "LIKES", "bob")]);
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query = parse("MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b").unwrap();
        // All-remote predicate: even with every node marked remote, a source
        // that is free-ranging or has local edges must not produce a frontier
        // entry.
        let is_remote: &dyn Fn(&str) -> bool = &|_| true;
        let outcome = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: Some(is_remote),
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap();

        assert!(outcome.rows.is_empty(), "expected no rows");
        assert!(
            outcome.unresolved_frontier.is_empty(),
            "must NOT emit frontier when source is free-ranging or has local edges; \
             got {:?}",
            outcome.unresolved_frontier
        );
    }

    /// Normal local expansion unchanged: a source with matching local edges
    /// produces rows as expected and the frontier stays empty.
    /// With `None` predicate no frontier entries can ever be emitted.
    #[test]
    fn no_frontier_for_fully_local_expansion() {
        let (csr, store, _dir) = make_social_graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query = parse("MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b").unwrap();
        let outcome = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap();

        assert_eq!(outcome.rows.len(), 1);
        assert_eq!(outcome.rows[0]["b"], "bob");
        assert!(
            outcome.unresolved_frontier.is_empty(),
            "frontier must be empty when all edges resolve locally"
        );
    }

    /// Multi-hop: a 2-triple pattern where hop-1 binds an intermediate that
    /// has no local out-edges for hop-2. The frontier entry must record
    /// `triple_idx == 1` and the partial_row carrying hop-1's binding.
    ///
    /// Graph: root --EDGE--> mid (mid has zero out-edges)
    /// Query: MATCH (x)-[:EDGE]->(y)-[:EDGE]->(z) WHERE x = 'root'
    /// "mid" is marked remote so the frontier is emitted for it.
    #[test]
    fn frontier_triple_idx_and_partial_row_for_multi_hop() {
        let (csr, store, _dir) = make_csr(&[("root", "EDGE", "mid")]);
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query =
            parse("MATCH (x)-[:EDGE]->(y)-[:EDGE]->(z) WHERE x = 'root' RETURN x, y, z").unwrap();
        // "mid" is the hop-2 source with zero out-edges; mark it remote.
        let is_remote: &dyn Fn(&str) -> bool = &|name| name == "mid";
        let outcome = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: Some(is_remote),
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap();

        assert!(outcome.rows.is_empty());
        assert_eq!(
            outcome.unresolved_frontier.len(),
            1,
            "expected exactly one frontier entry for hop-2"
        );
        let entry = &outcome.unresolved_frontier[0];
        assert_eq!(entry.node_name, "mid", "frontier source should be mid");
        assert_eq!(entry.triple_idx, 1, "second triple is index 1");
        // partial_row must carry x=root and y=mid (hop-1 bindings).
        assert_eq!(entry.partial_row.get("x").map(String::as_str), Some("root"));
        assert_eq!(entry.partial_row.get("y").map(String::as_str), Some("mid"));
    }

    // ── Property-predicate filtering (the silent-wrong-results bug fix) ────────

    /// Store a node-property document in collection `"col"` (matching
    /// `make_csr`'s `(DatabaseId::DEFAULT, TenantId::new(1))` scope), keyed by
    /// `StorageKey::for_surrogate(surrogate)` — the REAL document key. A graph
    /// node and its same-pk document share one surrogate, so the caller
    /// assigns the same surrogate to the node in the CSR via
    /// `set_node_surrogate`.
    fn put_node_doc(
        sparse: &SparseEngine,
        surrogate: nodedb_types::Surrogate,
        doc: nodedb_types::Value,
    ) {
        use nodedb_types::StorageKey;
        let bytes = nodedb_types::value_to_msgpack(&doc).unwrap();
        sparse
            .put(0, 1, "col", &StorageKey::for_surrogate(surrogate), &bytes)
            .unwrap();
    }

    fn obj(pairs: &[(&str, nodedb_types::Value)]) -> nodedb_types::Value {
        let mut map = std::collections::HashMap::new();
        for (k, v) in pairs {
            map.insert(k.to_string(), v.clone());
        }
        nodedb_types::Value::Object(map)
    }

    /// `WHERE a.age = '30'` now FILTERS by the node's stored document instead
    /// of matching every row (the stub bug). alice(age=30) and bob(age=25) both
    /// have a KNOWS edge to carol/dave; only alice survives the predicate.
    #[test]
    fn property_equals_filters_by_stored_document() {
        let (mut csr, store, _dir) =
            make_csr(&[("alice", "KNOWS", "carol"), ("bob", "KNOWS", "dave")]);
        let (sparse, _sdir) = make_sparse();
        // alice/bob share their surrogate with their stored document (the real
        // keying): node → surrogate → StorageKey → sparse.
        csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
        csr.set_node_surrogate("bob", nodedb_types::Surrogate::new(2));
        let props = props_for(&sparse, &csr);
        put_node_doc(
            &sparse,
            nodedb_types::Surrogate::new(1),
            obj(&[("age", nodedb_types::Value::Integer(30))]),
        );
        put_node_doc(
            &sparse,
            nodedb_types::Surrogate::new(2),
            obj(&[("age", nodedb_types::Value::Integer(25))]),
        );

        let query =
            parse("MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a.age = '30' RETURN a, b").unwrap();
        assert_eq!(query.collection.as_deref(), Some("col"));

        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;

        assert_eq!(rows.len(), 1, "only alice(age=30) matches");
        assert_eq!(rows[0]["a"], "alice");
        assert_eq!(rows[0]["b"], "carol");
    }

    /// A predicate matching no node's stored value returns zero rows (proving
    /// the predicate truly evaluates, not a no-op pass).
    #[test]
    fn property_equals_no_match_returns_empty() {
        let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "carol")]);
        let (sparse, _sdir) = make_sparse();
        csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
        let props = props_for(&sparse, &csr);
        put_node_doc(
            &sparse,
            nodedb_types::Surrogate::new(1),
            obj(&[("age", nodedb_types::Value::Integer(30))]),
        );

        let query =
            parse("MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a.age = '99' RETURN a, b").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert!(rows.is_empty(), "no node has age=99; got {rows:?}");
    }

    /// A node with NO stored document cannot satisfy a property predicate and
    /// is excluded (`Ok(false)`), even though it has a matching edge.
    #[test]
    fn property_predicate_excludes_node_without_document() {
        let (mut csr, store, _dir) =
            make_csr(&[("alice", "KNOWS", "carol"), ("bob", "KNOWS", "dave")]);
        let (sparse, _sdir) = make_sparse();
        // Both nodes have a surrogate, but only alice has a document stored at
        // its surrogate key; bob's surrogate resolves to no stored row → fetch
        // returns None → bob is excluded.
        csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
        csr.set_node_surrogate("bob", nodedb_types::Surrogate::new(2));
        let props = props_for(&sparse, &csr);
        put_node_doc(
            &sparse,
            nodedb_types::Surrogate::new(1),
            obj(&[("age", nodedb_types::Value::Integer(30))]),
        );

        let query =
            parse("MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a.age >= '20' RETURN a, b").unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 1, "bob has no document so is excluded");
        assert_eq!(rows[0]["a"], "alice");
    }

    /// A property predicate with NO `IN '<collection>'` clause is unresolvable
    /// and must return a typed `BadRequest` error — never silently pass/drop.
    #[test]
    fn property_predicate_without_collection_is_bad_request() {
        let (csr, store, _dir) = make_csr(&[("alice", "KNOWS", "carol")]);
        let (sparse, _sdir) = make_sparse();
        // No collection on the lookup (mirrors a query without `IN '...'`).
        // The BadRequest fires before any fetch, so no surrogate is needed.
        let props = PropertyLookup {
            sparse: &sparse,
            csr: &csr,
            database_id: 0,
            tenant_id: 1,
            collection: None,
        };
        put_node_doc(
            &sparse,
            nodedb_types::Surrogate::new(1),
            obj(&[("age", nodedb_types::Value::Integer(30))]),
        );

        let query = parse("MATCH (a)-[:KNOWS]->(b) WHERE a.age = '30' RETURN a, b").unwrap();
        assert_eq!(query.collection, None);

        let result = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        );
        // `MatchOutcome` (the Ok type) is not `Debug`, so match on the Result
        // directly rather than `unwrap_err()`.
        assert!(
            matches!(result, Err(crate::Error::BadRequest { .. })),
            "expected BadRequest error for a property predicate with no IN collection"
        );
    }

    /// Direct `check_property` coverage for each `ComparisonOp` plus the
    /// missing-field and missing-document branches, against a real
    /// `SparseEngine`.
    #[test]
    fn check_property_ops_against_real_sparse_engine() {
        let (mut csr, _store, _dir) = make_csr(&[("alice", "KNOWS", "carol")]);
        let (sparse, _sdir) = make_sparse();
        // alice has a surrogate + document; "ghost" is unknown to the CSR so its
        // surrogate resolves to None → missing-document branch.
        csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
        put_node_doc(
            &sparse,
            nodedb_types::Surrogate::new(1),
            obj(&[
                ("age", nodedb_types::Value::Integer(30)),
                ("name", nodedb_types::Value::String("alice".into())),
            ]),
        );
        let props = props_for(&sparse, &csr);

        // Eq / Neq (numeric coercion: stored Integer(30) vs literal "30").
        assert!(check(&props, "alice", "age", &ComparisonOp::Eq, "30").unwrap());
        assert!(!check(&props, "alice", "age", &ComparisonOp::Eq, "31").unwrap());
        assert!(check(&props, "alice", "age", &ComparisonOp::Neq, "31").unwrap());
        // Ordering.
        assert!(check(&props, "alice", "age", &ComparisonOp::Lt, "40").unwrap());
        assert!(!check(&props, "alice", "age", &ComparisonOp::Lt, "30").unwrap());
        assert!(check(&props, "alice", "age", &ComparisonOp::Lte, "30").unwrap());
        assert!(check(&props, "alice", "age", &ComparisonOp::Gt, "20").unwrap());
        assert!(check(&props, "alice", "age", &ComparisonOp::Gte, "30").unwrap());
        // String field equality.
        assert!(check(&props, "alice", "name", &ComparisonOp::Eq, "alice").unwrap());
        // Missing field → false.
        assert!(!check(&props, "alice", "missing", &ComparisonOp::Eq, "x").unwrap());
        // Missing document → false.
        assert!(!check(&props, "ghost", "age", &ComparisonOp::Eq, "30").unwrap());
    }

    // ── Property projection (`RETURN a.field`) ────────────────────────────────

    /// `RETURN a.name, a.age` now projects the node's STORED property values
    /// instead of the old `"NULL"` stub. alice's document `{name, age}` resolves
    /// to the string forms "Alice" / "30" via the canonical value display
    /// convention. The non-dotted `b` column still projects the node identity.
    #[test]
    fn property_projection_returns_stored_values() {
        let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
        let (sparse, _sdir) = make_sparse();
        csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
        let props = props_for(&sparse, &csr);
        put_node_doc(
            &sparse,
            nodedb_types::Surrogate::new(1),
            obj(&[
                ("name", nodedb_types::Value::String("Alice".into())),
                ("age", nodedb_types::Value::Integer(30)),
            ]),
        );

        let query =
            parse("MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a = 'alice' RETURN a.name, a.age, b")
                .unwrap();

        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a.name"], "Alice", "string property projected");
        assert_eq!(rows[0]["a.age"], "30", "integer property stringified");
        assert_eq!(
            rows[0]["b"], "bob",
            "non-dotted identity projection unchanged"
        );
    }

    /// A node WITHOUT a stored document projects `"NULL"` for a property column
    /// (SQL projection: missing row → NULL), not an error.
    #[test]
    fn property_projection_no_document_is_null() {
        let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
        let (sparse, _sdir) = make_sparse();
        // alice has a surrogate but NO document stored at its key → fetch None.
        csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
        let props = props_for(&sparse, &csr);

        let query =
            parse("MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a = 'alice' RETURN a.name").unwrap();

        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a.name"], "NULL", "absent document → NULL");
    }

    /// A property column whose `field` is absent from an EXISTING document
    /// projects `"NULL"` (missing field → NULL), not an error.
    #[test]
    fn property_projection_missing_field_is_null() {
        let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
        let (sparse, _sdir) = make_sparse();
        csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
        let props = props_for(&sparse, &csr);
        put_node_doc(
            &sparse,
            nodedb_types::Surrogate::new(1),
            obj(&[("name", nodedb_types::Value::String("Alice".into()))]),
        );

        let query =
            parse("MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a = 'alice' RETURN a.age").unwrap();

        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a.age"], "NULL", "missing field → NULL");
    }

    /// Property PROJECTION with no `IN '<collection>'` clause is unresolvable and
    /// must return a typed `BadRequest` — the same rule as the predicate path,
    /// never a silent `"NULL"`.
    #[test]
    fn property_projection_without_collection_is_bad_request() {
        let (csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
        let (sparse, _sdir) = make_sparse();
        // BadRequest fires before any fetch, so no surrogate is needed.
        let props = PropertyLookup {
            sparse: &sparse,
            csr: &csr,
            database_id: 0,
            tenant_id: 1,
            collection: None,
        };
        put_node_doc(
            &sparse,
            nodedb_types::Surrogate::new(1),
            obj(&[("name", nodedb_types::Value::String("Alice".into()))]),
        );

        let query = parse("MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a.name").unwrap();
        assert_eq!(query.collection, None);

        let result = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        );
        assert!(
            matches!(result, Err(crate::Error::BadRequest { .. })),
            "expected BadRequest for a property projection with no IN collection"
        );
    }

    /// Aliased property projection (`RETURN a.name AS who`) keys the output by
    /// the alias, and a plain `RETURN a` still projects the node identity.
    #[test]
    fn property_projection_alias_and_identity() {
        let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
        let (sparse, _sdir) = make_sparse();
        csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
        let props = props_for(&sparse, &csr);
        put_node_doc(
            &sparse,
            nodedb_types::Surrogate::new(1),
            obj(&[("name", nodedb_types::Value::String("Alice".into()))]),
        );

        let query =
            parse("MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a = 'alice' RETURN a.name AS who, a")
                .unwrap();

        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: expansion::VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["who"], "Alice", "property keyed by alias");
        assert_eq!(rows[0]["a"], "alice", "non-dotted identity unchanged");
    }
}
