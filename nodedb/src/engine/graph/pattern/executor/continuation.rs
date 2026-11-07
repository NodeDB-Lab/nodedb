// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard MATCH resume — the executor RESUME entry-point.
//!
//! When a shard cannot expand a bound source node (its edges are homed on
//! another shard) it emits an `UnresolvedExpansion` frontier entry. The
//! Control Plane dispatches a *continuation* to the owning shard, which
//! resumes the SAME pattern from where the originating shard left off via
//! [`execute_continuation`].

use super::super::ast::{MatchQuery, PatternChain};
use super::core::{MatchExecCtx, bind_node, binding_compatible, execute_triple};
use super::expansion::{VarLenCaps, VarLenCursor, VarLenPattern, resume_variable_length};
use super::overlay_expand;
use super::predicates;
use super::predicates::PropertyLookup;
use super::types::{BindingRow, ContinuationSeed, ExecutionState, MatchOutcome, VarLenResume};
use super::varlen_named::{self, NameOrId};
use crate::engine::graph::csr::{CsrIndex, GraphOverlayDelta};
use crate::engine::graph::edge_store::EdgeStore;

/// Chain-execution core: expand a pattern chain's triples starting at
/// `start_idx`, threading `initial_rows` through each remaining triple.
///
/// This is the single source of truth for triple iteration. The from-scratch
/// path calls it with `start_idx = 0` and a single seed row (via
/// `execute_chain`); the cross-shard resume path calls it with
/// `start_idx = resume_triple_idx` and a seed row whose first
/// `resume_triple_idx` triples are already bound (via [`execute_continuation`]).
///
/// `triple_idx` passed to [`execute_triple`] is the absolute 0-based index of
/// the triple WITHIN ITS CHAIN — identical to the index recorded in any
/// emitted `UnresolvedExpansion`. Skipped triples `[0, start_idx)` are assumed
/// already satisfied by the bindings present in `initial_rows`.
pub(super) fn run_chain_from(
    chain: &PatternChain,
    start_idx: usize,
    initial_rows: Vec<BindingRow>,
    csr: &CsrIndex,
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    overlay: Option<&GraphOverlayDelta>,
) -> Result<Vec<BindingRow>, crate::Error> {
    let mut rows = initial_rows;

    for (triple_idx, triple) in chain.triples.iter().enumerate().skip(start_idx) {
        let mut next_rows = Vec::new();
        for row in &rows {
            next_rows.extend(execute_triple(
                triple,
                triple_idx,
                csr,
                row,
                state,
                frontier_bitmap,
                overlay,
            )?);
        }
        rows = next_rows;
        if rows.is_empty() {
            break;
        }
    }

    Ok(rows)
}

/// Apply the query tail — WHERE predicates, LIMIT, RETURN projection, and
/// DISTINCT — to a fully-expanded set of binding rows.
///
/// This is the shared post-chain finalization step. Both the from-scratch
/// path (`execute_query`) and the cross-shard resume path
/// ([`execute_continuation`]) funnel their expanded rows through here so the
/// tail semantics are identical regardless of where expansion started.
pub(super) fn finalize_rows(
    query: &MatchQuery,
    mut rows: Vec<BindingRow>,
    csr: &CsrIndex,
    edge_store: &EdgeStore,
    varlen_caps: VarLenCaps,
    props: &PropertyLookup<'_>,
    overlay: Option<&GraphOverlayDelta>,
) -> Result<Vec<BindingRow>, crate::Error> {
    for predicate in &query.where_predicates {
        rows = predicates::apply_predicate(
            &rows,
            predicate,
            csr,
            edge_store,
            varlen_caps,
            props,
            overlay,
        )?;
    }

    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }

    if !query.return_columns.is_empty() {
        rows = predicates::project_columns(&rows, &query.return_columns, props)?;
    }

    if query.distinct {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|row| {
            // Build a sorted-key representation so that two BindingRows with
            // the same entries but different HashMap iteration orders are
            // treated as identical. `format!("{row:?}")` on a HashMap is
            // non-deterministic in key order, which would miss duplicates.
            let mut pairs: Vec<(&String, &String)> = row.iter().collect();
            pairs.sort_unstable_by_key(|(k, _)| *k);
            let key = format!("{pairs:?}");
            seen.insert(key)
        });
    }

    Ok(rows)
}

/// Resume a MATCH pattern on THIS shard's CSR starting at `seed.triple_idx`.
///
/// # Why this does NOT optimize
///
/// [`super::execute`] reorders the query's triples by per-shard selectivity
/// (using THIS CSR's edge counts) before running. A continuation MUST NOT
/// re-optimize: `seed.triple_idx` is an index into the **originating
/// shard's already-optimized triple order**. Re-optimizing here against a
/// different CSR's edge counts could yield a different order, so
/// `seed.triple_idx` would point at the wrong triple. The caller therefore
/// passes the originating shard's already-optimized `query` AS GIVEN, and this
/// function runs it verbatim — the optimizer is never invoked on the resume
/// path.
///
/// # How it resumes
///
/// `seed.seed_row` carries all bindings accumulated by the originating shard up
/// to (and including) the source node being resumed from — i.e. the bindings
/// for triples `[0, seed.triple_idx)` are already present. This function seeds
/// the row set as `vec![seed.seed_row]` and runs [`run_chain_from`] starting at
/// `seed.triple_idx`, skipping the already-satisfied prefix triples. The
/// query tail (WHERE / LIMIT / RETURN / DISTINCT) is then applied via
/// [`finalize_rows`], identically to the from-scratch path.
///
/// # `seed.triple_idx` semantics
///
/// `seed.triple_idx` is the index of the triple **within its pattern
/// chain** — the exact value the originating shard recorded in
/// `UnresolvedExpansion::triple_idx` (produced by `execute_chain`'s
/// `enumerate`). This is a within-chain index, not a global flattening across
/// clauses; the single-clause MATCH case (the dominant case) has exactly one
/// chain, so the within-chain index and the pattern index coincide.
///
/// # Multi-clause limitation
///
/// Resuming mid-pattern is only well-defined for a single MATCH clause with a
/// single pattern chain (`seed.triple_idx` indexes that chain). A query with
/// multiple clauses (e.g. `OPTIONAL MATCH`) or multiple comma-separated
/// patterns in the resumed clause makes mid-pattern resume ambiguous — there
/// is no single chain that `seed.triple_idx` unambiguously indexes. Rather
/// than silently mis-handle it (which would produce wrong results), this
/// function returns a typed `BadRequest` error for that case. The frontier is
/// only ever emitted from the single-chain expansion path today, so this
/// guard is defensive against a future multi-clause caller.
pub fn execute_continuation<'a>(
    query: &MatchQuery,
    ctx: MatchExecCtx<'a>,
    seed: ContinuationSeed,
) -> Result<MatchOutcome, crate::Error> {
    let MatchExecCtx {
        csr,
        edge_store,
        frontier_bitmap,
        is_remote_node,
        varlen_caps,
        props,
        overlay,
    } = ctx;
    // Mid-pattern resume is only unambiguous for a single clause holding a
    // single pattern chain. Reject anything else with a typed error rather
    // than guessing which chain `seed.triple_idx` refers to.
    let chain = match query.clauses.as_slice() {
        [clause] if clause.patterns.len() == 1 => &clause.patterns[0],
        _ => {
            return Err(crate::Error::BadRequest {
                detail: "cross-shard MATCH continuation is only supported for a single \
                         MATCH clause with a single pattern chain; multi-clause / \
                         multi-pattern continuation is not yet supported"
                    .to_string(),
            });
        }
    };

    if seed.triple_idx > chain.triples.len() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "cross-shard MATCH continuation resume_triple_idx {} \
                 exceeds chain length {}",
                seed.triple_idx,
                chain.triples.len()
            ),
        });
    }

    let mut state = ExecutionState::new(is_remote_node, varlen_caps);
    state.collection_filter =
        super::expansion::query_collection_filter(query, props.collection, csr);

    // Resume the chain from the originating shard's stopping point. The seed
    // row already carries the bindings for triples [0, seed.triple_idx).
    let rows = run_chain_from(
        chain,
        seed.triple_idx,
        vec![seed.seed_row],
        csr,
        &mut state,
        frontier_bitmap,
        overlay,
    )?;

    let rows = finalize_rows(
        query,
        rows,
        csr,
        edge_store,
        state.varlen_caps,
        props,
        overlay,
    )?;

    Ok(MatchOutcome {
        rows,
        truncation: state.varlen_resume,
        unresolved_frontier: state.frontier,
    })
}

/// Resume a TRUNCATED variable-length expansion from a [`VarLenResume`] cursor
/// and run the rest of the pattern on the resumed rows.
///
/// # When this fires
///
/// A `MATCH (a)-[*min..max]->(b)-...` whose `*min..max` expansion hit a hard cap
/// on the originating shard surfaces a [`VarLenResume`] in
/// [`MatchOutcome::truncation`]. The Control-Plane coordinator carries that
/// cursor in `GraphOp::MatchVarLenResume` and dispatches it back so the BFS
/// continues from exactly where it stopped — no row is silently dropped.
///
/// # How it resumes (vs [`execute_continuation`])
///
/// [`execute_continuation`] resumes at a TRIPLE boundary (the prior shard fully
/// finished triple `< resume_triple_idx`). This function resumes MID-triple: the
/// truncated triple `resume.triple_idx` is a variable-length edge whose BFS was
/// interrupted at `resume.depth` with `resume.frontier` still un-expanded. It:
///
/// 1. Rebuilds the `VarLenPattern` for that triple from the (already-optimized)
///    `query` chain — verbatim, NEVER re-optimized (same contract as
///    [`execute_continuation`]: `resume.triple_idx` indexes the originating
///    shard's order).
/// 2. Continues the BFS via [`resume_variable_length`] from `resume.frontier` /
///    `resume.depth`, honoring the same `VarLenCaps`. If this resume ALSO hits a
///    cap, the fresh [`VarLenCursor`] becomes a NEW [`VarLenResume`] in the
///    returned outcome so paging continues across multiple rounds.
/// 3. Binds the resumed destinations into rows exactly as the from-scratch
///    varlen branch in `execute_triple` does (same `binding_compatible` /
///    `bind_node` / edge-path logic), then runs the REMAINING triples
///    (`resume.triple_idx + 1 ..`) through [`run_chain_from`] and applies the
///    query tail via [`finalize_rows`] — identical downstream pipeline.
///
/// Per the cross-shard contract there is no `visited` carry-over: a node
/// re-reached on resume yields a duplicate row the coordinator collapses, never
/// a skipped or mis-depthed one.
pub fn execute_varlen_resume<'a>(
    query: &MatchQuery,
    ctx: MatchExecCtx<'a>,
    resume: VarLenResume,
) -> Result<MatchOutcome, crate::Error> {
    let MatchExecCtx {
        csr,
        edge_store,
        frontier_bitmap,
        is_remote_node,
        varlen_caps,
        props,
        overlay,
    } = ctx;
    // Same single-chain restriction as `execute_continuation`: mid-pattern
    // resume is only unambiguous for a single MATCH clause with one chain.
    let chain = match query.clauses.as_slice() {
        [clause] if clause.patterns.len() == 1 => &clause.patterns[0],
        _ => {
            return Err(crate::Error::BadRequest {
                detail: "cross-shard variable-length MATCH resume is only supported for a single \
                         MATCH clause with a single pattern chain"
                    .to_string(),
            });
        }
    };

    let triple = chain
        .triples
        .get(resume.triple_idx)
        .ok_or_else(|| crate::Error::BadRequest {
            detail: format!(
                "variable-length MATCH resume triple_idx {} exceeds chain length {}",
                resume.triple_idx,
                chain.triples.len()
            ),
        })?;

    if !triple.edge.is_variable_length() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "variable-length MATCH resume targets triple {} which is not a \
                 variable-length edge",
                resume.triple_idx
            ),
        });
    }

    let mut state = ExecutionState::new(is_remote_node, varlen_caps);
    state.collection_filter =
        super::expansion::query_collection_filter(query, props.collection, csr);

    // Rebuild the pattern shape for the truncated triple verbatim (no
    // re-optimization). `want_path` matches the from-scratch branch in
    // `execute_triple`.
    let want_path = triple.edge.name.is_some();
    let pattern = VarLenPattern {
        label_filter: triple.edge.edge_type.as_deref(),
        direction: triple.edge.direction.to_csr_direction(),
        min_hops: triple.edge.min_hops,
        max_hops: triple.edge.max_hops,
        want_path,
        collection_filter: state.collection_filter,
    };

    // Continue the BFS from the carried cursor. A fresh cap hit here records a
    // new resume cursor so multi-round paging can continue.
    let cursor = VarLenCursor {
        frontier: resume.frontier,
        depth: resume.depth,
    };
    let expansion = resume_variable_length(csr, &cursor, &pattern, varlen_caps, overlay);
    if let Some(next_cursor) = expansion.cursor {
        state.record_truncation(VarLenResume {
            triple_idx: resume.triple_idx,
            source_row: resume.source_row.clone(),
            frontier: next_cursor.frontier,
            depth: next_cursor.depth,
        });
    }

    // Cross-boundary continuations from the RESUMED segment: a frontier node
    // reached here with zero local out-degree is shipped onward exactly as on
    // the from-scratch path. `source_row` already carries the anchor bindings.
    varlen_named::record_boundary_resumes(
        &mut state,
        resume.triple_idx,
        &resume.source_row,
        &expansion.boundary,
    );

    // Bind the resumed destinations onto the source row exactly as the
    // from-scratch varlen branch does, then carry them into the remaining
    // triples.
    let src_binding = &triple.src;
    let dst_binding = &triple.dst;
    let mut resumed_rows: Vec<BindingRow> = Vec::new();

    // The source id is already carried in `source_row`; bind_node is a no-op
    // when the variable is present, but keep the call for parity with
    // `execute_triple` (handles anonymous/unbound source shapes).
    let bind_source = |row: &mut BindingRow| {
        if let Some(src_name) = src_binding.name.as_deref()
            && let Some(src_value) = resume.source_row.get(src_name)
            && let Some(src_id) = csr.node_id_raw(src_value)
        {
            bind_node(row, src_binding, csr, src_id);
        }
    };

    for (dst_id, path) in expansion.results {
        if !binding_compatible(dst_binding, csr, &resume.source_row, dst_id) {
            continue;
        }
        let mut row = resume.source_row.clone();
        bind_source(&mut row);
        bind_node(&mut row, dst_binding, csr, dst_id);
        if let Some(ref edge_name) = triple.edge.name {
            row.insert(edge_name.clone(), path);
        }
        resumed_rows.push(row);
    }

    // Overlay destinations from a resumed name-keyed segment: a durable dst
    // binds by id, a staged-only dst binds by name.
    for (bound, path) in expansion.named_results {
        match bound {
            NameOrId::Id(dst_id) => {
                if !binding_compatible(dst_binding, csr, &resume.source_row, dst_id) {
                    continue;
                }
                let mut row = resume.source_row.clone();
                bind_source(&mut row);
                bind_node(&mut row, dst_binding, csr, dst_id);
                if let Some(ref edge_name) = triple.edge.name {
                    row.insert(edge_name.clone(), path);
                }
                resumed_rows.push(row);
            }
            NameOrId::Name(dst_name) => {
                if !overlay_expand::dst_compatible(dst_binding, csr, &resume.source_row, &dst_name)
                {
                    continue;
                }
                let mut row = resume.source_row.clone();
                bind_source(&mut row);
                overlay_expand::bind_name(&mut row, dst_binding, &dst_name);
                if let Some(ref edge_name) = triple.edge.name {
                    row.insert(edge_name.clone(), path);
                }
                resumed_rows.push(row);
            }
        }
    }

    // Run the REMAINING triples (after the truncated one) over the resumed rows,
    // identical to the normal downstream pipeline.
    let rows = run_chain_from(
        chain,
        resume.triple_idx + 1,
        resumed_rows,
        csr,
        &mut state,
        frontier_bitmap,
        overlay,
    )?;

    let rows = finalize_rows(
        query,
        rows,
        csr,
        edge_store,
        state.varlen_caps,
        props,
        overlay,
    )?;

    Ok(MatchOutcome {
        rows,
        truncation: state.varlen_resume,
        unresolved_frontier: state.frontier,
    })
}

#[cfg(test)]
mod tests {
    use super::super::core::MatchExecCtx;
    use super::super::core::execute;
    use super::super::core::triple::tests::{make_csr, make_sparse, props_for};
    use super::super::expansion::{VarLenCaps, VarLenPattern, expand_variable_length};
    use super::super::types::{BindingRow, ContinuationSeed, VarLenResume};
    use super::{execute_continuation, execute_varlen_resume};
    use crate::engine::graph::edge_store::Direction;

    /// `VarLenResume` round-trips through zerompk byte-for-byte. The resume
    /// cursor rides the SPSC bridge inside `GraphOp::MatchVarLenResume` as a
    /// MessagePack blob, so it MUST survive ser/de unchanged.
    #[test]
    fn varlen_resume_zerompk_round_trip() {
        let mut source_row = BindingRow::new();
        source_row.insert("a".to_string(), "n0".to_string());
        source_row.insert("x".to_string(), "anchor".to_string());
        let resume = VarLenResume {
            triple_idx: 2,
            source_row,
            frontier: vec![
                ("n3".to_string(), "n0->n1->n3".to_string()),
                ("n7".to_string(), "n0->n2->n7".to_string()),
                ("n11".to_string(), String::new()),
                ("n0".to_string(), "n0".to_string()),
            ],
            depth: 4,
        };

        let bytes = zerompk::to_msgpack_vec(&resume).expect("serialize VarLenResume");
        let decoded: VarLenResume =
            zerompk::from_msgpack(&bytes).expect("deserialize VarLenResume");
        assert_eq!(decoded, resume, "VarLenResume must round-trip via zerompk");
    }

    /// Plan/handler-level union-equivalence at the resume-orchestration level: a
    /// `MATCH (a)-[*1..6]->(b)` expansion that TRUNCATES at a low cap,
    /// then is RESUMED via [`execute_varlen_resume`] (the exact function the DP
    /// `MatchVarLenResume` handler calls), produces — unioned with the first-pass
    /// rows — the SAME `b` binding set as a single uncapped MATCH over the same
    /// graph. The cap is injected via `VarLenCaps`, NOT by lowering the prod const.
    #[test]
    fn varlen_resume_handler_union_equals_uncapped_match() {
        // Chain n0 -> n1 -> ... -> n6. `(a)-[*1..6]->(b)` from n0 reaches
        // {n1..n6}; a low results cap forces mid-expansion truncation.
        let edges: Vec<(String, String, String)> = (0..6)
            .map(|i| (format!("n{i}"), "l".to_string(), format!("n{}", i + 1)))
            .collect();
        let edge_refs: Vec<(&str, &str, &str)> = edges
            .iter()
            .map(|(s, l, d)| (s.as_str(), l.as_str(), d.as_str()))
            .collect();
        let (csr, store, _dir) = make_csr(&edge_refs);
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);

        let query = super::super::super::compiler::parse(
            "MATCH (a)-[:l*1..6]->(b) WHERE a = 'n0' RETURN a, b",
        )
        .unwrap();

        // Ground truth: single uncapped MATCH.
        let full_b: std::collections::HashSet<String> = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r["b"].clone())
        .collect();
        assert_eq!(full_b.len(), 6, "uncapped MATCH reaches n1..n6 from n0");

        // First pass: drive truncation directly via a low cap on the same
        // varlen expansion the executor uses, capturing the resume cursor.
        let src = csr.node_id_raw("n0").unwrap();
        let pat = VarLenPattern {
            label_filter: Some("l"),
            direction: Direction::Out,
            min_hops: 1,
            max_hops: 6,
            want_path: false,
            collection_filter: super::super::expansion::CollectionFilter::Unscoped,
        };
        let caps = VarLenCaps {
            max_results: 2,
            max_frontier: usize::MAX,
        };
        let first = expand_variable_length(&csr, src, &pat, caps, None);
        let cursor = first.cursor.clone().expect("low cap must truncate");

        let mut source_row = BindingRow::new();
        source_row.insert("a".to_string(), "n0".to_string());

        // First-pass rows' `b` bindings (what the originating shard emitted).
        let mut union_b: std::collections::HashSet<String> = first
            .results
            .iter()
            .map(|(dst, _)| csr.node_name_raw(*dst).to_string())
            .collect();

        // Resume — possibly across multiple rounds — through the handler-level
        // entry point, unioning each round's `b` bindings.
        let mut next = Some(VarLenResume {
            triple_idx: 0,
            source_row: source_row.clone(),
            frontier: cursor.frontier,
            depth: cursor.depth,
        });
        while let Some(resume) = next.take() {
            let outcome = execute_varlen_resume(
                &query,
                MatchExecCtx {
                    csr: &csr,
                    edge_store: &store,
                    frontier_bitmap: None,
                    is_remote_node: None,
                    varlen_caps: VarLenCaps::default(),
                    props: &props,
                    overlay: None,
                },
                resume,
            )
            .unwrap();
            for row in &outcome.rows {
                assert_eq!(row["a"], "n0", "source binding carried through resume");
                union_b.insert(row["b"].clone());
            }
            next = outcome.truncation.into_iter().next().map(|t| VarLenResume {
                triple_idx: 0,
                source_row: source_row.clone(),
                frontier: t.frontier,
                depth: t.depth,
            });
        }

        assert_eq!(
            union_b, full_b,
            "first-pass ∪ resumed `b` bindings must equal the uncapped MATCH set"
        );
    }

    /// Resume produces the correct tail. Graph `(x)-[:E]->(y)-[:E]->(z)` with
    /// `root -E-> mid -E-> leaf`. Resume at triple_idx 1 with the seed bindings
    /// `{x:root, y:mid}` (i.e. triple 0 already satisfied on the originating
    /// shard). `mid` HAS a local out-edge `mid->leaf`, so the tail resolves to
    /// `z = leaf` and the row carries through the seed bindings.
    #[test]
    fn continuation_resumes_tail_with_seed_bindings() {
        let (csr, store, _dir) = make_csr(&[("root", "E", "mid"), ("mid", "E", "leaf")]);
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query =
            super::super::super::compiler::parse("MATCH (x)-[:E]->(y)-[:E]->(z) RETURN x, y, z")
                .unwrap();

        let mut seed = BindingRow::new();
        seed.insert("x".to_string(), "root".to_string());
        seed.insert("y".to_string(), "mid".to_string());

        let outcome = execute_continuation(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
            ContinuationSeed {
                triple_idx: 1,
                seed_row: seed,
            },
        )
        .unwrap();

        assert_eq!(outcome.rows.len(), 1, "expected exactly one tail row");
        assert_eq!(
            outcome.rows[0]["x"], "root",
            "seed binding x carried through"
        );
        assert_eq!(
            outcome.rows[0]["y"], "mid",
            "seed binding y carried through"
        );
        assert_eq!(outcome.rows[0]["z"], "leaf", "tail resolved z=leaf");
        assert!(outcome.unresolved_frontier.is_empty());
    }

    /// Resume with no matching tail edge yields empty rows. `mid` has NO local
    /// out-edge, so resuming triple 1 from `{x:root, y:mid}` produces nothing.
    #[test]
    fn continuation_no_matching_tail_edge_is_empty() {
        let (csr, store, _dir) = make_csr(&[("root", "E", "mid")]);
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query =
            super::super::super::compiler::parse("MATCH (x)-[:E]->(y)-[:E]->(z) RETURN x, y, z")
                .unwrap();

        let mut seed = BindingRow::new();
        seed.insert("x".to_string(), "root".to_string());
        seed.insert("y".to_string(), "mid".to_string());

        let outcome = execute_continuation(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
            ContinuationSeed {
                triple_idx: 1,
                seed_row: seed,
            },
        )
        .unwrap();

        assert!(
            outcome.rows.is_empty(),
            "mid has no local out-edge; tail must be empty, got {:?}",
            outcome.rows
        );
    }

    /// `execute()` (the from-scratch path) is unchanged: a full query still
    /// returns the same rows. Sanity that the chain-core refactor preserved
    /// from-scratch behaviour.
    #[test]
    fn full_execute_unchanged_after_refactor() {
        let (csr, store, _dir) = make_csr(&[("root", "E", "mid"), ("mid", "E", "leaf")]);
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let query = super::super::super::compiler::parse(
            "MATCH (x)-[:E]->(y)-[:E]->(z) WHERE x = 'root' RETURN x, y, z",
        )
        .unwrap();
        let rows = execute(
            &query,
            MatchExecCtx {
                csr: &csr,
                edge_store: &store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: VarLenCaps::default(),
                props: &props,
                overlay: None,
            },
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["x"], "root");
        assert_eq!(rows[0]["y"], "mid");
        assert_eq!(rows[0]["z"], "leaf");
    }
}
