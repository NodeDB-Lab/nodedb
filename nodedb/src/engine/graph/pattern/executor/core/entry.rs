//! Top-level MATCH entry points: optimize-then-execute, and the optimized-only path.

use std::collections::HashMap;

use crate::engine::graph::pattern::ast::MatchQuery;
use crate::engine::graph::pattern::executor::types::{BindingRow, ExecutionState, MatchOutcome};
use crate::engine::graph::pattern::executor::{continuation, expansion};
use crate::engine::graph::pattern::optimizer;

use super::clause::execute_clause;
use super::ctx::MatchExecCtx;
use super::join::left_join_rows;

/// Execute a MATCH query on a CSR index and edge store.
///
/// Applies join order optimization before execution: triples within each
/// PatternChain are reordered by selectivity (lowest edge count first,
/// bound variables preferred).
///
/// `frontier_bitmap`: when `Some`, only nodes whose surrogate is present in the
/// bitmap are eligible as pattern anchors. Bound variables (already resolved
/// from a prior binding row) bypass the bitmap check — only free-variable
/// anchor enumeration is restricted.
///
/// `is_remote_node`: when `Some(pred)`, `pred(node_name)` returns `true` for
/// nodes homed on a remote shard. Only nodes that (a) were reached via a
/// **bound** source variable (resolved from `input_row`, not free-ranged),
/// AND (b) satisfy this predicate, AND (c) have zero raw directional
/// adjacency, are added to `unresolved_frontier`.  Free-ranging anchors never
/// emit, even when the predicate and degree conditions hold, because each
/// shard's own pass covers all its local nodes.
/// Pass `None` (the production default on a fully-local CSR) to guarantee
/// an always-empty frontier, preserving byte-identical single-node behaviour.
///
/// `overlay`: when `Some(delta)` and the delta is non-empty, the query runs
/// inside a transaction and each fixed-hop triple observes the transaction's
/// own staged edge writes/deletes (read-your-own-writes) via the name-keyed
/// merge in [`super::overlay_expand`]. `None` (or an empty delta) is the
/// autocommit path and is byte-identical to committed-CSR-only execution.
pub fn execute<'a>(
    query: &MatchQuery,
    ctx: MatchExecCtx<'a>,
) -> Result<MatchOutcome, crate::Error> {
    // Optimize query before execution (reorder triples by selectivity). The
    // optimizer only REORDERS triples within a chain (it never drops one), and
    // a staged-only edge label has zero CSR edges so it scores as most
    // selective and simply sorts first — every triple is still visited, so a
    // staged edge/node cannot be pruned out of the plan.
    let mut optimized = query.clone();
    optimizer::optimize(&mut optimized, ctx.csr);
    execute_query(&optimized, ctx)
}

/// Execute a pre-optimized MATCH query (internal, skip optimizer).
fn execute_query<'a>(
    query: &MatchQuery,
    ctx: MatchExecCtx<'a>,
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
    let mut rows: Vec<BindingRow> = vec![HashMap::new()];
    let mut state = ExecutionState::new(is_remote_node, varlen_caps);
    // Resolve the `IN '<collection>'` scoping once against this partition's
    // collection interning; every edge expansion in this execution is filtered
    // by it so a collection-scoped MATCH never traverses another collection's
    // edges (they share one CSR partition).
    state.collection_filter = expansion::query_collection_filter(query, props.collection, csr);

    for clause in &query.clauses {
        let clause_rows = execute_clause(clause, csr, &rows, &mut state, frontier_bitmap, overlay)?;
        if clause.optional {
            rows = left_join_rows(&rows, &clause_rows, clause);
        } else {
            rows = clause_rows;
        }
    }

    let rows = continuation::finalize_rows(
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
