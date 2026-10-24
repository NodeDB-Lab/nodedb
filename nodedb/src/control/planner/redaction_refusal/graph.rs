// SPDX-License-Identifier: BUSL-1.1

//! Refusal of graph reads that column redaction cannot cover.
//!
//! A traversal or pattern match returns graph topology, not row bodies —
//! no columns to rewrite, but it can disclose data shape or probe a
//! redacted value via `WHERE`. Both are refused while a rule exists,
//! mirroring `rls_injection::plan`.

use nodedb_physical::physical_plan::GraphOp;

use super::lookup::RefusalCtx;

pub(super) fn refuse_graph_op(op: &GraphOp, ctx: &RefusalCtx<'_>) -> crate::Result<()> {
    match op {
        // `None` is a tree-index walk scoped by edge label, no collection
        // mapping — no policy to consult, same as RLS.
        GraphOp::Hop { collection, .. }
        | GraphOp::Neighbors { collection, .. }
        | GraphOp::NeighborsMulti { collection, .. }
        | GraphOp::Path { collection, .. }
        | GraphOp::Subgraph { collection, .. } => match collection.as_ref() {
            Some(collection) => refuse_traversal(ctx, collection),
            None => Ok(()),
        },

        // The bitemporal neighbor lookup always names its collection: the
        // versioned edge key layout is collection-scoped.
        GraphOp::TemporalNeighbors { collection, .. } => refuse_traversal(ctx, collection),

        // A pattern match names its collection inside the serialized query
        // (the `IN '<collection>'` clause), not on the plan node.
        GraphOp::Match { query, .. }
        | GraphOp::MatchContinuation { query, .. }
        | GraphOp::MatchVarLenResume { query, .. } => refuse_match(ctx, query),

        // Writes carry no read result; algorithm/superstep/stats ops return
        // whole-graph scalars, not columns; RAG fusion's document rows are
        // rewritten by the result-path redaction hook like any scan.
        GraphOp::EdgePut { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDelete { .. }
        | GraphOp::EdgeDeleteBatch { .. }
        | GraphOp::ResolveEdgeDelete(_)
        | GraphOp::RagFusion { .. }
        | GraphOp::Algo { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        | GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. }
        // A delete's planner reads which ids are stored. It returns no
        // column to the client.
        | GraphOp::NodePresenceRead { .. }
        | GraphOp::Stats { .. } => Ok(()),
    }
}

pub(super) fn refuse_traversal(
    ctx: &RefusalCtx<'_>,
    collection: &nodedb_types::QualifiedCollection,
) -> crate::Result<()> {
    if collection.as_str().is_empty() || !ctx.collection_is_redacted(collection.as_str()) {
        return Ok(());
    }
    Err(ctx.refusal(format!(
        "redaction policies on '{collection}' are not supported with graph traversal: a \
         traversal returns graph topology, which column redaction cannot be applied to"
    )))
}

/// Refuse a pattern match whose target collection carries a rule. The
/// collection lives in the serialized `MatchQuery`, decoded here to keep
/// the refusal narrow. A query naming no collection, or one that fails to
/// decode, falls back to the tenant-wide question: refused only when this
/// identity holds a redaction rule somewhere.
pub(super) fn refuse_match(ctx: &RefusalCtx<'_>, query: &[u8]) -> crate::Result<()> {
    let decoded: Result<crate::engine::graph::pattern::ast::MatchQuery, _> =
        zerompk::from_msgpack(query);
    refuse_match_scoped(
        ctx,
        decoded.ok().and_then(|query| query.collection).as_deref(),
    )
}

/// Refuse a pattern match already known to be scoped (or not) to `collection`.
///
/// Shares the fail-closed fallback with [`refuse_match`] for a caller that
/// already holds the decoded `MatchQuery` and will otherwise have to
/// re-serialize it only to decode it back here.
pub(super) fn refuse_match_scoped(
    ctx: &RefusalCtx<'_>,
    collection: Option<&str>,
) -> crate::Result<()> {
    match collection {
        Some(collection) => refuse_traversal(
            ctx,
            &nodedb_types::QualifiedCollection::new(ctx.database_id, collection),
        ),
        None => refuse_unscoped_match(ctx),
    }
}

fn refuse_unscoped_match(ctx: &RefusalCtx<'_>) -> crate::Result<()> {
    if !ctx.identity_has_any_rule() {
        return Ok(());
    }
    Err(ctx.refusal(
        "graph pattern matching is not supported while a redaction policy applies to \
         this role and the pattern names no collection: the match returns graph \
         topology, which column redaction cannot be applied to, and its scope cannot be \
         narrowed to an unprotected collection"
            .to_string(),
    ))
}
