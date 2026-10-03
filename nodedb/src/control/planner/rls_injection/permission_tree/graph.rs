// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for graph-overlay operations.
//!
//! No graph read carries a slot to evaluate a resource-column predicate
//! in, so each refuses while a permission tree governs the collection —
//! same verdicts as the RLS pass. Unlike RLS (which leaves writes to a
//! separate write-path check), this pass has its own write/delete levels
//! and applies them here, since edge writes do carry a collection.

use nodedb_physical::physical_plan::GraphOp;

use super::context::{PermCtx, PermTreeLevel};

const TRAVERSAL_REASON: &str =
    "a traversal returns graph topology, which the subtree filter cannot be evaluated against";

const ALGORITHM_REASON: &str = "an algorithm returns per-node scalars computed over every edge, which the subtree filter \
     cannot be evaluated against";

const MATCH_REASON: &str = "a pattern match returns bindings over graph topology, which the subtree filter cannot be \
     evaluated against";

/// Exhaustive over [`GraphOp`] so a new graph operation forces a decision
/// between filtering, refusing, and no-op.
pub(super) fn apply_graph(ctx: &PermCtx<'_>, op: &GraphOp) -> crate::Result<()> {
    match op {
        // Refuse: discloses topology, not row bodies. `None` is a
        // tree-index walk with no collection mapping — no tree to consult.
        GraphOp::Hop { collection, .. }
        | GraphOp::Neighbors { collection, .. }
        | GraphOp::NeighborsMulti { collection, .. }
        | GraphOp::Path { collection, .. }
        | GraphOp::Subgraph { collection, .. } => match collection.as_ref() {
            Some(collection) => ctx.refuse_if_tree(collection, TRAVERSAL_REASON),
            None => Ok(()),
        },

        // Refuse: same shape as `Neighbors`. The bitemporal form always names
        // its collection — the versioned edge key layout is collection-scoped.
        GraphOp::TemporalNeighbors { collection, .. } => {
            ctx.refuse_if_tree(collection, TRAVERSAL_REASON)
        }

        // Refuse: returns bindings with no filter slot; its own `WHERE`
        // can probe a hidden row's field one predicate at a time.
        GraphOp::Match { query, .. }
        | GraphOp::MatchContinuation { query, .. }
        | GraphOp::MatchVarLenResume { query, .. } => refuse_match(ctx, query),

        // Refuse: runs over the whole CSR, returns scalars derived from
        // every row (including ones outside the subtree), no column to
        // filter. `AlgoParams` comes from `nodedb-graph` and carries a bare
        // name — qualify it against this task's database before the lookup.
        GraphOp::Algo { params, .. } | GraphOp::TemporalAlgorithm { params, .. } => ctx
            .refuse_if_tree(
                &nodedb_types::QualifiedCollection::new(ctx.database_id, &params.collection),
                ALGORITHM_REASON,
            ),

        // Refuse: the distributed supersteps are the same algorithms one round
        // at a time, carrying the target collection in their params.
        GraphOp::BspSuperstep(plan) => ctx.refuse_if_tree(
            &nodedb_types::QualifiedCollection::new(ctx.database_id, &plan.params.collection),
            ALGORITHM_REASON,
        ),
        GraphOp::WccSuperstep(plan) => ctx.refuse_if_tree(
            &nodedb_types::QualifiedCollection::new(ctx.database_id, &plan.params.collection),
            ALGORITHM_REASON,
        ),

        // Refuse: fusion envelope has no filter slot and no sub-plan to
        // recurse into.
        GraphOp::RagFusion { collection, .. } => ctx.refuse_if_tree(
            collection,
            "fusion returns ranked document rows through a fused response shape that carries no \
             subtree filter",
        ),

        // Refuse: counters over the collection's edges, no column to filter.
        // `None` spans every collection, so no narrow question can be asked.
        GraphOp::Stats { collection, .. } => match collection.as_ref() {
            Some(collection) => ctx.refuse_if_tree(
                collection,
                "graph statistics are counters over the collection's edges, which the subtree \
                 filter cannot be evaluated against",
            ),
            None => ctx.refuse_if_any_tree(
                "graph statistics report counters for every collection holding edges, which the \
                 subtree filter cannot be evaluated against",
            ),
        },

        // Filter (write level, blanket): an edge write names its endpoints
        // directly, so there is no predicate to narrow.
        GraphOp::EdgePut { collection, .. } => ctx.authorize(collection, PermTreeLevel::Write),
        GraphOp::EdgePutBatch { edges } => {
            for edge in edges {
                ctx.authorize(&edge.collection, PermTreeLevel::Write)?;
            }
            Ok(())
        }

        // Filter (delete level, blanket): removing an edge removes stored
        // topology of the collection.
        GraphOp::EdgeDelete { collection, .. } => ctx.authorize(collection, PermTreeLevel::Delete),
        // Recurse: the resolve pass stands in for the delete it wraps.
        GraphOp::ResolveEdgeDelete(inner) => apply_graph(ctx, inner),
        GraphOp::EdgeDeleteBatch { edges } => {
            for edge in edges {
                ctx.authorize(&edge.collection, PermTreeLevel::Delete)?;
            }
            Ok(())
        }

        // No-op: node labels are keyed by node id alone and name no
        // collection, so this pass has no tree definition to resolve.
        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => Ok(()),
        // No-op: these derive from a document delete or TRUNCATE this pass
        // already decided on the same collection.
        GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. }
        | GraphOp::NodePresenceRead { .. } => Ok(()),
    }
}

/// Refuse a pattern match whose target collection carries a permission tree.
/// A query naming no collection, or one that fails to decode, falls back
/// to the tenant-wide question, as the RLS pass does.
fn refuse_match(ctx: &PermCtx<'_>, query: &[u8]) -> crate::Result<()> {
    let decoded: Result<crate::engine::graph::pattern::ast::MatchQuery, _> =
        zerompk::from_msgpack(query);
    match decoded.ok().and_then(|query| query.collection) {
        Some(collection) => ctx.refuse_if_tree(
            &nodedb_types::QualifiedCollection::new(ctx.database_id, &collection),
            MATCH_REASON,
        ),
        None => ctx.refuse_if_any_tree(MATCH_REASON),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_graph::{AlgoParams, GraphAlgorithm};
    use nodedb_physical::physical_plan::GraphOp;

    use super::super::plan::test_support::{
        apply, apply_without_tree, assert_refused, cache_with_tree,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn neighbors(collection: Option<&str>) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::Neighbors {
            collection: collection.map(|c| {
                nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, c)
            }),
            node_id: "n1".into(),
            edge_labels: Vec::new(),
            direction: nodedb_types::graph::Direction::Out,
            rls_filters: Vec::new(),
        })
    }

    /// A traversal over a governed collection discloses topology the subtree
    /// filter cannot narrow, so it is refused.
    #[test]
    fn traversal_is_refused_under_a_tree() {
        let cache = cache_with_tree("docs");
        let mut plan = neighbors(Some("docs"));
        assert_refused(apply(&mut plan, &cache), "docs");
    }

    /// A traversal that names no collection has no tree to resolve.
    #[test]
    fn unscoped_traversal_is_untouched() {
        let cache = cache_with_tree("docs");
        let mut plan = neighbors(None);
        let before = plan.clone();
        assert!(apply(&mut plan, &cache).is_ok());
        assert_eq!(plan, before);
    }

    /// …and with no tree registered at all, the scoped form runs untouched.
    #[test]
    fn traversal_without_a_tree_is_untouched() {
        let mut plan = neighbors(Some("docs"));
        let before = plan.clone();
        assert!(apply_without_tree(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A graph algorithm runs over every edge of the collection.
    #[test]
    fn algo_is_refused_under_a_tree() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Graph(GraphOp::Algo {
            algorithm: GraphAlgorithm::PageRank,
            params: AlgoParams {
                collection: "docs".into(),
                edge_label: None,
                damping: None,
                max_iterations: None,
                tolerance: None,
                source_node: None,
                sample_size: None,
                direction: None,
                resolution: None,
                mode: None,
                personalization_vector: None,
            },
            stage: nodedb_physical::physical_plan::AlgoStage::Local,
        });
        assert_refused(apply(&mut plan, &cache), "docs");
    }

    /// Tenant-wide graph stats cannot be narrowed, so any tree refuses them.
    #[test]
    fn unscoped_stats_is_refused_while_any_tree_applies() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Graph(GraphOp::Stats {
            collection: None,
            as_of: None,
        });
        assert!(matches!(
            apply(&mut plan, &cache),
            Err(crate::Error::PlanError { .. })
        ));
    }
}
