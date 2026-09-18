// SPDX-License-Identifier: Apache-2.0

//! Cross-node routing predicates for physical plans.
//!
//! Two questions, kept together because they recurse through the same wrapper
//! ops and must agree about what counts as a leaf:
//!
//! - whether a plan tree contains a cluster-partitioned leaf (graph / array,
//!   needing scatter-gather) versus a single-vShard-homed source (document /
//!   kv / columnar / ts / spatial / vector / text, routed directly)
//! - whether a plan is a sharded source the converter must wrap in
//!   `Exchange{Gather}`, so per-core results are merged on the coordinator

use super::{ColumnarOp, DocumentOp, ExchangeOp, GraphOp, PhysicalPlan, QueryOp, TextOp, VectorOp};

/// `true` if the plan tree contains a leaf whose rows are distributed across
/// vShards by node-id or tile-id (graph traversal, Array/ClusterArray),
/// rather than wholly owned by one vShard hashed from the collection name.
///
/// Routing a single-vShard-homed plan (document/kv/columnar/ts/spatial/
/// vector/text) via the gateway's `other` arm returns exactly the right rows;
/// broadcasting a cluster-partitioned one would multiply rows across vShards
/// — callers must take the scatter-gather path instead. Recurses through
/// wrapper ops the same way `is_sharded_source` does.
pub fn plan_contains_cluster_partitioned_leaf(plan: &PhysicalPlan) -> bool {
    match plan {
        // Recurse through aggregate wrappers.
        PhysicalPlan::Query(QueryOp::Aggregate { input, .. }) => match input {
            Some(child) => plan_contains_cluster_partitioned_leaf(child),
            None => false,
        },

        // Recurse through both sides of a HashJoin (and their bitmap inputs).
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            ..
        }) => {
            left_input
                .as_deref()
                .map(plan_contains_cluster_partitioned_leaf)
                .unwrap_or(false)
                || right_input
                    .as_deref()
                    .map(plan_contains_cluster_partitioned_leaf)
                    .unwrap_or(false)
                || left_bitmap
                    .as_deref()
                    .map(plan_contains_cluster_partitioned_leaf)
                    .unwrap_or(false)
                || right_bitmap
                    .as_deref()
                    .map(plan_contains_cluster_partitioned_leaf)
                    .unwrap_or(false)
        }

        // Recurse through Exchange wrapper (child is the real plan).
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, .. })) => {
            plan_contains_cluster_partitioned_leaf(child)
        }

        // Recurse through PostProcess (its materialized input is the real
        // plan). Normally resolved to a `ProviderScan` before routing, but a
        // conservative recursion keeps routing correct if one survives.
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => {
            plan_contains_cluster_partitioned_leaf(input)
        }

        // Recurse through every SetOp branch for the same reason.
        PhysicalPlan::Query(QueryOp::SetOp { inputs, .. }) => {
            inputs.iter().any(plan_contains_cluster_partitioned_leaf)
        }

        // Recurse through lateral outer plans.
        PhysicalPlan::Query(QueryOp::LateralTopK { outer_plan, .. })
        | PhysicalPlan::Query(QueryOp::LateralLoop { outer_plan, .. }) => {
            plan_contains_cluster_partitioned_leaf(outer_plan)
        }

        // Graph traversal / query ops are cluster-partitioned (node-id routed).
        PhysicalPlan::Graph(GraphOp::Hop { .. })
        | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
        | PhysicalPlan::Graph(GraphOp::NeighborsMulti { .. })
        | PhysicalPlan::Graph(GraphOp::Path { .. })
        | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
        | PhysicalPlan::Graph(GraphOp::RagFusion { .. })
        | PhysicalPlan::Graph(GraphOp::Algo { .. })
        | PhysicalPlan::Graph(GraphOp::Match { .. })
        | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. })
        | PhysicalPlan::Graph(GraphOp::MatchVarLenResume { .. })
        | PhysicalPlan::Graph(GraphOp::TemporalNeighbors { .. })
        | PhysicalPlan::Graph(GraphOp::TemporalAlgorithm { .. })
        | PhysicalPlan::Graph(GraphOp::BspSuperstep(_))
        | PhysicalPlan::Graph(GraphOp::WccSuperstep(_))
        | PhysicalPlan::Graph(GraphOp::Stats { .. }) => true,

        // Array ops are cluster-partitioned by tile-id.
        PhysicalPlan::Array(_) | PhysicalPlan::ClusterArray(_) => true,

        // Graph write ops and all other engine ops are single-vShard-homed.
        PhysicalPlan::Graph(GraphOp::EdgePut { .. })
        | PhysicalPlan::Graph(GraphOp::EdgePutBatch { .. })
        | PhysicalPlan::Graph(GraphOp::EdgeDelete { .. })
        | PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { .. })
        | PhysicalPlan::Graph(GraphOp::ResolveEdgeDelete(_))
        | PhysicalPlan::Graph(GraphOp::SetNodeLabels { .. })
        | PhysicalPlan::Graph(GraphOp::RemoveNodeLabels { .. })
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterEvent(_)
        | PhysicalPlan::Query(QueryOp::ProviderScan { .. })
        | PhysicalPlan::Query(QueryOp::PartialAggregate { .. })
        | PhysicalPlan::Query(QueryOp::PartialAggregateState { .. })
        // ShuffleJoinConsume / ShuffleAggregateConsume consume node-local staged
        // files: terminal local ops, never a cluster-partitioned scan to fan out.
        | PhysicalPlan::Query(QueryOp::ShuffleJoinConsume { .. })
        | PhysicalPlan::Query(QueryOp::ShuffleAggregateConsume { .. })
        | PhysicalPlan::Query(QueryOp::NestedLoopJoin { .. })
        | PhysicalPlan::Query(QueryOp::SortMergeJoin { .. })
        | PhysicalPlan::Query(QueryOp::FacetCounts { .. })
        | PhysicalPlan::Query(QueryOp::RecursiveScan { .. })
        | PhysicalPlan::Query(QueryOp::RecursiveValue { .. }) => false,
    }
}

impl PhysicalPlan {
    /// Whether this plan is a sharded-source operation that the converter must
    /// wrap in `Exchange{Gather}`. Identifies reads and joins that are
    /// distributed across all Data Plane cores and whose results must be
    /// gathered and merged on the coordinator.
    pub fn is_sharded_source(&self) -> bool {
        // Sharded only if a leaf reads a real per-shard collection — a
        // pure-catalog plan must run exactly once (broadcasting overcounts).
        match self {
            // Catalog sub-plan: inherit child's sharded-ness. No sub-plan:
            // legacy per-shard scan → always sharded.
            PhysicalPlan::Query(QueryOp::Aggregate { input, .. }) => match input {
                Some(child) => child.is_sharded_source(),
                None => true,
            },
            // Sharded iff at least one side reads a real per-shard collection.
            PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
                ..
            }) => {
                hash_join_side_is_sharded(left_collection.as_str(), left_input, left_bitmap)
                    || hash_join_side_is_sharded(
                        right_collection.as_str(),
                        right_input,
                        right_bitmap,
                    )
            }
            // Coordinator-local: the resolver materializes every branch and
            // merges on the coordinator, so the node itself is never fanned out.
            PhysicalPlan::Query(QueryOp::SetOp { .. }) => false,
            _ => self.is_sharded_source_leaf(),
        }
    }

    /// Leaf / non-recursive sharded-source check for all plan variants other
    /// than `Aggregate` and `HashJoin` (handled structurally in
    /// `is_sharded_source`).
    fn is_sharded_source_leaf(&self) -> bool {
        matches!(
            self,
            PhysicalPlan::Document(DocumentOp::Scan { .. })
                | PhysicalPlan::Columnar(ColumnarOp::Scan { .. })
                | PhysicalPlan::Query(QueryOp::PartialAggregate { .. })
                | PhysicalPlan::Query(QueryOp::PartialAggregateState { .. })
                | PhysicalPlan::Graph(GraphOp::Hop { .. })
                | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
                | PhysicalPlan::Graph(GraphOp::NeighborsMulti { .. })
                | PhysicalPlan::Graph(GraphOp::Path { .. })
                | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
                | PhysicalPlan::Graph(GraphOp::RagFusion { .. })
                | PhysicalPlan::Graph(GraphOp::Match { .. })
                | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. })
                | PhysicalPlan::Graph(GraphOp::MatchVarLenResume { .. })
                | PhysicalPlan::Graph(GraphOp::TemporalNeighbors { .. })
                | PhysicalPlan::Graph(GraphOp::TemporalAlgorithm { .. })
                | PhysicalPlan::Graph(GraphOp::BspSuperstep(_))
                | PhysicalPlan::Graph(GraphOp::WccSuperstep(_))
                | PhysicalPlan::Graph(GraphOp::Stats { .. })
                | PhysicalPlan::Vector(VectorOp::Search { .. })
                | PhysicalPlan::Text(TextOp::Search { .. })
                | PhysicalPlan::Text(TextOp::HybridSearch { .. })
                | PhysicalPlan::Text(TextOp::HybridSearchTriple { .. })
                | PhysicalPlan::Text(TextOp::BM25ScoreScan { .. })
        )
    }
}

/// Whether one side of a `HashJoin` reads a real per-shard collection (and so
/// makes the join a sharded source fanned to all cores). `input: Some` child
/// recurses; `input: None` + non-empty `collection` is sharded; `bitmap:
/// Some` (`IndexedFetch`) is sharded.
fn hash_join_side_is_sharded(
    collection: &str,
    input: &Option<Box<PhysicalPlan>>,
    bitmap: &Option<Box<PhysicalPlan>>,
) -> bool {
    if let Some(child) = input {
        // An Exchange wrapper always carries a sharded child; otherwise defer
        // to the child's own structural classification.
        return matches!(**child, PhysicalPlan::Query(QueryOp::Exchange(_)))
            || child.is_sharded_source();
    }
    if bitmap.is_some() {
        return true;
    }
    !collection.is_empty()
}
