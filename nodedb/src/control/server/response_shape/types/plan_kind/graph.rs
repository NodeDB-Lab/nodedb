// SPDX-License-Identifier: BUSL-1.1

//! `GraphOp` classification.

use nodedb_physical::physical_plan::GraphOp;

use super::kind::PlanKind;

pub(super) fn describe_graph(op: &GraphOp) -> PlanKind {
    match op {
        // Traversals, pattern matches, algorithms and stats all return one
        // row per hit / node / collection.
        GraphOp::Hop { .. }
        | GraphOp::Neighbors { .. }
        | GraphOp::NeighborsMulti { .. }
        | GraphOp::Path { .. }
        | GraphOp::Subgraph { .. }
        | GraphOp::RagFusion { .. }
        | GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::TemporalAlgorithm { .. }
        | GraphOp::Stats { .. } => PlanKind::MultiRow,

        GraphOp::EdgePut { .. } | GraphOp::EdgePutBatch { .. } => PlanKind::DmlResult("INSERT"),

        // `ResolveEdgeDelete` reports the same live/absent verdict as the
        // delete it wraps, via `response_affected` — matches an edge delete's
        // tag even though the resolve pass itself writes nothing.
        GraphOp::EdgeDelete { .. }
        | GraphOp::EdgeDeleteBatch { .. }
        | GraphOp::ResolveEdgeDelete(_) => PlanKind::DmlResult("DELETE"),

        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => {
            PlanKind::DmlResult("UPDATE")
        }
    }
}
