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

        // Handler reports no count yet.
        GraphOp::EdgePut { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDelete { .. }
        | GraphOp::EdgeDeleteBatch { .. }
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        // Read-only resolve: payload is the internal admission verdict, never a client row.
        | GraphOp::ResolveEdgeDelete(_) => PlanKind::Execution,
    }
}
