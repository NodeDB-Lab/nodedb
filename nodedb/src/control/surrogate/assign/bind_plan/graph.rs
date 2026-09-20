// SPDX-License-Identifier: BUSL-1.1

//! Graph ops: an edge binds both endpoint node ids in the edge's collection.

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use super::binder::IdentityBinder;

pub(super) fn bind(binder: &IdentityBinder<'_>, op: &mut GraphOp) -> crate::Result<()> {
    match op {
        GraphOp::EdgePut {
            collection,
            src_id,
            dst_id,
            src_surrogate,
            dst_surrogate,
            ..
        }
        | GraphOp::EdgeDelete {
            collection,
            src_id,
            dst_id,
            src_surrogate,
            dst_surrogate,
            ..
        } => {
            binder.resolve_in_place(collection.as_str(), src_id.as_bytes(), src_surrogate)?;
            binder.resolve_in_place(collection.as_str(), dst_id.as_bytes(), dst_surrogate)
        }
        GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges } => {
            for edge in edges.iter_mut() {
                bind_edge(binder, edge)?;
            }
            Ok(())
        }
        GraphOp::ResolveEdgeDelete(inner) => bind(binder, inner),
        // Label writes name a node by id only; traversals, analytics and
        // supersteps create no identity.
        GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        | GraphOp::Hop { .. }
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
        | GraphOp::Stats { .. } => Ok(()),
    }
}

fn bind_edge(binder: &IdentityBinder<'_>, edge: &mut BatchEdge) -> crate::Result<()> {
    let collection = edge.collection.as_str();
    binder.resolve_in_place(collection, edge.src_id.as_bytes(), &mut edge.src_surrogate)?;
    binder.resolve_in_place(collection, edge.dst_id.as_bytes(), &mut edge.dst_surrogate)
}
