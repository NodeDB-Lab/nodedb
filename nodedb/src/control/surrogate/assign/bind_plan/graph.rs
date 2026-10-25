// SPDX-License-Identifier: BUSL-1.1

//! Graph ops: an edge binds both endpoint node ids in the edge's collection.

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use super::binder::{BindSlot, IdentityBinder};

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
            let key = binder.plan_key(collection.as_str())?;
            binder.resolve_slots_in_place(vec![
                BindSlot::keyed(key, src_id.as_bytes(), src_surrogate),
                BindSlot::keyed(key, dst_id.as_bytes(), dst_surrogate),
            ])
        }
        GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges } => {
            let mut slots = Vec::with_capacity(edges.len() * 2);
            for edge in edges.iter_mut() {
                push_edge_slots(binder, edge, &mut slots)?;
            }
            binder.resolve_slots_in_place(slots)
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
        | GraphOp::Stats { .. } => Ok(()),
        // The guards compare edges and documents by name and bind nothing.
        // A TRUNCATE's share reads each edge's surrogates from the core's
        // graph.
        GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. }
        | GraphOp::NodePresenceRead { .. } => Ok(()),
    }
}

/// Both endpoint slots of one batch edge, keyed in the edge's collection.
fn push_edge_slots<'e>(
    binder: &IdentityBinder<'_>,
    edge: &'e mut BatchEdge,
    slots: &mut Vec<BindSlot<'e, 'e>>,
) -> crate::Result<()> {
    let BatchEdge {
        collection,
        src_id,
        dst_id,
        src_surrogate,
        dst_surrogate,
        ..
    } = edge;
    let key = binder.plan_key(collection.as_str())?;
    slots.push(BindSlot::keyed(key, src_id.as_bytes(), src_surrogate));
    slots.push(BindSlot::keyed(key, dst_id.as_bytes(), dst_surrogate));
    Ok(())
}
