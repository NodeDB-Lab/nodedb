// SPDX-License-Identifier: BUSL-1.1

//! Graph write keys.
//!
//! Every edge write locks its edge's surrogate pair and both endpoints'
//! node lock pairs, on both endpoint homes. A batch does so for each of its
//! edges. A node delete's guard locks its node's pair on the node's key
//! home, so the guard and every write of an edge on its node run in
//! sequence order there.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use super::plan::{not_a_write, unsequenced};
use super::set::{WriteKeys, row_id_key};

/// The lock namespace of node-label writes. Labels belong to a node, not to
/// a collection, so their node lock pairs never alias an edge collection's.
pub const NODE_LABEL_LOCKS: &str = "\0__graph_node_labels__";

/// Add the write keys of graph op `op` to `keys`.
pub(super) fn add_keys(keys: &mut WriteKeys, op: &GraphOp) -> crate::Result<()> {
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
        } => keys.edge(
            collection.as_str(),
            src_id,
            dst_id,
            (src_surrogate.as_u32(), dst_surrogate.as_u32()),
        ),
        GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges } => {
            if edges.is_empty() {
                return Err(unsequenced("an edge batch carries no edges"));
            }
            for edge in edges {
                add_batch_edge(keys, edge);
            }
        }
        GraphOp::SetNodeLabels { node_id, .. } | GraphOp::RemoveNodeLabels { node_id, .. } => {
            keys.node(NODE_LABEL_LOCKS, node_id);
        }
        GraphOp::NodeEdgeGuard {
            collection,
            node_id,
            ..
        } => keys.node(collection.as_str(), node_id),
        // A TRUNCATE's edge share locks no edge: it records a cut at its
        // transaction's ordinal, which hides every version applied below it
        // whatever order this vShard installs the edge writes in.
        GraphOp::TruncateEdges { collection, vshard } => keys.home(collection.as_str(), *vshard),
        // A CRDT delete's presence guard locks every document it names,
        // stored or not, by the key every writer of that document locks. Its
        // `Intent` on the collection orders it against every collection-wide
        // write. So it runs after every lower writer of those documents
        // installed.
        GraphOp::NodePresenceGuard {
            collection,
            vshard,
            present,
            absent,
        } => {
            keys.home(collection.as_str(), *vshard);
            keys.kv_keys(
                collection.as_str(),
                present.iter().chain(absent.iter()).map(|id| row_id_key(id)),
            );
        }
        GraphOp::ResolveEdgeDelete(_)
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
        | GraphOp::Stats { .. }
        | GraphOp::NodePresenceRead { .. } => {
            return Err(not_a_write("a graph read"));
        }
    }
    Ok(())
}

/// Lock one edge of a batch in its own collection, as a single edge write
/// does.
fn add_batch_edge(keys: &mut WriteKeys, edge: &BatchEdge) {
    keys.edge(
        edge.collection.as_str(),
        &edge.src_id,
        &edge.dst_id,
        (edge.src_surrogate.as_u32(), edge.dst_surrogate.as_u32()),
    );
}
