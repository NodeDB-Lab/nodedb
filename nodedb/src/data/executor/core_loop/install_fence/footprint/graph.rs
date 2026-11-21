// SPDX-License-Identifier: BUSL-1.1

//! Footprints of graph ops.
//!
//! The CSR partition, the interned labels and the edge store of a tenant
//! are one graph key of the request's database and tenant, the key an
//! install arms. An edge write also keys its collection. A graph read keys
//! the graph and the collection it names.

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};
use nodedb_types::QualifiedCollection;

use super::class::{Footprint, Scope};
use super::keys::KeySet;

pub(super) fn graph_footprint(op: &GraphOp, scope: &Scope) -> Footprint {
    match op {
        GraphOp::EdgePut { collection, .. }
        | GraphOp::EdgeDelete { collection, .. }
        | GraphOp::TruncateEdges { collection, .. } => {
            KeySet::new(scope).collection(collection).graph().write()
        }
        GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges } => {
            edges_write(edges, scope)
        }
        // The resolve pass reads what the wrapped delete writes.
        GraphOp::ResolveEdgeDelete(inner) => graph_footprint(inner, scope),
        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => {
            KeySet::new(scope).graph().write()
        }
        GraphOp::Hop { collection, .. }
        | GraphOp::Neighbors { collection, .. }
        | GraphOp::NeighborsMulti { collection, .. }
        | GraphOp::Path { collection, .. }
        | GraphOp::Subgraph { collection, .. }
        | GraphOp::Stats { collection, .. } => graph_read(collection.as_ref(), scope),
        GraphOp::RagFusion { collection, .. }
        | GraphOp::TemporalNeighbors { collection, .. }
        | GraphOp::NodeEdgeGuard { collection, .. }
        | GraphOp::NodePresenceGuard { collection, .. }
        | GraphOp::NodePresenceRead { collection, .. } => graph_read(Some(collection), scope),
        GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_) => graph_read(None, scope),
    }
}

/// A write of every edge of `edges` and of the graph.
fn edges_write(edges: &[BatchEdge], scope: &Scope) -> Footprint {
    edges
        .iter()
        .fold(KeySet::new(scope), |keys, edge| {
            keys.collection(&edge.collection)
        })
        .graph()
        .write()
}

/// A read of the graph and of `collection` when the op names one.
fn graph_read(collection: Option<&QualifiedCollection>, scope: &Scope) -> Footprint {
    let keys = match collection {
        Some(collection) => KeySet::new(scope).collection(collection),
        None => KeySet::new(scope),
    };
    keys.graph().read()
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_types::Surrogate;

    use super::super::keys::tests::{coll, graph, key, scope, write};
    use super::super::plan::plan_footprint;
    use super::*;

    fn class(op: GraphOp) -> Footprint {
        plan_footprint(&PhysicalPlan::Graph(op), &scope())
    }

    #[test]
    fn an_edge_put_keys_its_collection_and_the_graph() {
        let put = GraphOp::EdgePut {
            collection: coll("social"),
            src_id: "a".into(),
            label: "follows".into(),
            dst_id: "b".into(),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
        };
        assert_eq!(class(put), write(vec![key("social"), graph()]));
    }

    #[test]
    fn a_label_op_keys_the_graph_alone() {
        let labels = GraphOp::SetNodeLabels {
            node_id: "a".into(),
            labels: vec!["Person".into()],
        };
        assert_eq!(class(labels), write(vec![graph()]));
    }

    #[test]
    fn a_graph_read_keys_the_graph_and_its_collection() {
        let stats = GraphOp::Stats {
            collection: None,
            as_of: None,
        };
        assert_eq!(class(stats), Footprint::Keys(vec![graph()]));
        let stats = GraphOp::Stats {
            collection: Some(coll("social")),
            as_of: None,
        };
        assert_eq!(class(stats), Footprint::Keys(vec![key("social"), graph()]));
    }

    #[test]
    fn an_edge_truncate_keys_its_collection_and_the_graph() {
        let truncate = GraphOp::TruncateEdges {
            collection: coll("social"),
            vshard: 0,
        };
        assert_eq!(class(truncate), write(vec![key("social"), graph()]));
    }
}
