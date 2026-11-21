// SPDX-License-Identifier: BUSL-1.1

//! Footprints of graph ops.
//!
//! The CSR partition, the interned labels and the edge store of a tenant
//! are one graph key of the request's database and tenant, the key an
//! install arms when it writes graph state. An edge write also keys its
//! collection. A graph read keys the graph and every collection whose
//! documents or index it reads:
//! - a traversal, a guard and a stats read: the collection it names
//! - a fusion or triple search: its collection's vector and text index
//! - an algorithm or superstep: the edge collection of its params
//! - a MATCH: the `IN` collection its query names, whose node documents
//!   its property predicates read

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};
use nodedb_types::QualifiedCollection;

use crate::data::executor::handlers::graph_algo_edges::algo_collection;
use crate::engine::graph::algo::params::AlgoParams;
use crate::engine::graph::pattern::ast::MatchQuery;

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
        // An algorithm reads the CSR and the edges of its collection.
        GraphOp::Algo { params, .. } => algo_read(params, scope),
        GraphOp::BspSuperstep(plan) => algo_read(&plan.params, scope),
        GraphOp::WccSuperstep(plan) => algo_read(&plan.params, scope),
        // A MATCH reads the node documents of its `IN` collection, which
        // only its query names.
        GraphOp::Match { query, .. }
        | GraphOp::MatchContinuation { query, .. }
        | GraphOp::MatchVarLenResume { query, .. } => match_read(query, scope),
    }
}

/// A read of the graph and of the collection `params` names. A name that
/// is no collection name is a task of the whole database.
fn algo_read(params: &AlgoParams, scope: &Scope) -> Footprint {
    graph_read_named(&params.collection, scope)
}

/// A read of the graph and of the collection `name` names. A name that is
/// no collection name is a task of the whole database.
fn graph_read_named(name: &str, scope: &Scope) -> Footprint {
    match algo_collection(scope.db, name) {
        Ok(collection) => KeySet::new(scope).stored(&collection).graph().read(),
        Err(error) => {
            crate::diag::stored_collection_name_malformed(
                "fence_footprint: the graph read waits as a task of its database",
                name,
                &error,
            );
            scope.whole_database()
        }
    }
}

/// A read of the graph and of the node documents of the collection the
/// MATCH `query` names. A query that does not decode, or names no
/// collection name, is a task of the whole database.
fn match_read(query: &[u8], scope: &Scope) -> Footprint {
    let Ok(query) = zerompk::from_msgpack::<MatchQuery>(query) else {
        return scope.whole_database();
    };
    match query.collection.as_deref() {
        None => graph_read(None, scope),
        Some(name) => graph_read_named(name, scope),
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

    use super::super::keys::tests::{coll, graph, key, scope, whole, write};
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

    /// The bytes of a MATCH query over the collection `collection` names.
    fn match_query(collection: Option<&str>) -> Vec<u8> {
        zerompk::to_msgpack_vec(&MatchQuery {
            clauses: Vec::new(),
            where_predicates: Vec::new(),
            return_columns: Vec::new(),
            distinct: false,
            limit: None,
            offset: None,
            order_by: Vec::new(),
            collection: collection.map(str::to_owned),
        })
        .expect("encode match query")
    }

    fn match_op(query: Vec<u8>) -> GraphOp {
        GraphOp::Match {
            query,
            frontier_bitmap: None,
            cluster_mode: false,
            scope: nodedb_physical::physical_plan::GraphReadScope::default(),
        }
    }

    #[test]
    fn a_match_keys_the_graph_and_its_in_collection() {
        assert_eq!(
            class(match_op(match_query(Some("people")))),
            Footprint::Keys(vec![key("people"), graph()])
        );
        assert_eq!(
            class(match_op(match_query(None))),
            Footprint::Keys(vec![graph()])
        );
    }

    #[test]
    fn a_match_whose_query_does_not_decode_is_its_whole_database() {
        assert_eq!(class(match_op(vec![0xc1])), whole());
    }

    #[test]
    fn an_algorithm_keys_the_graph_and_its_edge_collection() {
        let algo = GraphOp::Algo {
            algorithm: crate::engine::graph::algo::params::GraphAlgorithm::PageRank,
            params: AlgoParams {
                collection: "social".into(),
                ..AlgoParams::default()
            },
            stage: nodedb_physical::physical_plan::AlgoStage::Local,
            scope: nodedb_physical::physical_plan::GraphReadScope::default(),
        };
        assert_eq!(class(algo), Footprint::Keys(vec![key("social"), graph()]));
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
