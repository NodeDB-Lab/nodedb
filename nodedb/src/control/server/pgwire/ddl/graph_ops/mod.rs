//! Graph DSL commands: GRAPH INSERT EDGE, GRAPH DELETE EDGE,
//! GRAPH LABEL/UNLABEL, GRAPH TRAVERSE, GRAPH NEIGHBORS, GRAPH PATH,
//! GRAPH ALGO.
//!
//! Every handler consumes already-parsed typed fields from
//! `nodedb_sql::ddl_ast::NodedbStatement`. Raw-SQL tokenising lives
//! in `nodedb-sql::ddl_ast::graph_parse` — no handler in this
//! directory does string-prefix parsing.

mod algo;
mod edge;
mod response;
mod traverse;

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::NodedbStatement;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Dispatch a parsed graph-DSL variant to its handler.
///
/// Returns `None` when the statement is not a graph DSL variant,
/// so the caller can fall through to other dispatchers.
pub async fn dispatch_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    stmt: NodedbStatement,
) -> Option<PgWireResult<Vec<Response>>> {
    match stmt {
        NodedbStatement::GraphInsertEdge {
            src,
            dst,
            label,
            properties,
        } => Some(edge::insert_edge(state, identity, src, dst, label, properties).await),
        NodedbStatement::GraphDeleteEdge { src, dst, label } => {
            Some(edge::delete_edge(state, identity, src, dst, label).await)
        }
        NodedbStatement::GraphSetLabels {
            node_id,
            labels,
            remove,
        } => Some(edge::set_node_labels(state, identity, node_id, labels, remove).await),
        NodedbStatement::GraphTraverse {
            start,
            depth,
            edge_label,
            direction,
        } => Some(traverse::traverse(state, identity, start, depth, edge_label, direction).await),
        NodedbStatement::GraphNeighbors {
            node,
            edge_label,
            direction,
        } => Some(traverse::neighbors(state, identity, node, edge_label, direction).await),
        NodedbStatement::GraphPath {
            src,
            dst,
            max_depth,
            edge_label,
        } => Some(traverse::shortest_path(state, identity, src, dst, max_depth, edge_label).await),
        NodedbStatement::GraphAlgo {
            algorithm,
            collection,
            damping,
            tolerance,
            resolution,
            max_iterations,
            sample_size,
            source_node,
            direction,
            mode,
        } => Some(
            algo::algo(
                state,
                identity,
                &algorithm,
                collection,
                damping,
                tolerance,
                resolution,
                max_iterations,
                sample_size,
                source_node,
                direction,
                mode,
            )
            .await,
        ),
        _ => None,
    }
}
