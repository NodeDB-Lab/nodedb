// SPDX-License-Identifier: BUSL-1.1

//! Dispatch a parsed graph-overlay statement to its protocol-neutral handler.

use nodedb_sql::ddl_ast::statement::{GraphStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::{algo, edge, rag_fusion, stats, traverse};

/// Dispatch a parsed graph-overlay variant to its handler.
///
/// Returns `None` when the statement is not a graph-overlay variant this family
/// owns (e.g. `GraphStmt::MatchQuery`, which the router dispatches to the neutral
/// `match_ops` handler from its own typed arm before calling this), so the caller
/// falls through to the transitional pgwire delegation.
pub async fn dispatch_graph(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    stmt: NodedbStatement,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // Graph reads honour the session's read consistency, like every other
    // read (see `DmlTxnCtx::linearizable_reads` for the default).
    let linearizable = txn_ctx.linearizable_reads();
    match stmt {
        NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
            collection,
            src,
            dst,
            label,
            properties,
        }) => Some(
            edge::insert_edge(
                state,
                identity,
                database_id,
                edge::EdgeRef {
                    collection,
                    src,
                    dst,
                    label,
                },
                properties,
                txn_ctx,
            )
            .await,
        ),
        NodedbStatement::Graph(GraphStmt::GraphDeleteEdge {
            collection,
            src,
            dst,
            label,
        }) => Some(
            edge::delete_edge(
                state,
                identity,
                database_id,
                edge::EdgeRef {
                    collection,
                    src,
                    dst,
                    label,
                },
                txn_ctx,
            )
            .await,
        ),
        NodedbStatement::Graph(GraphStmt::GraphSetLabels {
            node_id,
            labels,
            remove,
        }) => Some(edge::set_node_labels(state, identity, node_id, labels, remove).await),
        NodedbStatement::Graph(GraphStmt::GraphTraverse {
            collection,
            start,
            depth,
            edge_labels,
            direction,
            edge_predicate,
        }) => {
            // Every hop of the walk merges the session transaction's staged
            // edge writes. Idle sessions resolve to `None`.
            let (txn_id, _) = txn_ctx.sessions.txn_identity(txn_ctx.session_id);
            Some(
                traverse::traverse(
                    state,
                    identity,
                    database_id,
                    traverse::TraverseRequest {
                        collection,
                        start,
                        depth,
                        edge_labels,
                        direction,
                        edge_predicate,
                        txn_id,
                        linearizable,
                    },
                )
                .await,
            )
        }
        NodedbStatement::Graph(GraphStmt::GraphNeighbors {
            collection,
            node,
            edge_labels,
            direction,
        }) => {
            // Read-your-own-writes for single-hop GRAPH reads needs the
            // session's active `TxnId` so the Data Plane merges this
            // transaction's staged edge writes (`GraphTxnOverlay`). Idle
            // sessions resolve to `None` (autocommit read).
            let (txn_id, _) = txn_ctx.sessions.txn_identity(txn_ctx.session_id);
            Some(
                traverse::neighbors(
                    state,
                    identity,
                    database_id,
                    traverse::NeighborsRequest {
                        collection,
                        node,
                        edge_labels,
                        direction,
                        txn_id,
                        linearizable,
                    },
                )
                .await,
            )
        }
        NodedbStatement::Graph(GraphStmt::GraphPath {
            collection,
            src,
            dst,
            max_depth,
            edge_labels,
            edge_predicate,
        }) => {
            // Every hop of the path search merges the session transaction's
            // staged edge writes. Idle sessions resolve to `None`.
            let (txn_id, _) = txn_ctx.sessions.txn_identity(txn_ctx.session_id);
            Some(
                traverse::shortest_path(
                    state,
                    identity,
                    database_id,
                    traverse::ShortestPathRequest {
                        collection,
                        src,
                        dst,
                        max_depth,
                        edge_labels,
                        edge_predicate,
                        txn_id,
                        linearizable,
                    },
                )
                .await,
            )
        }
        NodedbStatement::Graph(GraphStmt::GraphAlgo {
            algorithm,
            collection,
            edge_label,
            damping,
            tolerance,
            resolution,
            max_iterations,
            sample_size,
            source_node,
            direction,
            mode,
            personalization,
        }) => Some(
            algo::algo(
                state,
                identity,
                database_id,
                algo::AlgoRequest {
                    algorithm_name: &algorithm,
                    collection,
                    edge_label,
                    damping,
                    tolerance,
                    resolution,
                    max_iterations,
                    sample_size,
                    source_node,
                    direction,
                    mode,
                    personalization,
                    linearizable,
                },
            )
            .await,
        ),
        NodedbStatement::Graph(GraphStmt::GraphRagFusion { collection, params }) => Some(
            rag_fusion::rag_fusion(
                state,
                identity,
                database_id,
                collection,
                params,
                linearizable,
            )
            .await,
        ),
        NodedbStatement::Graph(GraphStmt::ShowGraphStats {
            collection,
            verbose,
            as_of,
        }) => Some(
            stats::show_graph_stats(
                state,
                identity,
                database_id,
                collection,
                verbose,
                as_of,
                linearizable,
            )
            .await,
        ),
        // `MatchQuery` (handled by the router's typed arm → neutral `match_ops`)
        // and every non-graph-overlay variant return None so the caller can route
        // them elsewhere.
        _ => None,
    }
}
