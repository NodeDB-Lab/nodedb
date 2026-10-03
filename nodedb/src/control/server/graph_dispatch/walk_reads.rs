// SPDX-License-Identifier: BUSL-1.1

//! Multi-hop graph reads sent as a single plan (`Hop`, `Path`, `Subgraph`),
//! run through the cross-shard walk coordinators.
//!
//! In a cluster, one plan on one node walks only that node's partitions. Each
//! of these reads instead runs the hop-by-hop walk the SQL surface uses: every
//! frontier node expands at the node that owns its key vShard. The answer is
//! returned in the payload shape the Data Plane gives for the same plan, so a
//! protocol shapes it as before:
//!
//! - `Hop`: a msgpack array of every reached node, start nodes first.
//! - `Subgraph`: a msgpack array of `{src, label, dst}` edges. The last
//!   admitted level records its edges to admitted nodes.
//! - `Path`: a msgpack array `[src, …, dst]`, or a `NotFound` refusal when
//!   `dst` is unreachable.

use nodedb_physical::physical_plan::GraphOp;

use crate::bridge::envelope::{Payload, PhysicalPlan, Response};
use crate::control::server::dispatch_utils::{not_found_response, ok_payload_response};
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::types::{DatabaseId, TenantId, TxnId};

use super::bfs::{CrossCoreBfsParams, cross_core_bfs_with_options};
use super::shortest_path::{CrossCoreShortestPathParams, cross_core_shortest_path};
use super::traverse_subgraph::{CrossCoreTraverseSubgraphParams, SubgraphWalk, walk_subgraph};

/// One subgraph edge, in the Data Plane's `Subgraph` response shape.
#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct SubgraphEdgeWire<'a> {
    src: &'a str,
    label: &'a str,
    dst: &'a str,
}

/// Run `plan` through the walk coordinators when it is a `Hop`, `Path` or
/// `Subgraph`. Returns `None` for every other plan. The walk sees the staged
/// edge writes of `txn_id`, the session's transaction.
pub async fn serve_walk_plan(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
    txn_id: Option<TxnId>,
    linearizable: bool,
) -> Option<crate::Result<Response>> {
    let PhysicalPlan::Graph(op) = plan else {
        return None;
    };
    Some(match op {
        GraphOp::Hop {
            collection,
            start_nodes,
            edge_labels,
            direction,
            depth,
            options,
            ..
        } => cross_core_bfs_with_options(
            state,
            CrossCoreBfsParams {
                tenant_id,
                database_id,
                collection: collection.as_ref().map(|c| c.as_str()),
                start_nodes: start_nodes.clone(),
                edge_labels,
                direction: *direction,
                max_depth: *depth,
                options,
                linearizable,
                txn_id,
            },
        )
        .await
        .and_then(|response| rewrap_as_msgpack(&response.payload)),
        GraphOp::Subgraph {
            collection,
            start_nodes,
            edge_labels,
            depth,
            options,
            ..
        } => {
            subgraph(
                state,
                tenant_id,
                database_id,
                SubgraphRequest {
                    collection: collection.as_ref().map(|c| c.as_str().to_owned()),
                    start_nodes,
                    edge_labels,
                    depth: *depth,
                    options,
                    linearizable,
                    txn_id,
                },
            )
            .await
        }
        GraphOp::Path {
            collection,
            src,
            dst,
            edge_labels,
            max_depth,
            options,
            ..
        } => {
            path(
                state,
                tenant_id,
                database_id,
                PathRequest {
                    collection: collection.as_ref(),
                    src,
                    dst,
                    edge_labels,
                    max_depth: *max_depth,
                    options,
                    linearizable,
                    txn_id,
                },
            )
            .await
        }
        _ => return None,
    })
}

/// The inputs of a subgraph walk sent as one plan.
struct SubgraphRequest<'a> {
    collection: Option<String>,
    start_nodes: &'a [String],
    edge_labels: &'a [String],
    depth: usize,
    options: &'a crate::engine::graph::traversal_options::GraphTraversalOptions,
    linearizable: bool,
    txn_id: Option<TxnId>,
}

async fn subgraph(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    request: SubgraphRequest<'_>,
) -> crate::Result<Response> {
    let [start] = request.start_nodes else {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "a subgraph walk starts from one node, got {}",
                request.start_nodes.len()
            ),
        });
    };
    // The Data Plane's subgraph is the out-edge closure of its start node.
    let SubgraphWalk { edges, .. } = walk_subgraph(
        state,
        CrossCoreTraverseSubgraphParams {
            tenant_id,
            database_id,
            collection: request.collection,
            start: start.clone(),
            edge_labels: request.edge_labels,
            direction: Direction::Out,
            max_depth: request.depth,
            options: request.options,
            linearizable: request.linearizable,
            edge_predicate: &[],
            with_properties: false,
            txn_id: request.txn_id,
        },
    )
    .await?;
    let wire: Vec<SubgraphEdgeWire<'_>> = edges
        .iter()
        .map(|edge| SubgraphEdgeWire {
            src: edge.src.as_str(),
            label: edge.label.as_str(),
            dst: edge.dst.as_str(),
        })
        .collect();
    let payload = zerompk::to_msgpack_vec(&wire).map_err(|e| crate::Error::Codec {
        detail: format!("subgraph walk encode: {e}"),
    })?;
    Ok(ok_payload_response(Payload::from_vec(payload)))
}

/// The inputs of a shortest-path walk sent as one plan.
struct PathRequest<'a> {
    collection: Option<&'a nodedb_types::QualifiedCollection>,
    src: &'a str,
    dst: &'a str,
    edge_labels: &'a [String],
    max_depth: usize,
    options: &'a crate::engine::graph::traversal_options::GraphTraversalOptions,
    linearizable: bool,
    txn_id: Option<TxnId>,
}

async fn path(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    request: PathRequest<'_>,
) -> crate::Result<Response> {
    // A path with no collection walks every collection's edges, as the Data
    // Plane's path does.
    let response = cross_core_shortest_path(
        state,
        CrossCoreShortestPathParams {
            tenant_id,
            database_id,
            collection: request.collection.map(|c| c.as_str().to_owned()),
            src: request.src.to_owned(),
            dst: request.dst.to_owned(),
            edge_labels: request.edge_labels.to_vec(),
            max_depth: request.max_depth,
            options: request.options.clone(),
            linearizable: request.linearizable,
            edge_predicate: Vec::new(),
            txn_id: request.txn_id,
        },
    )
    .await?;
    let path: Vec<String> =
        sonic_rs::from_slice(response.payload.as_ref()).map_err(|e| crate::Error::Codec {
            detail: format!("graph path decode: {e}"),
        })?;
    // The Data Plane refuses an unreachable destination with `NotFound`.
    if path.is_empty() {
        return Ok(not_found_response());
    }
    encode_names(&path)
}

/// Re-encode a JSON array of node names as the msgpack array the Data Plane
/// returns.
fn rewrap_as_msgpack(payload: &Payload) -> crate::Result<Response> {
    let names: Vec<String> =
        sonic_rs::from_slice(payload.as_ref()).map_err(|e| crate::Error::Codec {
            detail: format!("graph walk decode: {e}"),
        })?;
    encode_names(&names)
}

fn encode_names(names: &[String]) -> crate::Result<Response> {
    let payload = zerompk::to_msgpack_vec(&names.to_vec()).map_err(|e| crate::Error::Codec {
        detail: format!("graph walk encode: {e}"),
    })?;
    Ok(ok_payload_response(Payload::from_vec(payload)))
}
