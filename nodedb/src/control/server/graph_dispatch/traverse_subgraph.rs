// SPDX-License-Identifier: BUSL-1.1

//! `cross_core_traverse_subgraph` — BFS that emits a wire-shape subgraph
//! matching what `nodedb-client`'s remote `graph_traverse` parses.
//!
//! Distinct from [`super::bfs::cross_core_bfs_with_options`]: that returns
//! only the visited node-id set (used by tree DDL aggregates that need a
//! flat reachable set). The remote client's `graph_traverse` trait method
//! parses a `{nodes:[{id,depth}], edges:[{from,to,label}]}` JSON object —
//! anything else (a bare array, or a `{visited: [...]}`-shaped object)
//! decodes to an empty `SubGraph`. This dispatcher emits exactly the
//! shape the client decoder expects so a same-session insert is visible
//! to a same-session traverse.
//!
//! The shared per-hop scatter/decode/merge logic lives in
//! [`super::hop::execute_neighbor_hop_bounded`]; this dispatcher layers depth
//! tagging and edge recording on top.

use sonic_rs;

use crate::bridge::envelope::Response;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TenantId};

use super::helpers::ok_response;
use super::hop::{NeighborHopParams, execute_neighbor_hop_bounded};
use super::subgraph_accumulator::{EdgeOrientation, RowAllowance, SubgraphAccumulator};

/// Wire-shape JSON node entry. Field names mirror the client decoder in
/// `nodedb-client/src/remote/parse.rs::parse_graph_traverse_json`.
#[derive(serde::Serialize)]
struct WireNode<'a> {
    id: &'a str,
    depth: u8,
}

/// Wire-shape JSON edge entry. Field names mirror the client decoder.
#[derive(serde::Serialize)]
struct WireEdge<'a> {
    from: &'a str,
    to: &'a str,
    label: &'a str,
}

/// Wire JSON envelope with node IDs, discovery depths, and physical edge endpoints.
#[derive(serde::Serialize)]
struct WireSubGraph<'a> {
    nodes: Vec<WireNode<'a>>,
    edges: Vec<WireEdge<'a>>,
}

/// Parameters for [`cross_core_traverse_subgraph`].
pub struct CrossCoreTraverseSubgraphParams<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// Collection scope, or `None` for a label-only traversal.
    pub collection: Option<String>,
    pub start: String,
    pub edge_label: Option<String>,
    pub direction: Direction,
    pub max_depth: usize,
    pub options: &'a GraphTraversalOptions,
}

/// BFS that returns a `{nodes,edges}` JSON subgraph for `GRAPH TRAVERSE`.
///
/// Incoming rows retain their physical edge orientation. Both-direction traversal
/// expands outgoing then incoming edges over the same frontier with one raw-row allowance.
/// Node admission enforces `max_visited` across shard results. Physical edges appear once.
pub async fn cross_core_traverse_subgraph(
    shared: &SharedState,
    params: CrossCoreTraverseSubgraphParams<'_>,
) -> crate::Result<Response> {
    let CrossCoreTraverseSubgraphParams {
        tenant_id,
        collection,
        database_id,
        start,
        edge_label,
        direction,
        max_depth,
        options,
    } = params;
    let mut frontier = if options.max_visited > 0 {
        vec![start.clone()]
    } else {
        Vec::new()
    };
    let mut state = SubgraphAccumulator::new(start, options.max_visited);
    for hop_idx in 0..max_depth {
        if frontier.is_empty() || state.remaining_nodes() == 0 {
            break;
        }
        let mut allowance = RowAllowance::new(state.remaining_nodes());
        let directions: &[EdgeOrientation] = match direction {
            Direction::Out => &[EdgeOrientation::Out],
            Direction::In => &[EdgeOrientation::In],
            Direction::Both => &[EdgeOrientation::Out, EdgeOrientation::In],
        };
        let mut next_frontier = Vec::new();
        for &pass_direction in directions {
            let Some(limit) = allowance.dispatch_limit() else {
                break;
            };
            let triples = execute_neighbor_hop_bounded(
                shared,
                tenant_id,
                database_id,
                NeighborHopParams {
                    collection: collection.as_deref(),
                    frontier: &frontier,
                    edge_label: edge_label.as_deref(),
                    direction: pass_direction.direction(),
                    options,
                    discovered_so_far: state.nodes.len(),
                },
                limit,
            )
            .await?;
            allowance.consume(triples.len());
            state.record(
                triples,
                pass_direction,
                (hop_idx + 1).min(u8::MAX as usize) as u8,
                &mut next_frontier,
            );
        }
        frontier = next_frontier;
    }

    let wire_nodes: Vec<WireNode<'_>> = state
        .nodes
        .iter()
        .map(|(id, depth)| WireNode {
            id: id.as_str(),
            depth: *depth,
        })
        .collect();
    let wire_edges: Vec<WireEdge<'_>> = state
        .edges
        .iter()
        .map(|(src, label, dst)| WireEdge {
            from: src.as_str(),
            to: dst.as_str(),
            label: label.as_str(),
        })
        .collect();
    let envelope = WireSubGraph {
        nodes: wire_nodes,
        edges: wire_edges,
    };

    let payload = sonic_rs::to_vec(&envelope).map_err(|e| crate::Error::Serialization {
        format: "json".into(),
        detail: e.to_string(),
    })?;

    Ok(ok_response(payload))
}
