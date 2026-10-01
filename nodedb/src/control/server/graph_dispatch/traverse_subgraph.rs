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
//! [`super::hop::execute_neighbor_hop`]; this dispatcher layers depth
//! tagging and edge recording on top.

use std::collections::{HashMap, HashSet};

use sonic_rs;

use crate::bridge::envelope::Response;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TenantId};

use super::bfs::{admit_by_name, walk_visit_cap, whole_hop};
use super::helpers::ok_response;
use super::hop::{NeighborHopParams, execute_neighbor_hop};
use super::shard_reads::ShardReadLog;
use super::subgraph_edges::{EdgeOrientation, PhysicalEdges};

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

/// Wire-shape JSON envelope. The client decoder calls
/// `parsed.get("nodes")` and `parsed.get("edges")`; a flat array or any
/// other key set decodes to an empty `SubGraph`, which is the visible
/// failure mode the regression test in
/// `nodedb-client-tests/tests/graph_traverse_remote_round_trip.rs`
/// guards against.
#[derive(serde::Serialize)]
struct WireSubGraph<'a> {
    nodes: Vec<WireNode<'a>>,
    edges: Vec<WireEdge<'a>>,
}

/// Parameters for [`cross_core_traverse_subgraph`].
pub struct CrossCoreTraverseSubgraphParams<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// Database-qualified collection scope, or `None` for a label-only
    /// traversal.
    pub collection: Option<String>,
    pub start: String,
    pub edge_label: Option<String>,
    pub direction: Direction,
    pub max_depth: usize,
    pub options: &'a GraphTraversalOptions,
    /// Each node that expands part of the walk confirms its groups first.
    pub linearizable: bool,
}

/// BFS that returns a `{nodes,edges}` JSON subgraph for `GRAPH TRAVERSE`.
///
/// The walk runs level by level, as one core's subgraph does
/// (`CsrIndex::subgraph`): each hop records every edge of the frontier, and
/// the level's new nodes are admitted in node-name order until the visit cap.
///
/// Each hop expands every frontier node at the node that owns
/// `from_key(node)` via the shared [`execute_neighbor_hop`] helper and
/// records:
///   * each newly-visited node (with its discovery depth), and
///   * each physical `(src, label, dst)` edge the hop crossed, once. An
///     incoming edge keeps its physical orientation. A `Both` hop expands
///     outgoing, then incoming, edges over the same frontier.
pub async fn cross_core_traverse_subgraph(
    shared: &SharedState,
    params: CrossCoreTraverseSubgraphParams<'_>,
) -> crate::Result<Response> {
    let SubgraphWalk {
        node_order,
        depth_of,
        edges,
    } = walk_subgraph(shared, params).await?;

    let wire_nodes: Vec<WireNode<'_>> = node_order
        .iter()
        .map(|id| WireNode {
            id: id.as_str(),
            depth: *depth_of.get(id).unwrap_or(&0),
        })
        .collect();
    let wire_edges: Vec<WireEdge<'_>> = edges
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

/// What a subgraph walk found: every visited node in discovery order, the
/// hop at which each was first reached, and every `(src, label, dst)` edge
/// crossed.
pub(crate) struct SubgraphWalk {
    pub node_order: Vec<String>,
    pub depth_of: HashMap<String, u8>,
    pub edges: Vec<(String, String, String)>,
}

/// Walk the subgraph around `params.start`, expanding every frontier node at
/// the node that owns `from_key(node)`.
pub(crate) async fn walk_subgraph(
    shared: &SharedState,
    params: CrossCoreTraverseSubgraphParams<'_>,
) -> crate::Result<SubgraphWalk> {
    let CrossCoreTraverseSubgraphParams {
        tenant_id,
        collection,
        database_id,
        start,
        edge_label,
        direction,
        max_depth,
        options,
        linearizable,
    } = params;
    // Per-node depth: the start node is at depth 0; subsequent nodes
    // are tagged with the hop index that first surfaced them.
    let mut depth_of: HashMap<String, u8> = HashMap::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut node_order: Vec<String> = Vec::new();
    let mut edges = PhysicalEdges::default();
    let mut frontier: Vec<String> = vec![start.clone()];
    let mut reads = ShardReadLog::new();
    let cap = walk_visit_cap(shared, options);
    let whole = whole_hop();

    visited.insert(start.clone());
    depth_of.insert(start.clone(), 0);
    node_order.push(start);

    for hop_idx in 0..max_depth {
        if frontier.is_empty() || node_order.len() >= cap {
            break;
        }

        // Every pass expands the whole frontier. A row carries no
        // orientation, so each direction runs as its own pass.
        let mut destinations: Vec<String> = Vec::new();
        for &pass in EdgeOrientation::passes(direction) {
            let hop = execute_neighbor_hop(
                shared,
                tenant_id,
                database_id,
                NeighborHopParams {
                    collection: collection.as_deref(),
                    frontier: &frontier,
                    edge_label: edge_label.as_deref(),
                    direction: pass.direction(),
                    options: &whole,
                    discovered_so_far: node_order.len(),
                    linearizable,
                },
            )
            .await?;
            // Every frontier node was expanded at its owner. Each edge is
            // recorded even when its other endpoint is already visited: an
            // A→B→C graph with a back-edge B→A surfaces that edge once.
            edges.record(hop.local_triples, pass);
            destinations.extend(hop.merged_destinations);
            reads.merge(hop.reads);
        }

        // Admit the level's new nodes in name order under the cap, and tag
        // them with the current hop's depth. `hop_idx=0` expands the depth-0
        // start node into depth-1 neighbors.
        let next_depth_tag = (hop_idx + 1).min(u8::MAX as usize) as u8;
        frontier = admit_by_name(destinations, &mut visited, &mut node_order, cap);
        for node in &frontier {
            depth_of.insert(node.clone(), next_depth_tag);
        }
    }

    // Every vShard the walk expanded joins the transaction read-set.
    reads.publish(shared, tenant_id, database_id, collection);
    Ok(SubgraphWalk {
        node_order,
        depth_of,
        edges: edges.into_vec(),
    })
}
