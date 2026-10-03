// SPDX-License-Identifier: BUSL-1.1

//! `cross_core_traverse_subgraph` — BFS that emits the `GRAPH TRAVERSE`
//! result: `{nodes:[{id,depth}], edges:[{from,to,label,properties}]}` JSON.
//!
//! Distinct from [`super::bfs::cross_core_bfs_with_options`]: that returns
//! only the visited node-id set (used by tree DDL aggregates that need a
//! flat reachable set). Both clients decode this shape with one strict
//! decoder, so a row of any other shape is an error on the client.
//!
//! The shared per-hop scatter/decode/merge logic lives in
//! [`super::hop::execute_neighbor_hop`]; this dispatcher layers depth
//! tagging and edge recording on top.

use std::collections::HashSet;

use nodedb_types::filter::MetadataFilter;
use sonic_rs;

use crate::bridge::envelope::Response;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TenantId, TxnId};

use super::bfs::{admit_by_name, walk_visit_cap, whole_hop};
use super::helpers::ok_response;
use super::hop::{NeighborHopParams, execute_neighbor_hop};
use super::neighbor_rows::NeighborRow;
use super::presence::{PresenceScope, graph_nodes_present};
use super::shard_reads::ShardReadLog;
use super::subgraph_edges::{EdgeOrientation, PhysicalEdges, WalkEdge};

/// Wire-shape JSON node entry.
#[derive(serde::Serialize)]
struct WireNode<'a> {
    id: &'a str,
    depth: u8,
}

/// Wire-shape JSON edge entry. `properties` is the edge's current property
/// object, `{}` for an edge without properties.
#[derive(serde::Serialize)]
struct WireEdge<'a> {
    from: &'a str,
    to: &'a str,
    label: &'a str,
    properties: serde_json::Value,
}

/// Wire-shape JSON envelope.
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
    /// Empty keeps every edge. Otherwise an edge with any listed label.
    pub edge_labels: &'a [String],
    pub direction: Direction,
    pub max_depth: usize,
    pub options: &'a GraphTraversalOptions,
    /// Each node that expands part of the walk confirms its groups first.
    pub linearizable: bool,
    /// AND-ed edge-property predicate. An edge it rejects is not crossed.
    /// Empty admits every edge. Non-empty requires `collection`.
    pub edge_predicate: &'a [MetadataFilter],
    /// Each recorded edge carries its property object. Requires
    /// `collection`.
    pub with_properties: bool,
    /// The session's transaction. The walk, its predicate and the returned
    /// properties see its staged edge writes.
    pub txn_id: Option<TxnId>,
}

/// BFS that returns a `{nodes,edges}` JSON subgraph for `GRAPH TRAVERSE`.
///
/// A start no edge names is absent from the graph: the result is
/// `{nodes:[], edges:[]}`.
///
/// The walk runs level by level, as one core's subgraph does
/// (`CsrIndex::subgraph`): each hop records every edge of the frontier, and
/// the level's new nodes are admitted in node-name order until the visit cap.
/// The last admitted level is not expanded: it records only its edges to
/// admitted nodes.
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
    let SubgraphWalk { node_order, edges } = walk_subgraph(shared, params).await?;

    let wire_nodes: Vec<WireNode<'_>> = node_order
        .iter()
        .map(|(id, depth)| WireNode {
            id: id.as_str(),
            depth: *depth,
        })
        .collect();
    let wire_edges: Vec<WireEdge<'_>> = edges.iter().map(wire_edge).collect();
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

/// One recorded edge as its wire entry. An edge read without properties
/// carries `{}`.
fn wire_edge(edge: &WalkEdge) -> WireEdge<'_> {
    let properties = edge
        .properties
        .clone()
        .map(|fields| serde_json::Value::from(nodedb_types::Value::Object(fields)))
        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    WireEdge {
        from: edge.src.as_str(),
        to: edge.dst.as_str(),
        label: edge.label.as_str(),
        properties,
    }
}

/// What a subgraph walk found: every visited node in discovery order with
/// the hop at which it was first reached, and every edge crossed.
pub(crate) struct SubgraphWalk {
    pub node_order: Vec<(String, u8)>,
    pub edges: Vec<WalkEdge>,
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
        edge_labels,
        direction,
        max_depth,
        options,
        linearizable,
        edge_predicate,
        with_properties,
        txn_id,
    } = params;
    // Per-node depth: the start node is at depth 0. Each later node carries
    // the hop index that first surfaced it.
    let mut visited: HashSet<String> = HashSet::new();
    let mut node_order: Vec<(String, u8)> = Vec::new();
    let mut edges = PhysicalEdges::default();
    let mut frontier: Vec<String> = vec![start.clone()];
    let mut reads = ShardReadLog::new();
    let cap = walk_visit_cap(shared, options);
    let whole = whole_hop();
    let hop = WalkHop {
        tenant_id,
        database_id,
        collection: collection.as_deref(),
        edge_labels,
        direction,
        options: &whole,
        linearizable,
        edge_predicate,
        with_properties,
        txn_id,
    };

    // One core's subgraph is empty for a start its graph does not hold, as is
    // `GRAPH PATH` for an absent endpoint. A start the graph holds is in the
    // result even when no edge passes the walk's label or predicate filter.
    // The presence read spans every collection, so it joins the read-set
    // unscoped.
    let mut presence_reads = ShardReadLog::new();
    let present = graph_nodes_present(
        shared,
        PresenceScope {
            tenant_id,
            database_id,
            options: &whole,
            linearizable,
            txn_id,
        },
        std::slice::from_ref(&start),
        &mut presence_reads,
    )
    .await?;
    presence_reads.publish(shared, tenant_id, database_id, None);
    if !present.contains(&start) {
        return Ok(SubgraphWalk {
            node_order: Vec::new(),
            edges: Vec::new(),
        });
    }

    visited.insert(start.clone());
    node_order.push((start, 0));

    for hop_idx in 0..max_depth {
        if frontier.is_empty() || node_order.len() >= cap {
            break;
        }

        // Every pass expands the whole frontier. Each edge is recorded even
        // when its other endpoint is already visited: an A→B→C graph with a
        // back-edge B→A surfaces that edge once.
        let mut destinations: Vec<String> = Vec::new();
        for (pass, rows) in hop
            .expand(shared, &frontier, node_order.len(), &mut reads)
            .await?
        {
            destinations.extend(rows.iter().map(|row| row.node.clone()));
            edges.record(rows, pass);
        }

        // Admit the level's new nodes in name order under the cap, and tag
        // them with the current hop's depth. `hop_idx=0` expands the depth-0
        // start node into depth-1 neighbors.
        let next_depth_tag = (hop_idx + 1).min(u8::MAX as usize) as u8;
        frontier = admit_by_name(destinations, &mut visited, node_order.len(), cap);
        node_order.extend(frontier.iter().map(|node| (node.clone(), next_depth_tag)));
    }

    // The last admitted level is never expanded. Its edges to admitted
    // nodes, itself included, are still part of the subgraph.
    if !frontier.is_empty() {
        for (pass, mut rows) in hop
            .expand(shared, &frontier, node_order.len(), &mut reads)
            .await?
        {
            rows.retain(|row| visited.contains(&row.node));
            edges.record(rows, pass);
        }
    }

    // Every vShard the walk expanded joins the transaction read-set.
    reads.publish(shared, tenant_id, database_id, collection);
    Ok(SubgraphWalk {
        node_order,
        edges: edges.into_vec(),
    })
}

/// The scope every hop of one subgraph walk expands in.
struct WalkHop<'a> {
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: Option<&'a str>,
    edge_labels: &'a [String],
    direction: Direction,
    options: &'a GraphTraversalOptions,
    linearizable: bool,
    edge_predicate: &'a [MetadataFilter],
    with_properties: bool,
    txn_id: Option<TxnId>,
}

impl WalkHop<'_> {
    /// Expand `frontier` once per pass of the walk's direction. A row carries
    /// no orientation, so each direction runs as its own pass. Each pass's
    /// rows are sorted, so the edges come back in the same order on every
    /// run whatever order cores and owners answered in.
    async fn expand(
        &self,
        shared: &SharedState,
        frontier: &[String],
        discovered_so_far: usize,
        reads: &mut ShardReadLog,
    ) -> crate::Result<Vec<(EdgeOrientation, Vec<NeighborRow>)>> {
        let mut passes = Vec::with_capacity(2);
        for &pass in EdgeOrientation::passes(self.direction) {
            let hop = execute_neighbor_hop(
                shared,
                self.tenant_id,
                self.database_id,
                NeighborHopParams {
                    collection: self.collection,
                    frontier,
                    edge_labels: self.edge_labels,
                    direction: pass.direction(),
                    options: self.options,
                    discovered_so_far,
                    linearizable: self.linearizable,
                    edge_predicate: self.edge_predicate,
                    with_properties: self.with_properties,
                    txn_id: self.txn_id,
                },
            )
            .await?;
            reads.merge(hop.reads);
            let mut rows = hop.rows;
            rows.sort_by(|a, b| (&a.src, &a.label, &a.node).cmp(&(&b.src, &b.label, &b.node)));
            passes.push((pass, rows));
        }
        Ok(passes)
    }
}
