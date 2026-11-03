// SPDX-License-Identifier: Apache-2.0

//! The graph half of the client interface: `NodeDbGraph`, a supertrait of
//! [`NodeDb`](super::NodeDb).
//!
//! Every `NodeDb` implementation also implements `NodeDbGraph`. Generic code
//! bound on `NodeDb`, and `dyn NodeDb`, reach these methods without a second
//! import. A call on a concrete client type imports `NodeDbGraph`.

use async_trait::async_trait;

use nodedb_types::{
    document::Document, error::NodeDbResult, filter::EdgeFilter, graph::GraphStats, id::EdgeId,
    id::NodeId, protocol::EdgeWrite, result::SubGraph,
};

use super::default_impls;
use super::marker::NodeDbMarker;

/// Graph operations of the unified database interface. See
/// [`NodeDb`](super::NodeDb) for the implementations.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait NodeDbGraph: NodeDbMarker {
    /// Traverse the graph from `start` up to `depth` hops within `collection`.
    ///
    /// Edges are scoped per collection, so the caller picks which graph to
    /// walk. Returns the discovered subgraph (nodes + edges). `direction`
    /// selects outgoing, incoming, or both orientations.
    ///
    /// Every label of the filter is followed. Every property filter must
    /// admit an edge before it is crossed. Each returned edge carries its
    /// properties.
    ///
    /// On Lite: CSR pointer-chasing in contiguous memory.
    /// On Remote and Native: `GRAPH TRAVERSE IN '<collection>' FROM '<start>'
    /// DEPTH <n> DIRECTION out|in|both [LABEL '<a>', '<b>' …]
    /// [EDGE WHERE <property filters>]`, one round trip.
    async fn graph_traverse(
        &self,
        collection: &str,
        start: &NodeId,
        depth: u8,
        direction: nodedb_types::graph::Direction,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph>;

    /// Insert a directed edge `from` → `to` with label `edge_type` into
    /// `collection`. Returns the generated edge ID.
    ///
    /// On Lite: mutable adjacency buffer + CRDT delta + local storage.
    /// On Remote: `GRAPH INSERT EDGE IN '<collection>' FROM '<from>' TO '<to>' TYPE '<label>'`.
    async fn graph_insert_edge(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId>;

    /// Delete a graph edge by ID from `collection`.
    ///
    /// On Lite: marks deleted + CRDT tombstone.
    /// On Remote: `GRAPH DELETE EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>'`.
    async fn graph_delete_edge(&self, collection: &str, edge_id: &EdgeId) -> NodeDbResult<()>;

    /// Read aggregated graph statistics from the persisted edge store.
    ///
    /// `Some(name)` returns zero or one entry for that collection. `None`
    /// returns one entry per collection that has edges. Each entry holds the
    /// edge count, distinct node count, distinct label count, and per-label
    /// edge counts sorted by label.
    ///
    /// `as_of` pins the read to a past system time (ms since Unix epoch).
    /// `None` reads the live state. Lite returns an error for `Some`.
    ///
    /// On Lite: direct read of the local edge store.
    /// On Remote: `SHOW GRAPH STATS [<'collection'>] [AS OF SYSTEM TIME <ms>]`.
    async fn graph_stats(
        &self,
        collection: Option<&str>,
        as_of: Option<i64>,
    ) -> NodeDbResult<Vec<GraphStats>>;

    /// Run PageRank on a graph collection's edges.
    ///
    /// `personalization` biases the initial rank toward its seed nodes
    /// (values are normalized). `None` runs uniform-init PageRank.
    /// `damping` defaults to 0.85, `max_iterations` to 20.
    ///
    /// Returns `(node_id, rank)` pairs sorted by rank descending. Ranks sum to 1.0.
    async fn graph_pagerank(
        &self,
        collection: &str,
        personalization: Option<std::collections::HashMap<String, f64>>,
        damping: Option<f64>,
        max_iterations: Option<u32>,
    ) -> NodeDbResult<Vec<(String, f64)>> {
        default_impls::graph_pagerank_default(collection, personalization, damping, max_iterations)
    }

    /// Insert `edges` into `collection` as one unit. Returns the edges
    /// written.
    ///
    /// Each edge carries its own properties and valid time. Every edge
    /// lands, or none does. A call over the edge cap is refused whole:
    /// [`MAX_EDGES_PER_STATEMENT`](nodedb_types::protocol::MAX_EDGES_PER_STATEMENT).
    /// A write policy decides each edge on its properties, and one refused
    /// edge refuses the call.
    ///
    /// On Lite: one local unit over the adjacency buffer and CRDT state.
    /// On Remote: one `GRAPH INSERT EDGE IN '<collection>' FROM .. TO ..
    /// TYPE .. [PROPERTIES '<json>'] [VALID_TIME <from> [UNTIL <until>]] ..`
    /// statement.
    /// On Native: one `OpCode::EdgePutBatch` request.
    async fn graph_insert_edges(&self, collection: &str, edges: &[EdgeWrite]) -> NodeDbResult<u64>;

    /// Delete `edges` from `collection` as one unit. Returns the edges that
    /// were live and are now deleted. `seq` of each id is ignored: an edge is
    /// named by its endpoints and label.
    ///
    /// A call over the edge cap is refused whole:
    /// [`MAX_EDGES_PER_STATEMENT`](nodedb_types::protocol::MAX_EDGES_PER_STATEMENT).
    /// A write policy decides each edge on its stored properties, and one
    /// refused edge refuses the call.
    ///
    /// On Lite: one local unit over the adjacency buffer and CRDT state.
    /// On Remote: one `GRAPH DELETE EDGE IN '<collection>' FROM .. TO ..
    /// TYPE .. ..` statement.
    /// On Native: one `OpCode::EdgeDeleteBatch` request.
    async fn graph_delete_edges(&self, collection: &str, edges: &[EdgeId]) -> NodeDbResult<u64>;

    /// Find the shortest path between two nodes.
    ///
    /// Returns node IDs from `from` to `to`, or `None` when no path exists
    /// within `max_depth` hops. The path follows outgoing edges the filter
    /// admits.
    ///
    /// The default is a breadth-first search, one depth-1 `graph_traverse`
    /// per expanded node.
    /// On Remote and Native: `GRAPH PATH IN '<collection>' FROM '<from>'
    /// TO '<to>' MAX_DEPTH <n> [LABEL '<a>', '<b>' …]
    /// [EDGE WHERE <property filters>]`, one round trip.
    async fn graph_shortest_path(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        max_depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<Option<Vec<NodeId>>> {
        default_impls::graph_shortest_path_default(
            self,
            collection,
            from,
            to,
            max_depth,
            edge_filter,
        )
        .await
    }

    /// Insert `edges` `(from, to, label)` into `collection` for a bulk load.
    ///
    /// The default sends them through [`NodeDbGraph::graph_insert_edges`] in
    /// chunks at the edge cap:
    /// [`MAX_EDGES_PER_STATEMENT`](nodedb_types::protocol::MAX_EDGES_PER_STATEMENT).
    /// Each chunk is one unit. A refused chunk stops the load, and the chunks
    /// before it stay.
    async fn batch_graph_insert_edges(
        &self,
        collection: &str,
        edges: &[(&str, &str, &str)],
    ) -> NodeDbResult<()> {
        default_impls::batch_graph_insert_edges_default(self, collection, edges).await
    }
}
