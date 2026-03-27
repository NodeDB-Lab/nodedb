//! Graph engine operations dispatched to the Data Plane.

use std::sync::Arc;

use crate::engine::graph::algo::params::{AlgoParams, GraphAlgorithm};
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;

/// Graph engine physical operations.
#[derive(Debug, Clone)]
pub enum GraphOp {
    /// Insert a graph edge with properties.
    EdgePut {
        src_id: String,
        label: String,
        dst_id: String,
        properties: Vec<u8>,
    },

    /// Delete a graph edge.
    EdgeDelete {
        src_id: String,
        label: String,
        dst_id: String,
    },

    /// Graph hop traversal: BFS from start nodes via label, bounded by depth.
    Hop {
        start_nodes: Vec<String>,
        edge_label: Option<String>,
        direction: Direction,
        depth: usize,
        options: GraphTraversalOptions,
    },

    /// Immediate 1-hop neighbors lookup.
    Neighbors {
        node_id: String,
        edge_label: Option<String>,
        direction: Direction,
    },

    /// Shortest path between two nodes.
    Path {
        src: String,
        dst: String,
        edge_label: Option<String>,
        max_depth: usize,
        options: GraphTraversalOptions,
    },

    /// Materialize a subgraph as edge tuples.
    Subgraph {
        start_nodes: Vec<String>,
        edge_label: Option<String>,
        depth: usize,
        options: GraphTraversalOptions,
    },

    /// GraphRAG fusion: vector search → graph expansion → RRF ranking.
    RagFusion {
        collection: String,
        query_vector: Arc<[f32]>,
        vector_top_k: usize,
        edge_label: Option<String>,
        direction: Direction,
        expansion_depth: usize,
        final_top_k: usize,
        /// RRF k constants: (vector_k, graph_k).
        rrf_k: (f64, f64),
        options: GraphTraversalOptions,
    },

    /// Graph algorithm execution (PageRank, WCC, SSSP, etc.).
    Algo {
        algorithm: GraphAlgorithm,
        params: AlgoParams,
    },

    /// Graph pattern matching (MATCH clause execution).
    Match {
        /// Serialized `MatchQuery` (MessagePack).
        query: Vec<u8>,
    },
}
