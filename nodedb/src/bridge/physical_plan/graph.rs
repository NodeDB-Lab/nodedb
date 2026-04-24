//! Graph engine operations dispatched to the Data Plane.

use crate::engine::graph::algo::params::{AlgoParams, GraphAlgorithm};
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;

/// One edge in an `EdgePutBatch` / `EdgeDeleteBatch`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BatchEdge {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
}

/// Graph engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum GraphOp {
    /// Insert a graph edge with properties.
    EdgePut {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        properties: Vec<u8>,
    },

    /// Batched edge insert: many `(collection, src, label, dst)` tuples.
    /// Every edge in the batch must target the same collection — the
    /// batch is a unit of work, not a cross-collection scatter.
    EdgePutBatch { edges: Vec<BatchEdge> },

    /// Delete a graph edge.
    EdgeDelete {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
    },

    /// Batched edge delete: used to revert a partial `EdgePutBatch` on
    /// failure so the DDL leaves no stranded edges.
    EdgeDeleteBatch { edges: Vec<BatchEdge> },

    /// Graph hop traversal: BFS from start nodes via label, bounded by depth.
    Hop {
        start_nodes: Vec<String>,
        edge_label: Option<String>,
        direction: Direction,
        depth: usize,
        options: GraphTraversalOptions,
        /// RLS filters applied to traversed nodes before returning.
        rls_filters: Vec<u8>,
    },

    /// Immediate 1-hop neighbors lookup.
    Neighbors {
        node_id: String,
        edge_label: Option<String>,
        direction: Direction,
        /// RLS filters applied to neighbor nodes before returning.
        rls_filters: Vec<u8>,
    },

    /// Batched 1-hop neighbors lookup: one RPC per hop of a BFS frontier
    /// instead of one RPC per frontier node. Returns
    /// `[{ src, label, node }, ...]` so the caller can attribute each
    /// neighbor to its origin (needed for shortest-path parent pointers).
    ///
    /// `max_results` is the per-RPC cap: the Data Plane handler stops
    /// emitting entries once the batch reaches this size so a single
    /// wide hop cannot allocate past the caller's budget. `0` means
    /// unbounded (use with care).
    NeighborsMulti {
        node_ids: Vec<String>,
        edge_label: Option<String>,
        direction: Direction,
        max_results: u32,
        /// RLS filters applied to neighbor nodes before returning.
        rls_filters: Vec<u8>,
    },

    /// Shortest path between two nodes.
    Path {
        src: String,
        dst: String,
        edge_label: Option<String>,
        max_depth: usize,
        options: GraphTraversalOptions,
        /// RLS filters applied to path nodes before returning.
        rls_filters: Vec<u8>,
    },

    /// Materialize a subgraph as edge tuples.
    Subgraph {
        start_nodes: Vec<String>,
        edge_label: Option<String>,
        depth: usize,
        options: GraphTraversalOptions,
        /// RLS filters applied to subgraph nodes/edges before returning.
        rls_filters: Vec<u8>,
    },

    /// GraphRAG fusion: vector search → graph expansion → RRF ranking.
    RagFusion {
        collection: String,
        query_vector: Vec<f32>,
        vector_top_k: usize,
        edge_label: Option<String>,
        direction: Direction,
        expansion_depth: usize,
        final_top_k: usize,
        /// RRF k constants: (vector_k, graph_k).
        rrf_k: (f64, f64),
        /// Vector index field name. Empty string selects the raw (field-less)
        /// index created via `VectorOp::Insert`; a non-empty value selects
        /// the field-backed index created when documents are inserted with an
        /// embedded vector column (e.g. `INSERT INTO col (id, embedding) VALUES …`).
        vector_field: String,
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

    /// Set node labels (bitset-based, up to 64 distinct labels).
    SetNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },

    /// Remove node labels.
    RemoveNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },

    /// Bitemporal 1-hop neighbors lookup.
    ///
    /// Resolves edges whose latest version with `system_from <= system_as_of_ms`
    /// (converted to HLC ordinal) is not a sentinel, optionally also filtering
    /// by `valid_from_ms <= valid_at_ms < valid_until_ms`. The handler calls
    /// [`ceiling_resolve_edge`](crate::engine::graph::edge_store::EdgeStore::ceiling_resolve_edge)
    /// per candidate base edge.
    TemporalNeighbors {
        /// Edge store is collection-scoped; current-state `Neighbors` reads
        /// the tenant-wide CSR, but the versioned key layout is
        /// `{collection}\x00...`, so the bitemporal path must name the
        /// collection explicitly.
        collection: String,
        node_id: String,
        edge_label: Option<String>,
        direction: Direction,
        /// System-time cutoff in ms. `None` falls back to current-state
        /// semantics identical to `Neighbors`.
        system_as_of_ms: Option<i64>,
        /// Optional valid-time point. Skipped when `None`.
        valid_at_ms: Option<i64>,
        rls_filters: Vec<u8>,
    },

    /// Bitemporal graph algorithm execution.
    ///
    /// Identical to `Algo` but builds its CSR snapshot via
    /// [`CsrSnapshot::from_edge_store_as_of`](crate::engine::graph::olap::snapshot::CsrSnapshot)
    /// at the given system-time cutoff before running the algorithm.
    TemporalAlgorithm {
        algorithm: GraphAlgorithm,
        params: AlgoParams,
        /// System-time cutoff in ms. `None` means current state (equivalent
        /// to plain `Algo`).
        system_as_of_ms: Option<i64>,
    },
}
