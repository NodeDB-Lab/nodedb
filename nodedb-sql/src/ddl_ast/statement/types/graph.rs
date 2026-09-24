// SPDX-License-Identifier: Apache-2.0

//! Graph DDL/DML statements.

use crate::ddl_ast::graph_types::{GraphDirection, GraphEdgeTuple, GraphProperties};

#[derive(Debug, Clone, PartialEq)]
pub enum GraphStmt {
    // ── Graph DSL ────────────────────────────────────────────────
    GraphInsertEdge {
        collection: String,
        src: String,
        dst: String,
        label: String,
        properties: GraphProperties,
    },
    GraphDeleteEdge {
        collection: String,
        src: String,
        dst: String,
        label: String,
    },
    /// Batched edge insert: one statement carries many property-less
    /// `(src, dst, label)` triples in one round trip.
    ///
    /// The batch is deliberately property-less for now: the physical
    /// `BatchEdge` carries no property object, so per-edge `PROPERTIES`
    /// stays on the single-edge form until `BatchEdge` grows one
    ///
    GraphInsertEdges {
        collection: String,
        edges: Vec<GraphEdgeTuple>,
    },
    /// Batched edge delete, the delete-side twin of [`GraphInsertEdges`].
    GraphDeleteEdges {
        collection: String,
        edges: Vec<GraphEdgeTuple>,
    },
    GraphSetLabels {
        node_id: String,
        labels: Vec<String>,
        remove: bool,
    },
    GraphTraverse {
        /// Collection whose edges the traversal is scoped to. Required: a
        /// traversal with no collection cannot be authorized, and the CSR
        /// partition holds every collection's edges under one node space.
        collection: String,
        start: String,
        depth: usize,
        edge_label: Option<String>,
        direction: GraphDirection,
    },
    GraphNeighbors {
        /// Collection whose edges the traversal is scoped to. Required: a
        /// traversal with no collection cannot be authorized, and the CSR
        /// partition holds every collection's edges under one node space.
        collection: String,
        node: String,
        edge_label: Option<String>,
        direction: GraphDirection,
    },
    GraphPath {
        /// Collection whose edges the traversal is scoped to. Required: a
        /// traversal with no collection cannot be authorized, and the CSR
        /// partition holds every collection's edges under one node space.
        collection: String,
        src: String,
        dst: String,
        max_depth: usize,
        edge_label: Option<String>,
    },
    GraphAlgo {
        algorithm: String,
        collection: String,
        edge_label: Option<String>,
        damping: Option<f64>,
        tolerance: Option<f64>,
        resolution: Option<f64>,
        max_iterations: Option<usize>,
        sample_size: Option<usize>,
        source_node: Option<String>,
        direction: Option<String>,
        mode: Option<String>,
        /// Raw JSON object literal for Personalized PageRank seed weights,
        /// e.g. `PERSONALIZATION {"alice": 1.0, "bob": 0.5}`. Parsed into a
        /// `node_id → weight` map by the handler. `None` = standard PageRank.
        personalization: Option<String>,
    },
    /// `MATCH (x)-[:l]->(y) RETURN x, y` — body forwarded verbatim to the graph pattern compiler.
    MatchQuery { body: String },
    /// `GRAPH RAG FUSION ON <collection> QUERY ARRAY[…] [options…]`
    GraphRagFusion {
        collection: String,
        params: crate::ddl_ast::graph_parse::FusionParams,
    },
    /// `SHOW GRAPH STATS ['<collection>'] [VERBOSE] [AS OF SYSTEM TIME <ms>]`
    ///
    /// Read-only persistence-rooted stats readout. Bypasses the in-memory
    /// CSR cache to report counts derived from the durable edge store.
    /// `collection = None` means tenant-wide aggregate over all graph
    /// collections. `verbose` toggles compact (one row + JSON labels
    /// column) vs per-label (one row per (collection, label)) output.
    /// `as_of` is system-time in ms; `None` selects the live snapshot.
    ShowGraphStats {
        collection: Option<String>,
        verbose: bool,
        as_of: Option<i64>,
    },
}
