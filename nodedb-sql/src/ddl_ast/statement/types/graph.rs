// SPDX-License-Identifier: Apache-2.0

//! Graph DDL/DML statements.

use crate::ddl_ast::graph_types::{GraphDirection, GraphEdgeSpec};
use crate::temporal::TemporalScope;

#[derive(Debug, Clone, PartialEq)]
pub enum GraphStmt {
    // ── Graph DSL ────────────────────────────────────────────────
    /// `GRAPH INSERT EDGE IN <c> <edge> [<edge> ...]`: one or more edges
    /// into one collection, written as one unit.
    GraphInsertEdge {
        collection: String,
        /// Never empty.
        edges: Vec<GraphEdgeSpec>,
    },
    GraphDeleteEdge {
        collection: String,
        src: String,
        dst: String,
        label: String,
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
        /// Empty keeps every edge. Otherwise an edge with any listed label.
        edge_labels: Vec<String>,
        direction: GraphDirection,
        /// `EDGE WHERE` filters, AND-ed. Empty admits every edge.
        edge_predicate: Vec<nodedb_types::filter::MetadataFilter>,
        /// `AS OF SYSTEM TIME` / `AS OF VALID TIME` of the read.
        temporal: TemporalScope,
    },
    GraphNeighbors {
        /// Collection whose edges the traversal is scoped to. Required: a
        /// traversal with no collection cannot be authorized, and the CSR
        /// partition holds every collection's edges under one node space.
        collection: String,
        node: String,
        /// Empty keeps every edge. Otherwise an edge with any listed label.
        edge_labels: Vec<String>,
        direction: GraphDirection,
        /// `AS OF SYSTEM TIME` / `AS OF VALID TIME` of the read.
        temporal: TemporalScope,
    },
    GraphPath {
        /// Collection whose edges the traversal is scoped to. Required: a
        /// traversal with no collection cannot be authorized, and the CSR
        /// partition holds every collection's edges under one node space.
        collection: String,
        src: String,
        dst: String,
        max_depth: usize,
        /// Empty keeps every edge. Otherwise an edge with any listed label.
        edge_labels: Vec<String>,
        /// `EDGE WHERE` filters, AND-ed. Empty admits every edge.
        edge_predicate: Vec<nodedb_types::filter::MetadataFilter>,
        /// `AS OF SYSTEM TIME` / `AS OF VALID TIME` of the read.
        temporal: TemporalScope,
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
        /// `AS OF SYSTEM TIME` / `AS OF VALID TIME` of the read.
        temporal: TemporalScope,
    },
    /// `MATCH (x)-[:l]->(y) RETURN x, y`. `body` is the statement with its
    /// temporal clauses removed, for the graph pattern compiler.
    MatchQuery {
        body: String,
        /// `AS OF SYSTEM TIME` / `AS OF VALID TIME` of the match.
        temporal: TemporalScope,
    },
    /// `GRAPH RAG FUSION ON <collection> QUERY ARRAY[…] [options…]`
    GraphRagFusion {
        collection: String,
        params: crate::ddl_ast::graph_parse::FusionParams,
        /// `AS OF SYSTEM TIME` / `AS OF VALID TIME` of the read.
        temporal: TemporalScope,
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
