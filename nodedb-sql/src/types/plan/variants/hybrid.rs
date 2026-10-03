// SPDX-License-Identifier: Apache-2.0

//! Hybrid search plan payloads: vector + text, and vector + text + graph.

use nodedb_types::text_search::QueryMode;

use crate::types::filter::Filter;
use crate::types::query::Projection;

/// Payload of [`SqlPlan::HybridSearch`](crate::types::SqlPlan::HybridSearch).
#[derive(Debug, Clone)]
pub struct HybridSearchPlan {
    pub collection: String,
    /// Vector column of the `vector_distance(column, q)` leg.
    pub vector_field: String,
    pub query_vector: Vec<f32>,
    /// Column of the `bm25_score(column, q)` leg. `None` for `*`: the
    /// whole-document index.
    pub text_field: Option<String>,
    pub query_text: String,
    /// Residual WHERE predicates. They restrict both legs before fusion.
    pub filters: Vec<Filter>,
    pub top_k: usize,
    pub ef_search: usize,
    pub vector_weight: f32,
    /// `mode => 'and' | 'or'` of the `bm25_score` leg.
    pub mode: QueryMode,
    /// `fuzzy => true | false` of the `bm25_score` leg.
    pub fuzzy: bool,
    /// SELECT-list alias the response should use for the RRF score
    /// column. `None` means the executor falls back to the fixed
    /// internal field name `rrf_score`. Set by the planner from the
    /// SELECT projection's `AS <alias>` for the `rrf_score(...)` call.
    pub score_alias: Option<String>,
    /// Resolved SELECT target list, for output-schema derivation.
    pub projection: Vec<Projection>,
}

/// Payload of [`SqlPlan::HybridSearchTriple`](crate::types::SqlPlan::HybridSearchTriple).
#[derive(Debug, Clone)]
pub struct HybridSearchTriplePlan {
    pub collection: String,
    /// Vector column of the `vector_distance(column, q)` leg.
    pub vector_field: String,
    pub query_vector: Vec<f32>,
    /// Column of the `bm25_score(column, q)` leg. `None` for `*`: the
    /// whole-document index.
    pub text_field: Option<String>,
    pub query_text: String,
    /// Residual WHERE predicates. They restrict the vector and text legs
    /// before fusion.
    pub filters: Vec<Filter>,
    /// Node id used as the BFS seed for the graph leg.
    pub graph_seed_id: String,
    /// Maximum BFS depth from the seed node.
    pub graph_depth: usize,
    /// Edge label filter for graph BFS. `None` = all edges.
    pub graph_edge_label: Option<String>,
    pub top_k: usize,
    pub ef_search: usize,
    /// `mode => 'and' | 'or'` of the `bm25_score` leg.
    pub mode: QueryMode,
    /// `fuzzy => true | false` of the `bm25_score` leg.
    pub fuzzy: bool,
    /// Per-source RRF k constants: (vector_k, text_k, graph_k).
    pub rrf_k: (f64, f64, f64),
    /// SELECT-list alias for the fused RRF score column.
    pub score_alias: Option<String>,
    /// Resolved SELECT target list, for output-schema derivation.
    pub projection: Vec<Projection>,
}
