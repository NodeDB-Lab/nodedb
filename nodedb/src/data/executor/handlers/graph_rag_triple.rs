// SPDX-License-Identifier: BUSL-1.1

//! Three-source GraphRAG fusion: vector search + BM25 text + graph expansion, fused via RRF.
//!
//! Pipeline:
//! 1. Vector search returns top-K semantically similar nodes.
//! 2. BM25 text search returns top-K text-relevant documents.
//! 3. Result vector node IDs feed into graph BFS as start nodes.
//! 4. All three ranked lists are fused by `reciprocal_rank_fusion_weighted` with
//!    per-source k-constants `(vector_k, text_k, graph_k)`.
//! 5. Final top-N results are materialised.

use nodedb_fts::FtsSearchParams;
use nodedb_fts::posting::QueryMode;
use nodedb_types::RowIdentity;
use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::graph_expansion::{GraphExpansionParams, GraphSeeds};
use crate::data::executor::handlers::graph_rag::{
    RagResponseParams, graph_nodes_to_ranked_results,
};
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::edge_store::Direction;
use crate::query::fusion::{RankedResult, reciprocal_rank_fusion_weighted};
use crate::types::TenantId;

/// Bundled arguments for [`CoreLoop::execute_graph_rag_fusion_triple`].
pub(in crate::data::executor) struct GraphRagFusionTripleParams<'a> {
    pub tenant_id: u64,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub vector_top_k: usize,
    pub edge_label: &'a Option<String>,
    pub direction: Direction,
    pub expansion_depth: usize,
    pub final_top_k: usize,
    pub rrf_k: (f64, f64, f64),
    pub vector_field: &'a str,
    pub max_visited: usize,
    pub bm25_query: &'a str,
    pub bm25_field: &'a str,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_rag_fusion_triple(
        &self,
        task: &ExecutionTask,
        params: GraphRagFusionTripleParams<'_>,
    ) -> Response {
        let GraphRagFusionTripleParams {
            tenant_id,
            collection,
            query_vector,
            vector_top_k,
            edge_label,
            direction,
            expansion_depth,
            final_top_k,
            rrf_k,
            vector_field,
            max_visited,
            bm25_query,
            bm25_field: _bm25_field,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            vector_top_k,
            expansion_depth,
            final_top_k,
            %bm25_query,
            "graph rag fusion triple"
        );

        let tid_typed = TenantId::new(tenant_id);

        let (vector_results, vector_scores, seeds) = match self.vector_search_to_node_scores(
            task,
            tenant_id,
            collection,
            query_vector,
            vector_top_k,
            vector_field,
        ) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        // BM25 text search.
        let fetch_k = final_top_k.saturating_mul(3).max(20);
        let text_results = self
            .inverted
            .search(
                task.request.database_id.as_u64(),
                tid_typed,
                collection,
                FtsSearchParams {
                    query: bm25_query,
                    top_k: fetch_k,
                    fuzzy_enabled: true,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap_or_default();

        // Graph expansion seeded directly by the vector hits' surrogates.
        let expansion = self.expand_graph(GraphExpansionParams {
            database_id: task.request.database_id.as_u64(),
            tid: tenant_id,
            seeds: GraphSeeds::Surrogates(&seeds),
            label_filter: edge_label.as_deref(),
            direction,
            max_depth: expansion_depth,
            max_visited,
            collection,
        });
        let (expanded_nodes, hop_distances, bfs_truncated, unaddressable) = (
            expansion.names,
            expansion.distances,
            expansion.truncated,
            expansion.unaddressable,
        );

        let (vector_k, text_k, graph_k) = rrf_k;

        let vector_list: Vec<RankedResult<RowIdentity>> = vector_scores
            .iter()
            .map(|(node_id, (rank, dist))| RankedResult {
                document_id: node_id.clone(),
                rank: *rank,
                score: *dist,
                source: "vector",
            })
            .collect();

        let text_list: Vec<RankedResult<RowIdentity>> = text_results
            .iter()
            .enumerate()
            .map(|(rank, r)| RankedResult {
                document_id: RowIdentity::for_surrogate(r.doc_id),
                rank,
                score: r.score,
                source: "text",
            })
            .collect();

        let graph_expanded_count = expanded_nodes.len();
        let graph_list = graph_nodes_to_ranked_results(expanded_nodes, &hop_distances);

        let fused = reciprocal_rank_fusion_weighted(
            &[vector_list, text_list, graph_list],
            &[vector_k, text_k, graph_k],
            final_top_k,
        );

        self.build_rag_response(
            task,
            RagResponseParams {
                fused: &fused,
                vector_scores: &vector_scores,
                hop_distances: &hop_distances,
                vector_candidate_count: vector_results.len(),
                graph_expanded_count,
                bfs_truncated,
                graph_unaddressable: unaddressable,
                op_name: "graph rag fusion triple",
            },
        )
    }
}
