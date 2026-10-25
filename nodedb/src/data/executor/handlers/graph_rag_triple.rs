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
use nodedb_types::Surrogate;
use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::graph_expansion::{GraphExpansionParams, GraphSeeds};
use crate::data::executor::handlers::graph_rag::{
    RagResponseParams, VectorLeg, graph_nodes_to_ranked_results, vector_ranked_list,
};
use crate::data::executor::handlers::rag_key::RagKey;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::edge_store::Direction;
use crate::query::fusion::{RankedResult, reciprocal_rank_fusion_weighted};
use crate::types::TenantId;
use nodedb_physical::physical_plan::GraphReadScope;

/// The BM25 leg of a three-source fusion: the query and the field whose
/// index it searches.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct Bm25Leg<'a> {
    pub query: &'a str,
    pub field: &'a str,
}

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
    /// The bitemporal cut the graph expansion walks.
    pub scope: &'a GraphReadScope,
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
            bm25_field,
            scope,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            vector_top_k,
            expansion_depth,
            final_top_k,
            %bm25_query,
            %bm25_field,
            "graph rag fusion triple"
        );

        let tid_typed = TenantId::new(tenant_id);

        let (vector_results, vector_scores, seeds) = match self.vector_search_to_node_scores(
            task,
            VectorLeg {
                tenant_id,
                collection,
                query_vector,
                vector_top_k,
                vector_field,
                scope,
            },
        ) {
            Ok(r) => r,
            Err(resp) => return resp,
        };

        let bm25 = Bm25Leg {
            query: bm25_query,
            field: bm25_field,
        };
        let text_results = match self.bm25_leg_hits(task, tid_typed, collection, bm25, final_top_k)
        {
            Ok(results) => results,
            Err(resp) => return resp,
        };

        // Graph expansion seeded directly by the vector hits' surrogates.
        let expansion = match self.expand_graph(GraphExpansionParams {
            database_id: task.request.database_id.as_u64(),
            tid: tenant_id,
            seeds: GraphSeeds::Surrogates(&seeds),
            label_filter: edge_label.as_deref().as_slice(),
            direction,
            max_depth: expansion_depth,
            max_visited,
            collection,
            scope,
        }) {
            Ok(expansion) => expansion,
            Err(e) => return self.response_error(task, e),
        };
        let (expanded_nodes, hop_distances, bfs_truncated, unaddressable) = (
            expansion.names,
            expansion.distances,
            expansion.truncated,
            expansion.unaddressable,
        );

        let (vector_k, text_k, graph_k) = rrf_k;

        let vector_list = vector_ranked_list(&vector_scores);
        let csr = match self.csr_view(
            task.request.database_id.as_u64(),
            tenant_id,
            Some(collection),
            scope,
        ) {
            Ok(csr) => csr,
            Err(e) => return self.response_error(task, e),
        };
        let text_list = text_ranked_list(&text_results, |s| {
            csr.and_then(|c| c.node_id_for_surrogate(s))
        });

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

    /// Run the BM25 leg of a three-source fusion: `(surrogate, score)` per
    /// hit, in rank order. The leg searches the index of `bm25.field`. It
    /// fetches three times `final_top_k`, at least 20, so fusion has
    /// candidates past the final cut. A collection with no text at all gives
    /// no hits. A field that holds no text while other text exists is a
    /// `TextColumn` error.
    pub(in crate::data::executor) fn bm25_leg_hits(
        &self,
        task: &ExecutionTask,
        tenant_id: TenantId,
        collection: &str,
        bm25: Bm25Leg<'_>,
        final_top_k: usize,
    ) -> Result<Vec<(Surrogate, f32)>, Response> {
        let index = match self.text_index(task, tenant_id.as_u64(), collection, Some(bm25.field)) {
            Ok(Some(index)) => index,
            Ok(None) => return Ok(Vec::new()),
            Err(e) => return Err(self.response_error(task, e)),
        };
        let fetch_k = final_top_k.saturating_mul(3).max(20);
        match self.inverted.search(
            task.request.database_id.as_u64(),
            tenant_id,
            index,
            FtsSearchParams {
                query: bm25.query,
                top_k: fetch_k,
                fuzzy_enabled: true,
                mode: QueryMode::And,
                prefilter: None,
            },
        ) {
            Ok(results) => Ok(results.iter().map(|r| (r.doc_id, r.score)).collect()),
            Err(e) => Err(self.response_error(task, e)),
        }
    }
}

/// The BM25 leg as a ranked list. A text hit keys as a vector hit does: the
/// graph node `name_of` names for its surrogate, else the surrogate.
pub(crate) fn text_ranked_list<'n>(
    hits: &[(Surrogate, f32)],
    name_of: impl Fn(Surrogate) -> Option<&'n str>,
) -> Vec<RankedResult<RagKey>> {
    hits.iter()
        .enumerate()
        .map(|(rank, (doc_id, score))| RankedResult {
            document_id: RagKey::of_hit(name_of(*doc_id), *doc_id),
            rank,
            score: *score,
            source: "text",
        })
        .collect()
}
