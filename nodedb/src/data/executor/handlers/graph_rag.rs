// SPDX-License-Identifier: BUSL-1.1

//! GraphRAG fusion handler: vector search, graph expansion, RRF scoring.
//!
//! Pipeline:
//! 1. Vector engine returns top-K semantically similar rows.
//! 2. Their surrogates seed graph expansion directly — the two engines meet on
//!    global identity, so nothing is translated between the hops.
//! 3. Graph-expanded result set is scored by hop distance.
//! 4. RRF fuses vector_score and graph_score into unified ranking.
//! 5. Final top-N results are materialized.
//!
//! Expansion is bounded by a per-query memory budget derived from the node
//! count. If the budget is exceeded, expansion stops early and results are
//! marked as truncated.

use std::collections::HashMap;

use nodedb_types::Surrogate;
use nodedb_vector::SearchResult;
use tracing::{debug, warn};

use super::graph_expansion::{GraphExpansionParams, GraphSeeds};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec::{
    GraphRagMetadata, GraphRagResponse, GraphRagResult, encode,
};
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::edge_store::Direction;
use crate::query::fusion::{FusedResult, RankedResult, reciprocal_rank_fusion_weighted};

/// Result of a successful vector search.
///
/// - `Vec<SearchResult>` is the raw HNSW output, for reporting candidate counts.
/// - `HashMap` maps each hit's *reporting key* to `(rank, distance)`. That key
///   is what RRF fuses on and what the response returns, so it has to be a name.
/// - `Vec<Surrogate>` is the same hits in the identity currency, ready to seed
///   graph expansion with no translation.
type VectorNodeScores = (
    Vec<SearchResult>,
    HashMap<String, (usize, f32)>,
    Vec<Surrogate>,
);

/// Parameters for `build_rag_response`.
pub(in crate::data::executor) struct RagResponseParams<'a> {
    pub fused: &'a [FusedResult],
    pub vector_scores: &'a HashMap<String, (usize, f32)>,
    pub hop_distances: &'a HashMap<String, usize>,
    pub vector_candidate_count: usize,
    pub graph_expanded_count: usize,
    pub bfs_truncated: bool,
    /// Expanded nodes with no surrogate — see
    /// [`GraphRagMetadata::graph_unaddressable`].
    pub graph_unaddressable: usize,
    pub op_name: &'a str,
}

/// Bundled arguments for [`CoreLoop::execute_graph_rag_fusion`].
pub(in crate::data::executor) struct GraphRagFusionParams<'a> {
    pub tenant_id: u64,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub vector_top_k: usize,
    pub edge_label: &'a Option<String>,
    pub direction: Direction,
    pub expansion_depth: usize,
    pub final_top_k: usize,
    pub rrf_k: (f64, f64),
    pub vector_field: &'a str,
    pub max_visited: usize,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_rag_fusion(
        &self,
        task: &ExecutionTask,
        params: GraphRagFusionParams<'_>,
    ) -> Response {
        let GraphRagFusionParams {
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
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            vector_top_k,
            expansion_depth,
            final_top_k,
            "graph rag fusion"
        );

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

        let (vector_k, graph_k) = rrf_k;

        let vector_list: Vec<RankedResult> = vector_scores
            .iter()
            .map(|(node_id, (rank, dist))| RankedResult {
                document_id: node_id.clone(),
                rank: *rank,
                score: *dist,
                source: "vector",
            })
            .collect();

        let graph_list = graph_nodes_to_ranked_results(&expanded_nodes, &hop_distances);

        let fused = reciprocal_rank_fusion_weighted(
            &[vector_list, graph_list],
            &[vector_k, graph_k],
            final_top_k,
        );

        self.build_rag_response(
            task,
            RagResponseParams {
                fused: &fused,
                vector_scores: &vector_scores,
                hop_distances: &hop_distances,
                vector_candidate_count: vector_results.len(),
                graph_expanded_count: expanded_nodes.len(),
                bfs_truncated,
                graph_unaddressable: unaddressable,
                op_name: "graph rag fusion",
            },
        )
    }

    /// Look up the HNSW index for `collection` and run the search.
    ///
    /// Returns the raw hits, their reporting keys, and their surrogates — see
    /// [`VectorNodeScores`]. `Err(response)` when the index is missing or the
    /// search returned no candidates; the caller forwards it directly.
    pub(in crate::data::executor) fn vector_search_to_node_scores(
        &self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        query_vector: &[f32],
        vector_top_k: usize,
        vector_field: &str,
    ) -> Result<VectorNodeScores, Response> {
        let database_id = task.request.database_id.as_u64();
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, collection, vector_field);
        let Some(index) = self.vector_collections.get(&index_key) else {
            return Err(self.response_error(task, ErrorCode::NotFound));
        };
        if index.is_empty() {
            return Err(self.response_with_payload(task, b"[]".to_vec()));
        }

        let ef = vector_top_k.saturating_mul(4).max(64);
        let vector_results = index.search(query_vector, vector_top_k, ef);

        if vector_results.is_empty() {
            return Err(self.response_with_payload(task, b"[]".to_vec()));
        }

        // Each hit is carried in two forms. The surrogate is the identity the
        // graph leg seeds from directly — no name is minted to seed a walk that
        // would only hash it straight back to the same node.
        //
        // The reporting key is resolved once per hit: the graph node name when
        // the surrogate is bound to one, otherwise the surrogate's
        // client-visible decimal identity. Falling back to the identity rather
        // than to an index-local sentinel is what lets a hit fuse with the
        // *text* leg, which keys on exactly that; the old `__local_{hnsw_id}`
        // sentinel could match nothing and leaked an internal index id into
        // the response's `node_id`.
        let csr = self.csr_partition(database_id, tenant_id);
        let mut vector_scores: HashMap<String, (usize, f32)> = HashMap::new();
        let mut seeds: Vec<Surrogate> = Vec::with_capacity(vector_results.len());
        for (rank, result) in vector_results.iter().enumerate() {
            let surrogate = index.get_surrogate(result.id);
            if let Some(s) = surrogate {
                seeds.push(s);
            }
            let key = match surrogate {
                Some(s) => csr
                    .and_then(|c| c.node_id_for_surrogate(s))
                    .map(str::to_string)
                    .unwrap_or_else(|| {
                        crate::engine::document::store::RowIdentity::for_surrogate(s)
                            .as_str()
                            .to_string()
                    }),
                // No surrogate at all: the vector entry predates surrogate
                // plumbing, so it has no cross-engine identity. It still ranks
                // in the vector leg under a key that deliberately matches
                // nothing else.
                None => format!("__unbound_{}", result.id),
            };
            vector_scores.insert(key, (rank, result.distance));
        }

        Ok((vector_results, vector_scores, seeds))
    }

    /// Encode a `GraphRagResponse` from RRF-fused results.
    ///
    /// Shared by both 2-source (`execute_graph_rag_fusion`) and 3-source
    /// (`execute_graph_rag_fusion_triple`) fusion pipelines.
    pub(in crate::data::executor) fn build_rag_response(
        &self,
        task: &ExecutionTask,
        p: RagResponseParams<'_>,
    ) -> Response {
        let results: Vec<GraphRagResult> = p
            .fused
            .iter()
            .map(|f| {
                let (vector_rank, vector_distance) = p
                    .vector_scores
                    .get(f.document_id.as_str())
                    .map(|(rank, dist)| (Some(*rank), Some(*dist)))
                    .unwrap_or((None, None));
                let hop_distance = p.hop_distances.get(f.document_id.as_str()).copied();
                GraphRagResult {
                    node_id: f.document_id.clone(),
                    rrf_score: f.rrf_score,
                    vector_rank,
                    vector_distance,
                    hop_distance,
                }
            })
            .collect();

        let response_body = GraphRagResponse {
            results,
            metadata: GraphRagMetadata {
                vector_candidates: p.vector_candidate_count,
                graph_expanded: p.graph_expanded_count,
                truncated: p.bfs_truncated,
                graph_unaddressable: p.graph_unaddressable,
                watermark_lsn: self.watermark.as_u64(),
            },
        };

        match encode(&response_body) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "{} serialization failed", p.op_name);
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }
}

/// Sort expanded graph nodes by hop distance and convert to `RankedResult` list.
///
/// Used by 2-source GraphRAG, 3-source GraphRAG triple, and 3-source hybrid
/// text search to avoid duplicating the sort-and-rank pattern.
pub(super) fn graph_nodes_to_ranked_results(
    expanded_nodes: &[String],
    hop_distances: &HashMap<String, usize>,
) -> Vec<RankedResult> {
    let mut sorted: Vec<(&str, usize)> = expanded_nodes
        .iter()
        .map(|node| {
            let dist = hop_distances
                .get(node.as_str())
                .copied()
                .unwrap_or(usize::MAX);
            (node.as_str(), dist)
        })
        .collect();
    sorted.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(b.0)));

    sorted
        .into_iter()
        .enumerate()
        .map(|(rank, (node_id, hop_dist))| RankedResult {
            document_id: node_id.to_string(),
            rank,
            score: hop_dist as f32,
            source: "graph",
        })
        .collect()
}
