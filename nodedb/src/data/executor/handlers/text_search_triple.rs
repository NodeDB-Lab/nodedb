// SPDX-License-Identifier: BUSL-1.1

//! Three-source hybrid search handler: vector + BM25 text + graph BFS, fused via weighted RRF.
//!
//! Pipeline:
//! 1. Vector search from the HNSW index — top-K by distance.
//! 2. BM25 full-text search from the inverted index — top-K by score.
//! 3. Graph BFS from `graph_seed_id` up to `graph_depth` hops — scored by hop distance.
//! 4. All three ranked lists are fused via `reciprocal_rank_fusion_weighted` with
//!    per-source k-constants `(vector_k, text_k, graph_k)`. Every leg keys on
//!    [`HybridFusionKey`], so a graph node fuses with the vector and text hits
//!    for the same row through its surrogate.
//! 5. Final top-K fused results are materialised with per-source rank diagnostics.

use tracing::debug;

use nodedb_fts::FtsSearchParams;
use nodedb_fts::posting::QueryMode;
use nodedb_types::Surrogate;

use super::hybrid_key::HybridFusionKey;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::graph_expansion::{GraphExpansionParams, GraphSeeds};
use crate::data::executor::handlers::text_rows::{TextRowGate, combine_eligible};
use crate::data::executor::scan_normalize::sparse_body_to_msgpack;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::edge_store::Direction;
use crate::query::fusion::{FusedResult, RankedResult, reciprocal_rank_fusion_weighted};

/// Parameters for [`CoreLoop::execute_hybrid_search_triple`].
pub(in crate::data::executor) struct HybridSearchTripleParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    /// Vector column the vector leg searches. Empty names the
    /// collection-level index.
    pub vector_field: &'a str,
    pub query_vector: &'a [f32],
    /// Field index the text leg reads. `None` reads the whole-document index.
    pub text_field: Option<&'a str>,
    pub query_text: &'a str,
    /// Residual WHERE predicates (`Vec<ScanFilter>`), applied to every leg
    /// before fusion.
    pub filters: &'a [u8],
    pub graph_seed_id: &'a str,
    pub graph_depth: usize,
    pub graph_edge_label: Option<&'a str>,
    pub top_k: usize,
    pub ef_search: usize,
    /// Boolean combination of the text leg's query terms.
    pub mode: nodedb_types::text_search::QueryMode,
    pub fuzzy: bool,
    pub rrf_k: (f64, f64, f64),
    pub filter_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    pub rls_filters: &'a [u8],
    pub score_alias: Option<&'a str>,
}

impl CoreLoop {
    /// Execute a three-source hybrid search: vector + BM25 text + graph BFS, fused via RRF.
    ///
    /// `rrf_k` is `(vector_k, text_k, graph_k)`. Lower k → steeper rank discount → more
    /// influence from that source.
    pub(in crate::data::executor) fn execute_hybrid_search_triple(
        &self,
        task: &ExecutionTask,
        params: HybridSearchTripleParams<'_>,
    ) -> Response {
        let HybridSearchTripleParams {
            tid,
            collection,
            vector_field,
            query_vector,
            text_field,
            query_text,
            filters,
            graph_seed_id,
            graph_depth,
            graph_edge_label,
            top_k,
            ef_search,
            mode,
            fuzzy,
            rrf_k,
            filter_bitmap,
            rls_filters,
            score_alias,
        } = params;
        let tenant_id = crate::types::TenantId::new(tid);
        debug!(
            core = self.core_id,
            tid,
            %collection,
            %query_text,
            %graph_seed_id,
            graph_depth,
            top_k,
            "hybrid search triple"
        );

        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let fetch_k = top_k.saturating_mul(3).max(20);

        // The rows the residual filters and RLS admit restrict every leg
        // before fusion, so the fused top-k counts only admitted rows.
        let eligible = match TextRowGate::new(filters, rls_filters)
            .and_then(|gate| self.text_eligible_rows(task, tid, collection, &gate))
        {
            Ok(eligible) => combine_eligible(filter_bitmap, eligible),
            Err(e) => return self.response_error(task, e),
        };
        let filter_bitmap = eligible.as_ref();
        let text_index = match self.text_index(task, tid, collection, text_field) {
            Ok(index) => index,
            Err(e) => return self.response_error(task, e),
        };

        // 1. Vector search.
        let index_key = self.resolve_vector_index_key(
            task.request.database_id.as_u64(),
            tid,
            collection,
            vector_field,
        );
        let vector_collection = self.vector_collections.get(&index_key);
        let vector_results = match vector_collection {
            Some(index) => {
                let ef = if ef_search > 0 {
                    ef_search.max(fetch_k)
                } else {
                    fetch_k.saturating_mul(4).max(64)
                };
                match super::vector_search::search_vector_leg(
                    index,
                    query_vector,
                    fetch_k,
                    ef,
                    filter_bitmap,
                ) {
                    Ok(results) => results,
                    Err(code) => return self.response_error(task, code),
                }
            }
            None => Vec::new(),
        };

        // 2. BM25 text search over the field's index, restricted to the same
        //    rows, with the transaction's staged rows ranked in.
        let text_results = match self.hybrid_text_leg(
            task,
            tid,
            text_index,
            FtsSearchParams {
                query: query_text,
                top_k: fetch_k,
                fuzzy_enabled: fuzzy,
                mode: QueryMode::from(mode),
                prefilter: filter_bitmap,
            },
        ) {
            Ok(results) => results,
            Err(e) => return self.response_error(task, e),
        };

        // 3. Graph BFS from seed node.
        // The seed is named by the query itself, so it resolves to a surrogate
        // once; the walk then runs in the same identity currency as the vector
        // and text legs it will be fused with.
        let expansion = self.expand_graph(GraphExpansionParams {
            database_id: task.request.database_id.as_u64(),
            tid,
            seeds: GraphSeeds::Names(&[graph_seed_id]),
            label_filter: graph_edge_label.as_slice(),
            direction: Direction::Out,
            max_depth: graph_depth,
            max_visited: self.query_tuning.bfs_memory_budget_bytes
                / self.query_tuning.bfs_bytes_per_node,
            collection,
        });
        // 4. Build ranked lists.

        // Inside a transaction, read-your-own-writes: the vector and text legs
        // must also observe this transaction's staged document writes, folded
        // in via the shared overlay splice (reusing the single-source
        // vector/FTS overlay merges). The graph leg's RYOW is a separate
        // concern and is not folded in here. Outside a transaction the
        // committed-only construction below runs unchanged.
        let (vector_ranked, text_ranked): super::hybrid_overlay::HybridRankedLegs =
            if let Some(txn_id) = task.request.txn_id {
                match self.hybrid_ranked_with_overlay(
                    super::hybrid_overlay::HybridOverlayParams {
                        txn_id,
                        database_id: task.request.database_id,
                        tid: tenant_id,
                        collection,
                        vector_field,
                        query_vector,
                        fetch_k,
                        filter_bitmap,
                    },
                    &vector_results,
                    vector_collection,
                    &text_results,
                ) {
                    Ok(ranked) => ranked,
                    Err(e) => return self.response_error(task, e),
                }
            } else {
                let vector_ranked: Vec<RankedResult<HybridFusionKey>> = vector_results
                    .iter()
                    .enumerate()
                    .map(|(rank, r)| RankedResult {
                        document_id: super::vector_search::vector_leg_key(vector_collection, r.id),
                        rank,
                        score: r.distance,
                        source: "vector",
                    })
                    .collect();

                let text_ranked: Vec<RankedResult<HybridFusionKey>> = text_results
                    .iter()
                    .enumerate()
                    .map(|(rank, r)| RankedResult {
                        document_id: HybridFusionKey::for_surrogate(r.doc_id),
                        rank,
                        score: r.score,
                        source: "text",
                    })
                    .collect();
                (vector_ranked, text_ranked)
            };

        // The graph walk reaches rows regardless of the filters, so its leg
        // keeps only the admitted ones. A node with no row is not admitted.
        let mut graph_ranked = graph_reached_to_ranked_keys(&expansion.reached);
        if let Some(admitted) = filter_bitmap {
            graph_ranked.retain(|r| {
                r.document_id
                    .storage_key()
                    .is_some_and(|key| admitted.contains(key.surrogate()))
            });
            for (rank, r) in graph_ranked.iter_mut().enumerate() {
                r.rank = rank;
            }
        }

        let (k_vector, k_text, k_graph) = rrf_k;
        let fused = reciprocal_rank_fusion_weighted(
            &[vector_ranked, text_ranked, graph_ranked],
            &[k_vector, k_text, k_graph],
            top_k,
        );

        // 5. Materialise results with per-engine rank diagnostics (reusing HybridSearchHit).
        //
        // The RLS predicate runs against the NORMALIZED msgpack image, never
        // the stored bytes: a strict Binary Tuple is not a msgpack map at all
        // and a vector-primary sidecar is a TAGGED one, so a predicate pushed
        // at the stored bytes finds no field it recognizes and the row is
        // dropped on a format mismatch rather than on policy. The encoding is
        // resolved from the collection's registered kind — a tagged map and a
        // plain document map share the same map header, so the bytes cannot
        // answer it.
        let body_format = self.sparse_body_format(task.request.database_id, tenant_id, collection);
        // The fused key is rendered once per row here, at the response
        // envelope; it is the only place the key becomes text.
        let rendered: Vec<(String, &FusedResult<HybridFusionKey>)> = fused
            .iter()
            .filter(|f| {
                if rls_filters.is_empty() {
                    return true;
                }
                // A headless hit has no stored row to check the policy against,
                // so it is treated the same as a row the lookup cannot find.
                let Some(key) = f.document_id.storage_key() else {
                    return false;
                };
                match self.text_base_body(task.request.database_id.as_u64(), tid, collection, &key)
                {
                    Ok(Some(bytes)) => {
                        let normalized =
                            sparse_body_to_msgpack(&bytes, body_format.as_format_ref());
                        super::rls_eval::rls_check_msgpack_bytes(rls_filters, &normalized)
                    }
                    _ => false,
                }
            })
            .map(|f| (f.document_id.to_string(), f))
            .collect();
        let results: Vec<_> = rendered
            .iter()
            .map(|(doc_id, f)| {
                let vector_rank = vector_results.iter().position(|r| {
                    super::vector_search::vector_leg_key(vector_collection, r.id) == f.document_id
                });
                let text_rank = text_results
                    .iter()
                    .position(|r| HybridFusionKey::for_surrogate(r.doc_id) == f.document_id);

                super::super::response_codec::HybridSearchHit {
                    doc_id,
                    score_field: score_alias.unwrap_or("rrf_score"),
                    rrf_score: f.rrf_score,
                    vector_rank,
                    text_rank,
                }
            })
            .collect();

        if let Some(ref m) = self.metrics {
            m.record_fts_search(0);
        }
        match super::super::response_codec::encode(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

/// Rank the surrogate-bound nodes an expansion reached by hop distance, keyed
/// on [`HybridFusionKey`] so they fuse with the vector and text legs.
///
/// Ties on hop distance break on the surrogate, so the rank order is
/// deterministic. Nodes without a surrogate never enter the list: they have
/// no cross-engine identity to fuse on.
fn graph_reached_to_ranked_keys(
    reached: &[(Surrogate, usize)],
) -> Vec<RankedResult<HybridFusionKey>> {
    let mut sorted: Vec<(Surrogate, usize)> = reached.to_vec();
    sorted.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    sorted
        .into_iter()
        .enumerate()
        .map(|(rank, (surrogate, hop_dist))| RankedResult {
            document_id: HybridFusionKey::for_surrogate(surrogate),
            rank,
            score: hop_dist as f32,
            source: "graph",
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_leg_ranks_by_hop_then_surrogate() {
        let reached = [
            (Surrogate::new(9), 2),
            (Surrogate::new(4), 1),
            (Surrogate::new(2), 1),
        ];
        let ranked = graph_reached_to_ranked_keys(&reached);
        let keys: Vec<HybridFusionKey> = ranked.iter().map(|r| r.document_id).collect();
        assert_eq!(
            keys,
            vec![
                HybridFusionKey::for_surrogate(Surrogate::new(2)),
                HybridFusionKey::for_surrogate(Surrogate::new(4)),
                HybridFusionKey::for_surrogate(Surrogate::new(9)),
            ]
        );
        assert_eq!(ranked[0].rank, 0);
        assert_eq!(ranked[2].score, 2.0);
    }
}
