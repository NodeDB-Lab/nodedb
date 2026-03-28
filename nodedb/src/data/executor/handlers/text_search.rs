//! Text search and hybrid search handlers for the Data Plane CoreLoop.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Default hybrid search weight: 0.5 = equal vector + text.
const DEFAULT_VECTOR_WEIGHT: f32 = 0.5;

impl CoreLoop {
    /// Execute a full-text search using BM25 + optional fuzzy matching.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_text_search(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy: bool,
        rls_filters: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, %query, top_k, fuzzy, "text search");

        // Fetch extra candidates when RLS is active.
        let fetch_k = if rls_filters.is_empty() {
            top_k
        } else {
            top_k.saturating_mul(2).max(20)
        };

        match self.inverted.search(collection, query, fetch_k, fuzzy) {
            Ok(results) => {
                // RLS post-score filtering: look up each candidate's document.
                let hits: Vec<_> = results
                    .iter()
                    .filter(|r| {
                        if rls_filters.is_empty() {
                            return true;
                        }
                        match self.sparse.get(tid, collection, &r.doc_id) {
                            Ok(Some(bytes)) => {
                                super::rls_eval::rls_check_msgpack_bytes(rls_filters, &bytes)
                            }
                            _ => false,
                        }
                    })
                    .take(top_k)
                    .map(|r| super::super::response_codec::TextSearchHit {
                        doc_id: &r.doc_id,
                        score: r.score,
                        fuzzy: r.fuzzy,
                    })
                    .collect();
                match super::super::response_codec::encode(&hits) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Execute a hybrid search: vector + text, fused via weighted RRF.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_hybrid_search(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        query_vector: &[f32],
        query_text: &str,
        top_k: usize,
        ef_search: usize,
        fuzzy: bool,
        vector_weight: f32,
        filter_bitmap: Option<&std::sync::Arc<[u8]>>,
        rls_filters: &[u8],
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %query_text,
            top_k,
            vector_weight,
            "hybrid search"
        );

        let weight = if vector_weight <= 0.0 || vector_weight >= 1.0 {
            DEFAULT_VECTOR_WEIGHT
        } else {
            vector_weight
        };
        let text_weight = 1.0 - weight;

        // Fetch more candidates than top_k from each engine so RRF has
        // enough material to fuse. 3x is a good balance.
        let fetch_k = top_k.saturating_mul(3).max(20);

        // 1. Vector search.
        let index_key = CoreLoop::vector_index_key(tid, collection, "");
        let vector_results = if let Some(index) = self.vector_collections.get(&index_key) {
            if index.is_empty() {
                Vec::new()
            } else {
                let ef = if ef_search > 0 {
                    ef_search.max(fetch_k)
                } else {
                    fetch_k.saturating_mul(4).max(64)
                };
                match filter_bitmap {
                    Some(bm) => index.search_with_bitmap_bytes(query_vector, fetch_k, ef, bm),
                    None => index.search(query_vector, fetch_k, ef),
                }
            }
        } else {
            Vec::new()
        };

        // 2. Text search.
        let text_results = self
            .inverted
            .search(collection, query_text, fetch_k, fuzzy)
            .unwrap_or_default();

        // 3. Build ranked lists for weighted RRF.
        // Higher weight → lower k → steeper rank discount → more influence.
        use crate::query::fusion::{RankedResult, reciprocal_rank_fusion_weighted};

        let base_k = 60.0_f64;
        let k_vector = if weight > 0.01 {
            base_k / weight as f64
        } else {
            base_k * 100.0
        };
        let k_text = if text_weight > 0.01 {
            base_k / text_weight as f64
        } else {
            base_k * 100.0
        };

        let vector_ranked: Vec<RankedResult> = vector_results
            .iter()
            .enumerate()
            .map(|(rank, r)| RankedResult {
                document_id: r.id.to_string(),
                rank,
                score: r.distance,
                source: "vector",
            })
            .collect();

        let text_ranked: Vec<RankedResult> = text_results
            .iter()
            .enumerate()
            .map(|(rank, r)| RankedResult {
                document_id: r.doc_id.clone(),
                rank,
                score: r.score,
                source: "text",
            })
            .collect();

        let fused = reciprocal_rank_fusion_weighted(
            &[vector_ranked, text_ranked],
            &[k_vector, k_text],
            top_k,
        );

        // Build response with per-engine rank diagnostics.
        // RLS post-fusion: filter fused results by looking up each document.
        let results: Vec<_> = fused
            .iter()
            .filter(|f| {
                if rls_filters.is_empty() {
                    return true;
                }
                match self.sparse.get(tid, collection, &f.document_id) {
                    Ok(Some(bytes)) => {
                        super::rls_eval::rls_check_msgpack_bytes(rls_filters, &bytes)
                    }
                    _ => false,
                }
            })
            .map(|f| {
                let vector_rank = vector_results
                    .iter()
                    .position(|r| r.id.to_string() == f.document_id);
                let text_rank = text_results.iter().position(|r| r.doc_id == f.document_id);

                super::super::response_codec::HybridSearchHit {
                    doc_id: &f.document_id,
                    rrf_score: f.rrf_score,
                    vector_rank,
                    text_rank,
                }
            })
            .collect();

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
