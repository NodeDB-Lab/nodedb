// SPDX-License-Identifier: BUSL-1.1

//! Hybrid (vector + text) search handler for the Data Plane CoreLoop.

use tracing::debug;

use nodedb_fts::FtsSearchParams;
use nodedb_fts::posting::QueryMode;

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scan_normalize::sparse_body_to_msgpack;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// Default hybrid search weight: 0.5 = equal vector + text.
const DEFAULT_VECTOR_WEIGHT: f32 = 0.5;

/// Parameters for [`CoreLoop::execute_hybrid_search`].
pub(in crate::data::executor) struct HybridSearchParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub query_text: &'a str,
    pub top_k: usize,
    pub ef_search: usize,
    pub fuzzy: bool,
    pub vector_weight: f32,
    pub filter_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    pub rls_filters: &'a [u8],
    pub score_alias: Option<&'a str>,
}

impl CoreLoop {
    /// Execute a hybrid search: vector + text, fused via weighted RRF.
    ///
    /// `score_alias` overrides the response field name for the RRF score
    /// column. When `None` the executor uses the fixed default `rrf_score`.
    pub(in crate::data::executor) fn execute_hybrid_search(
        &self,
        task: &ExecutionTask,
        params: HybridSearchParams<'_>,
    ) -> Response {
        let HybridSearchParams {
            tid,
            collection,
            query_vector,
            query_text,
            top_k,
            ef_search,
            fuzzy,
            vector_weight,
            filter_bitmap,
            rls_filters,
            score_alias,
        } = params;
        let tenant_id = TenantId::new(tid);
        debug!(
            core = self.core_id,
            tid,
            %collection,
            %query_text,
            top_k,
            vector_weight,
            "hybrid search"
        );

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

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
        let index_key =
            CoreLoop::vector_index_key(task.request.database_id.as_u64(), tid, collection, "");
        let vector_collection = self.vector_collections.get(&index_key);
        let vector_results = if let Some(index) = vector_collection {
            if index.is_empty() {
                Vec::new()
            } else {
                let ef = if ef_search > 0 {
                    ef_search.max(fetch_k)
                } else {
                    fetch_k.saturating_mul(4).max(64)
                };
                match filter_bitmap {
                    Some(surrogate_bm) => {
                        let mut buf = Vec::with_capacity(surrogate_bm.0.serialized_size());
                        if surrogate_bm.0.serialize_into(&mut buf).is_ok() {
                            index.search_with_bitmap_bytes(query_vector, fetch_k, ef, &buf)
                        } else {
                            index.search(query_vector, fetch_k, ef)
                        }
                    }
                    None => index.search(query_vector, fetch_k, ef),
                }
            }
        } else {
            Vec::new()
        };

        // 2. Text search (no surrogate prefilter for the text leg of hybrid search).
        let text_results = self
            .inverted
            .search(
                task.request.database_id.as_u64(),
                tenant_id,
                collection,
                FtsSearchParams {
                    query: query_text,
                    top_k: fetch_k,
                    fuzzy_enabled: fuzzy,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
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

        // Translate vector local-hnsw IDs to surrogate-hex doc_ids so the
        // vector and text legs share the same RRF key space. Headless rows
        // (no surrogate binding) fall back to a non-fusable sentinel —
        // they cannot match any FTS doc_id, which is the correct behavior.
        //
        // Inside a transaction, read-your-own-writes: the vector and text legs
        // must also observe this transaction's staged document writes, folded
        // in via the shared overlay splice (which reuses the single-source
        // vector/FTS overlay merges). Outside a transaction the committed-only
        // construction below runs unchanged.
        let (vector_ranked, text_ranked): (Vec<RankedResult>, Vec<RankedResult>) =
            if let Some(txn_id) = task.request.txn_id {
                match self.hybrid_ranked_with_overlay(
                    super::hybrid_overlay::HybridOverlayParams {
                        txn_id,
                        database_id: task.request.database_id,
                        tid: tenant_id,
                        collection,
                        query_vector,
                        query_text,
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
                let vector_ranked: Vec<RankedResult> = vector_results
                    .iter()
                    .enumerate()
                    .map(|(rank, r)| RankedResult {
                        document_id: super::vector_search::vector_leg_doc_id(
                            vector_collection,
                            r.id,
                        ),
                        rank,
                        score: r.distance,
                        source: "vector",
                    })
                    .collect();

                let text_ranked: Vec<RankedResult> = text_results
                    .iter()
                    .enumerate()
                    .map(|(rank, r)| RankedResult {
                        document_id: crate::engine::document::store::surrogate_to_doc_id(r.doc_id),
                        rank,
                        score: r.score,
                        source: "text",
                    })
                    .collect();
                (vector_ranked, text_ranked)
            };

        let fused = reciprocal_rank_fusion_weighted(
            &[vector_ranked, text_ranked],
            &[k_vector, k_text],
            top_k,
        );

        // Build response with per-engine rank diagnostics.
        // RLS post-fusion: filter fused results by looking up each document.
        //
        // The predicate runs against the NORMALIZED msgpack image, never the
        // stored bytes: a strict Binary Tuple is not a msgpack map at all and a
        // vector-primary sidecar is a TAGGED one, so a predicate pushed at the
        // stored bytes finds no field it recognizes and the row is dropped on a
        // format mismatch rather than on policy. The encoding is resolved from
        // the collection's registered kind — a tagged map and a plain document
        // map share the same map header, so the bytes cannot answer it.
        let body_format = self.sparse_body_format(task.request.database_id, tenant_id, collection);
        let results: Vec<_> = fused
            .iter()
            .filter(|f| {
                if rls_filters.is_empty() {
                    return true;
                }
                // `document_id` is a fused-result string several hops from any
                // scan; a shape that fails to parse as a storage key is
                // treated the same as a row the lookup below could not find.
                let Some(key) = nodedb_types::StorageKey::parse(&f.document_id) else {
                    return false;
                };
                match self
                    .sparse
                    .get(task.request.database_id.as_u64(), tid, collection, &key)
                {
                    Ok(Some(bytes)) => {
                        let normalized =
                            sparse_body_to_msgpack(&bytes, body_format.as_format_ref());
                        super::rls_eval::rls_check_msgpack_bytes(rls_filters, &normalized)
                    }
                    _ => false,
                }
            })
            .map(|f| {
                let vector_rank = vector_results.iter().position(|r| {
                    let doc_id = vector_collection
                        .and_then(|c| c.get_surrogate(r.id))
                        .map(crate::engine::document::store::surrogate_to_doc_id)
                        .unwrap_or_else(|| format!("__local_{}", r.id));
                    doc_id == f.document_id
                });
                let text_rank = text_results.iter().position(|r| {
                    crate::engine::document::store::surrogate_to_doc_id(r.doc_id) == f.document_id
                });

                super::super::response_codec::HybridSearchHit {
                    doc_id: &f.document_id,
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
