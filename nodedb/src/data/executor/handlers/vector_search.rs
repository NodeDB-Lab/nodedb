//! Vector search handlers: VectorSearch, VectorMultiSearch.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::vector::collection::VectorCollection;

/// Build a search hit from raw search result data.
fn build_search_hit(
    id: u32,
    distance: f32,
    doc_id: Option<&str>,
) -> super::super::response_codec::VectorSearchHit {
    super::super::response_codec::VectorSearchHit {
        id,
        distance,
        doc_id: doc_id.map(String::from),
    }
}

/// Resolve a doc_id from a VectorCollection, if available.
fn resolve_doc_id(collection: Option<&VectorCollection>, vector_id: u32) -> Option<&str> {
    collection.and_then(|c| c.get_doc_id(vector_id))
}

/// Encode search hits and return response.
fn encode_hits_response(
    core: &CoreLoop,
    task: &ExecutionTask,
    hits: &Vec<super::super::response_codec::VectorSearchHit>,
) -> Response {
    match super::super::response_codec::encode(hits) {
        Ok(payload) => core.response_with_payload(task, payload),
        Err(e) => {
            warn!(core = core.core_id, error = %e, "vector search serialization failed");
            core.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            )
        }
    }
}

/// Parameters for vector search.
pub(in crate::data::executor) struct VectorSearchParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u32,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub top_k: usize,
    pub ef_search: usize,
    pub filter_bitmap: Option<&'a [u8]>,
    pub field_name: &'a str,
    /// RLS post-candidate filters. Applied after HNSW/IVF returns candidates.
    pub rls_filters: &'a [u8],
}

/// Parameters for multi-vector search (all named fields, RRF fusion).
pub(in crate::data::executor) struct VectorMultiSearchParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u32,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub top_k: usize,
    pub ef_search: usize,
    pub filter_bitmap: Option<&'a [u8]>,
    /// RLS post-candidate filters (evaluated per-candidate after RRF fusion).
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_vector_search(
        &self,
        params: VectorSearchParams<'_>,
    ) -> Response {
        let VectorSearchParams {
            task,
            tid,
            collection,
            query_vector,
            top_k,
            ef_search,
            filter_bitmap,
            field_name,
            rls_filters,
        } = params;
        debug!(core = self.core_id, %collection, top_k, ef_search, "vector search");
        let index_key = CoreLoop::vector_index_key(tid, collection, field_name);

        // Check for IVF-PQ index first.
        if let Some(ivf) = self.ivf_indexes.get(&index_key) {
            return self.search_ivf(task, &index_key, ivf, query_vector, top_k, filter_bitmap);
        }

        // Default: HNSW collection.
        let Some(collection_ref) = self.vector_collections.get(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        if collection_ref.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }
        // Fetch extra candidates when RLS is active, since some will be filtered.
        let fetch_k = if rls_filters.is_empty() {
            top_k
        } else {
            top_k.saturating_mul(2).max(20)
        };
        let ef = effective_ef(ef_search, fetch_k);
        let results = match filter_bitmap {
            Some(bitmap_bytes) => {
                collection_ref.search_with_bitmap_bytes(query_vector, fetch_k, ef, bitmap_bytes)
            }
            None => collection_ref.search(query_vector, fetch_k, ef),
        };

        // RLS post-candidate filtering: look up each candidate's document.
        // For strict collections, decode binary tuples via schema before filter eval.
        let config_key = format!("{tid}:{collection}");
        let strict_schema = self.doc_configs.get(&config_key).and_then(|config| {
            if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                config.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        let hits: Vec<_> = if rls_filters.is_empty() {
            results
                .iter()
                .map(|r| build_search_hit(r.id, r.distance, collection_ref.get_doc_id(r.id)))
                .collect()
        } else {
            results
                .iter()
                .filter(|r| {
                    let doc_id_str = match collection_ref.get_doc_id(r.id) {
                        Some(id) if !id.is_empty() => id,
                        _ => return false,
                    };
                    match self.sparse.get(tid, collection, doc_id_str) {
                        Ok(Some(bytes)) => {
                            // For strict collections, decode binary tuple → JSON for filter eval.
                            if let Some(ref schema) = strict_schema {
                                if let Some(json) =
                                    super::super::strict_format::binary_tuple_to_json(
                                        &bytes, schema,
                                    )
                                {
                                    let json_bytes = sonic_rs::to_vec(&json).unwrap_or_default();
                                    super::rls_eval::rls_check_msgpack_bytes(
                                        rls_filters,
                                        &json_bytes,
                                    )
                                } else {
                                    false
                                }
                            } else {
                                super::rls_eval::rls_check_msgpack_bytes(rls_filters, &bytes)
                            }
                        }
                        _ => false,
                    }
                })
                .take(top_k)
                .map(|r| build_search_hit(r.id, r.distance, collection_ref.get_doc_id(r.id)))
                .collect()
        };
        if let Some(ref m) = self.metrics {
            m.record_vector_search(0);
            m.record_query_by_engine("vector");
        }
        encode_hits_response(self, task, &hits)
    }

    /// Search an IVF-PQ index with optional bitmap post-filtering.
    fn search_ivf(
        &self,
        task: &ExecutionTask,
        index_key: &str,
        ivf: &crate::engine::vector::ivf::IvfPqIndex,
        query_vector: &[f32],
        top_k: usize,
        filter_bitmap: Option<&[u8]>,
    ) -> Response {
        if ivf.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }
        let fetch_k = if filter_bitmap.is_some() {
            top_k * self.query_tuning.bitmap_over_fetch_factor
        } else {
            top_k
        };
        let results = ivf.search(query_vector, fetch_k);
        let doc_id_source = self.vector_collections.get(index_key);

        let mut hits: Vec<_> = results
            .iter()
            .map(|r| build_search_hit(r.id, r.distance, resolve_doc_id(doc_id_source, r.id)))
            .collect();

        if let Some(bitmap_bytes) = filter_bitmap {
            if let Ok(bm) = crate::query::bitmap::deserialize(bitmap_bytes) {
                hits.retain(|h| bm.contains(h.id));
            }
            hits.truncate(top_k);
        }

        if let Some(ref m) = self.metrics {
            m.record_vector_search(0);
            m.record_query_by_engine("vector");
        }
        encode_hits_response(self, task, &hits)
    }

    /// Multi-vector search: query all named vector fields in a collection,
    /// fuse results via RRF.
    pub(in crate::data::executor) fn execute_vector_multi_search(
        &self,
        params: VectorMultiSearchParams<'_>,
    ) -> Response {
        let VectorMultiSearchParams {
            task,
            tid,
            collection,
            query_vector,
            top_k,
            ef_search,
            filter_bitmap,
            rls_filters,
        } = params;
        debug!(core = self.core_id, %collection, top_k, "vector multi-search");

        let prefix = format!("{tid}:{collection}:");
        let plain_key = CoreLoop::vector_index_key(tid, collection, "");

        let mut all_results: Vec<Vec<crate::engine::vector::hnsw::SearchResult>> = Vec::new();

        for (key, coll) in &self.vector_collections {
            if key == &plain_key || key.starts_with(&prefix) {
                if coll.is_empty() || coll.dim() != query_vector.len() {
                    continue;
                }
                let ef = effective_ef(ef_search, top_k);
                let results = match filter_bitmap {
                    Some(bm) => coll.search_with_bitmap_bytes(query_vector, top_k, ef, bm),
                    None => coll.search(query_vector, top_k, ef),
                };
                all_results.push(results);
            }
        }

        if all_results.is_empty() {
            return self.response_error(task, ErrorCode::NotFound);
        }

        // Single field — return directly (with RLS filtering).
        if all_results.len() == 1 {
            let Some(results) = all_results.into_iter().next() else {
                return self.response_error(task, ErrorCode::NotFound);
            };
            let doc_source = self.vector_collections.get(&plain_key);
            let hits: Vec<_> = results
                .iter()
                .filter(|r| {
                    if rls_filters.is_empty() {
                        return true;
                    }
                    let doc_id_str = resolve_doc_id(doc_source, r.id).unwrap_or("");
                    if doc_id_str.is_empty() {
                        return false;
                    }
                    match self.sparse.get(tid, collection, doc_id_str) {
                        Ok(Some(bytes)) => {
                            super::rls_eval::rls_check_msgpack_bytes(rls_filters, &bytes)
                        }
                        _ => false,
                    }
                })
                .take(top_k)
                .map(|r| build_search_hit(r.id, r.distance, resolve_doc_id(doc_source, r.id)))
                .collect();
            if let Some(ref m) = self.metrics {
                m.record_vector_search(0);
                m.record_query_by_engine("vector");
            }
            return encode_hits_response(self, task, &hits);
        }

        // RRF fusion across fields using shared fusion module.
        use crate::query::fusion::{RankedResult, reciprocal_rank_fusion};

        let ranked_lists: Vec<Vec<RankedResult>> = all_results
            .iter()
            .map(|results| {
                results
                    .iter()
                    .enumerate()
                    .map(|(rank, r)| RankedResult {
                        document_id: r.id.to_string(),
                        rank,
                        score: r.distance,
                        source: "vector",
                    })
                    .collect()
            })
            .collect();

        let fused = reciprocal_rank_fusion(&ranked_lists, None, top_k);

        // Look up doc_id for each fused result, apply RLS post-fusion filtering.
        let hits: Vec<_> = fused
            .iter()
            .filter_map(|f| {
                let id: u32 = f.document_id.parse().ok()?;
                let doc_id = self
                    .vector_collections
                    .get(&plain_key)
                    .and_then(|c| c.get_doc_id(id))
                    .or_else(|| {
                        self.vector_collections
                            .iter()
                            .filter(|(k, _)| *k == &plain_key || k.starts_with(&prefix))
                            .find_map(|(_, c)| c.get_doc_id(id))
                    });
                // RLS post-fusion: look up document and evaluate filters.
                if !rls_filters.is_empty() {
                    let doc_id_str = doc_id.unwrap_or("");
                    if doc_id_str.is_empty() {
                        return None;
                    }
                    match self.sparse.get(tid, collection, doc_id_str) {
                        Ok(Some(bytes))
                            if super::rls_eval::rls_check_msgpack_bytes(rls_filters, &bytes) => {}
                        _ => return None,
                    }
                }
                Some(build_search_hit(id, f.rrf_score as f32, doc_id))
            })
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_vector_search(0);
            m.record_query_by_engine("vector");
        }
        encode_hits_response(self, task, &hits)
    }
}

/// Maximum allowed ef_search value. Prevents DoS via unbounded beam width.
const MAX_EF_SEARCH: usize = 8192;

/// Compute effective ef parameter for HNSW search.
fn effective_ef(ef_search: usize, top_k: usize) -> usize {
    if ef_search > 0 {
        ef_search.max(top_k).min(MAX_EF_SEARCH)
    } else {
        top_k.saturating_mul(4).max(64).min(MAX_EF_SEARCH)
    }
}
