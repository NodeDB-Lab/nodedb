//! Vector search handlers: VectorSearch, VectorMultiSearch.
//!
//! DP emits each hit's `id` as the bound `Surrogate.as_u32()` (or the
//! local node id if the row is headless / pre-surrogate). `doc_id` is
//! always `None` from DP; the Control Plane fills it via the catalog
//! at the response boundary.

use nodedb_types::Surrogate;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::vector::collection::VectorCollection;

/// Build a search hit from raw search result data. `id` is the bound
/// surrogate when present, else the local node id (so headless rows
/// still round-trip).
fn build_search_hit(
    collection: Option<&VectorCollection>,
    local_id: u32,
    distance: f32,
) -> super::super::response_codec::VectorSearchHit {
    let id = collection
        .and_then(|c| c.get_surrogate(local_id))
        .map(|s| s.as_u32())
        .unwrap_or(local_id);
    super::super::response_codec::VectorSearchHit {
        id,
        distance,
        doc_id: None,
    }
}

/// Surrogate-as-u32 resolver kept for IVF-PQ bitmap filter compatibility.
fn resolve_surrogate(collection: Option<&VectorCollection>, vector_id: u32) -> Option<Surrogate> {
    collection.and_then(|c| c.get_surrogate(vector_id))
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

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

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
        // RLS post-filter for vector search now lives at the Control
        // Plane response boundary (post-surrogate→PK translation). DP
        // returns the raw HNSW top-K and ignores `rls_filters`.
        let _ = rls_filters;
        let fetch_k = top_k;
        let ef = effective_ef(ef_search, fetch_k);
        let results = match filter_bitmap {
            Some(bitmap_bytes) => {
                collection_ref.search_with_bitmap_bytes(query_vector, fetch_k, ef, bitmap_bytes)
            }
            None => collection_ref.search(query_vector, fetch_k, ef),
        };

        let hits: Vec<_> = results
            .iter()
            .map(|r| build_search_hit(Some(collection_ref), r.id, r.distance))
            .collect();
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
        index_key: &(crate::types::TenantId, String),
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
        let surrogate_source = self.vector_collections.get(index_key);

        let mut hits: Vec<_> = results
            .iter()
            .map(|r| build_search_hit(surrogate_source, r.id, r.distance))
            .collect();

        if let Some(bitmap_bytes) = filter_bitmap {
            if let Ok(bm) = crate::query::bitmap::deserialize(bitmap_bytes) {
                // Bitmap is a set of surrogates; hit.id is now the surrogate.
                hits.retain(|h| bm.contains(h.id));
                let _ = resolve_surrogate; // kept available for future bitmap modes
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

        let tenant_id = crate::types::TenantId::new(tid);
        let plain_key = CoreLoop::vector_index_key(tid, collection, "");
        // A named-field key looks like `"{collection}:{field_name}"` in the String part.
        let field_prefix = format!("{collection}:");

        let mut all_results: Vec<Vec<crate::engine::vector::hnsw::SearchResult>> = Vec::new();

        for (key, coll) in &self.vector_collections {
            if key.0 != tenant_id {
                continue;
            }
            if key == &plain_key || key.1.starts_with(&field_prefix) {
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

        // RLS for vector multi-search moved to CP response boundary.
        let _ = rls_filters;

        // Single field — return directly.
        if all_results.len() == 1 {
            let Some(results) = all_results.into_iter().next() else {
                return self.response_error(task, ErrorCode::NotFound);
            };
            let doc_source = self.vector_collections.get(&plain_key);
            let hits: Vec<_> = results
                .iter()
                .take(top_k)
                .map(|r| build_search_hit(doc_source, r.id, r.distance))
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

        // Surface fused results with surrogate-as-id; CP fills doc_id.
        let _ = (tid, collection);
        let hits: Vec<_> = fused
            .iter()
            .filter_map(|f| {
                let local_id: u32 = f.document_id.parse().ok()?;
                let source = self.vector_collections.get(&plain_key).or_else(|| {
                    self.vector_collections
                        .iter()
                        .filter(|(k, _)| {
                            k.0 == tenant_id && (k == &&plain_key || k.1.starts_with(&field_prefix))
                        })
                        .map(|(_, c)| c)
                        .next()
                });
                Some(build_search_hit(source, local_id, f.rrf_score as f32))
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
        top_k.saturating_mul(4).clamp(64, MAX_EF_SEARCH)
    }
}
