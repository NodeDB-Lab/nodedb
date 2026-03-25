//! Vector search handlers: VectorSearch, VectorMultiSearch.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::vector::collection::VectorCollection;

/// Over-fetch multiplier when bitmap filtering is active.
/// Compensates for post-filter result reduction; 3x is effective
/// for typical filter densities (10-50% pass rate).
const BITMAP_OVER_FETCH_FACTOR: usize = 3;

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
    pub filter_bitmap: Option<&'a std::sync::Arc<[u8]>>,
    pub field_name: &'a str,
}

/// Parameters for multi-vector search (all named fields, RRF fusion).
pub(in crate::data::executor) struct VectorMultiSearchParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u32,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub top_k: usize,
    pub ef_search: usize,
    pub filter_bitmap: Option<&'a std::sync::Arc<[u8]>>,
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
        let ef = effective_ef(ef_search, top_k);
        let results = match filter_bitmap {
            Some(bitmap_bytes) => {
                collection_ref.search_with_bitmap_bytes(query_vector, top_k, ef, bitmap_bytes)
            }
            None => collection_ref.search(query_vector, top_k, ef),
        };
        let hits: Vec<_> = results
            .iter()
            .map(|r| build_search_hit(r.id, r.distance, collection_ref.get_doc_id(r.id)))
            .collect();
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
        filter_bitmap: Option<&std::sync::Arc<[u8]>>,
    ) -> Response {
        if ivf.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }
        let fetch_k = if filter_bitmap.is_some() {
            top_k * BITMAP_OVER_FETCH_FACTOR
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
            hits.retain(|h| {
                let byte_idx = h.id as usize / 8;
                let bit_idx = h.id as usize % 8;
                byte_idx < bitmap_bytes.len() && (bitmap_bytes[byte_idx] >> bit_idx) & 1 == 1
            });
            hits.truncate(top_k);
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

        // Single field — return directly.
        if all_results.len() == 1 {
            let Some(results) = all_results.into_iter().next() else {
                return self.response_error(task, ErrorCode::NotFound);
            };
            let doc_source = self.vector_collections.get(&plain_key);
            let hits: Vec<_> = results
                .iter()
                .map(|r| build_search_hit(r.id, r.distance, resolve_doc_id(doc_source, r.id)))
                .collect();
            return encode_hits_response(self, task, &hits);
        }

        // RRF fusion across fields.
        let rrf_k = 60.0_f64;
        let mut fused: std::collections::HashMap<u32, f64> = std::collections::HashMap::new();
        for results in &all_results {
            for (rank, r) in results.iter().enumerate() {
                *fused.entry(r.id).or_default() += 1.0 / (rrf_k + rank as f64 + 1.0);
            }
        }

        let mut ranked: Vec<(u32, f64)> = fused.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(top_k);

        // Look up doc_id for each fused result from any matching collection.
        let hits: Vec<_> = ranked
            .iter()
            .map(|&(id, score)| {
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
                build_search_hit(id, score as f32, doc_id)
            })
            .collect();
        encode_hits_response(self, task, &hits)
    }
}

/// Compute effective ef parameter for HNSW search.
fn effective_ef(ef_search: usize, top_k: usize) -> usize {
    if ef_search > 0 {
        ef_search.max(top_k)
    } else {
        top_k.saturating_mul(4).max(64)
    }
}
