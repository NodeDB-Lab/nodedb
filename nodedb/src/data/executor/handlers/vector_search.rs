//! Vector search handlers: VectorSearch, VectorMultiSearch.
//!
//! DP emits each hit's `id` as the bound `Surrogate.as_u32()` (or the
//! local node id if the row is headless / pre-surrogate). `doc_id` is
//! always `None` from DP; the Control Plane fills it via the catalog
//! at the response boundary.

use roaring::RoaringBitmap;
use tracing::{debug, warn};

use super::vector_search_ann::{ResolvedAnnOptions, apply_ann_options, quantization_matches};
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
        body: None,
    }
}

/// Translate a `SurrogateBitmap` (keyed by global surrogate IDs) into a
/// `RoaringBitmap` keyed by the collection's global vector IDs.
///
/// The HNSW search layer checks candidate eligibility by testing whether
/// `(local_node_id + segment_base_id)` is present in the bitmap. Global
/// vector IDs are the collection's own monotonic counter, distinct from
/// surrogate IDs allocated by the Control Plane. Using surrogate IDs
/// directly would silently pass or reject wrong nodes.
///
/// Surrogates without a recorded local mapping (e.g. headless inserts) are
/// omitted from the result bitmap — they would never match anyway.
fn surrogate_bitmap_to_global_ids(
    collection: &VectorCollection,
    surrogate_bm: &nodedb_types::SurrogateBitmap,
) -> RoaringBitmap {
    let mut local_bm = RoaringBitmap::new();
    for surrogate in surrogate_bm.iter() {
        if let Some(&global_id) = collection.surrogate_to_local.get(&surrogate) {
            local_bm.insert(global_id);
        }
    }
    local_bm
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
    pub filter_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    pub field_name: &'a str,
    /// RLS post-candidate filters. Applied after HNSW/IVF returns candidates.
    pub rls_filters: &'a [u8],
    /// Cross-engine prefilter sub-plan: when `Some`, executed locally and
    /// its output rows materialized into a `SurrogateBitmap` that is
    /// intersected with `filter_bitmap` before HNSW search.
    pub inline_prefilter_plan: Option<&'a crate::bridge::envelope::PhysicalPlan>,
    /// ANN tuning knobs from the SQL caller.
    pub ann_options: &'a nodedb_types::VectorAnnOptions,
}

/// Parameters for multi-vector search (all named fields, RRF fusion).
pub(in crate::data::executor) struct VectorMultiSearchParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u32,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub top_k: usize,
    pub ef_search: usize,
    pub filter_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    /// RLS post-candidate filters (evaluated per-candidate after RRF fusion).
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    /// When RLS filters are active, fetch the document body via the
    /// sparse engine (keyed by surrogate-hex) and attach it to the hit so
    /// the Control Plane response translator can evaluate the predicate
    /// at the security boundary. The Data Plane does not interpret the
    /// filter — it only ferries bytes. When `rls_filters` is empty the
    /// hit is returned unchanged.
    #[inline]
    fn attach_body_for_rls(
        &self,
        tid: u32,
        collection: &str,
        rls_filters: &[u8],
        mut hit: super::super::response_codec::VectorSearchHit,
    ) -> super::super::response_codec::VectorSearchHit {
        if rls_filters.is_empty() {
            return hit;
        }
        let hex = format!("{:08x}", hit.id);
        if let Ok(Some(bytes)) = self.sparse.get(tid, collection, &hex) {
            hit.body = Some(bytes);
        }
        hit
    }

    pub(in crate::data::executor) fn execute_vector_search(
        &mut self,
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
            inline_prefilter_plan,
            ann_options,
        } = params;

        let ResolvedAnnOptions {
            ef_search,
            oversample,
        } = apply_ann_options(self.core_id, collection, ef_search, ann_options);

        // Materialize cross-engine prefilter sub-plan (e.g. NDARRAY_SLICE
        // → surrogate bitmap) and intersect with any pre-existing
        // `filter_bitmap`. The sub-plan emits document-shaped rows whose
        // `id` is the cell's surrogate as 8-char zero-padded lowercase
        // hex; `collect_surrogates` decodes that back into surrogate IDs.
        let inline_bitmap = inline_prefilter_plan.map(|sub_plan| {
            crate::data::executor::dispatch::bitmap::hashjoin_inline::run_bitmap_subplan(
                self, task, sub_plan,
            )
        });
        let effective_filter: Option<nodedb_types::SurrogateBitmap> =
            match (filter_bitmap.cloned(), inline_bitmap) {
                (Some(a), Some(b)) => Some(a.intersect(&b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) if !b.is_empty() => Some(b),
                _ => None,
            };
        let filter_bitmap = effective_filter.as_ref();
        debug!(core = self.core_id, %collection, top_k, ef_search, "vector search");

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let index_key = CoreLoop::vector_index_key(tid, collection, field_name);

        // Check for IVF-PQ index first.
        if let Some(ivf) = self.ivf_indexes.get(&index_key) {
            return self.search_ivf(
                task,
                tid,
                collection,
                &index_key,
                ivf,
                query_vector,
                top_k,
                filter_bitmap,
                rls_filters,
            );
        }

        // Default: HNSW collection.
        let Some(collection_ref) = self.vector_collections.get(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        if collection_ref.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }

        // Quantization mismatch: if the SQL caller requested a specific
        // quantization that differs from what the index actually uses, warn
        // once per query and proceed with the collection's actual quantization.
        // Per-collection codec dispatch will honor the hint when that lands.
        if let Some(requested_q) = ann_options.quantization {
            let index_q = collection_ref.stats().quantization;
            if !quantization_matches(requested_q, index_q) {
                warn!(
                    core = self.core_id,
                    %collection,
                    requested = ?requested_q,
                    actual = %index_q,
                    "ann_options: quantization hint does not match index; proceeding with index quantization"
                );
            }
        }

        // Over-fetch to accommodate both oversample breadth (for re-rank
        // headroom) and RLS post-filter headroom. The two factors are
        // multiplied so each can independently request more candidates.
        let fetch_k = if rls_filters.is_empty() {
            top_k.saturating_mul(oversample)
        } else {
            top_k.saturating_mul(2).saturating_mul(oversample).max(20)
        };
        let ef = effective_ef(ef_search, fetch_k);
        let results = match filter_bitmap {
            Some(surrogate_bm) => {
                let local_bm = surrogate_bitmap_to_global_ids(collection_ref, surrogate_bm);
                let mut buf = Vec::with_capacity(local_bm.serialized_size());
                if local_bm.serialize_into(&mut buf).is_ok() {
                    collection_ref.search_with_bitmap_bytes(query_vector, fetch_k, ef, &buf)
                } else {
                    collection_ref.search(query_vector, fetch_k, ef)
                }
            }
            None => collection_ref.search(query_vector, fetch_k, ef),
        };

        // RLS evaluation lives at the Control-Plane response boundary
        // (`response_translate::vector`). DP attaches the document body
        // when filters are active so CP can run the predicate without
        // a follow-up round-trip; CP applies the filter and truncates to
        // `top_k`. Data Plane stays pure SIMD + sparse-fetch.
        let hits: Vec<_> = results
            .iter()
            .map(|r| build_search_hit(Some(collection_ref), r.id, r.distance))
            .map(|hit| self.attach_body_for_rls(tid, collection, rls_filters, hit))
            .take(if rls_filters.is_empty() {
                top_k
            } else {
                fetch_k
            })
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_vector_search(0);
            m.record_query_by_engine("vector");
        }
        encode_hits_response(self, task, &hits)
    }

    /// Search an IVF-PQ index with optional bitmap post-filtering.
    #[allow(clippy::too_many_arguments)]
    fn search_ivf(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        index_key: &(crate::types::TenantId, String),
        ivf: &crate::engine::vector::ivf::IvfPqIndex,
        query_vector: &[f32],
        top_k: usize,
        filter_bitmap: Option<&nodedb_types::SurrogateBitmap>,
        rls_filters: &[u8],
    ) -> Response {
        if ivf.is_empty() {
            return self.response_with_payload(task, b"[]".to_vec());
        }
        let fetch_k = if filter_bitmap.is_some() || !rls_filters.is_empty() {
            top_k * self.query_tuning.bitmap_over_fetch_factor.max(2)
        } else {
            top_k
        };
        let results = ivf.search(query_vector, fetch_k);
        let surrogate_source = self.vector_collections.get(index_key);

        let mut hits: Vec<_> = results
            .iter()
            .map(|r| build_search_hit(surrogate_source, r.id, r.distance))
            .collect();

        if let Some(surrogate_bm) = filter_bitmap {
            // Bitmap is a set of surrogates; hit.id is now the surrogate.
            hits.retain(|h| surrogate_bm.0.contains(h.id));
        }
        if !rls_filters.is_empty() {
            // CP-side translator runs the predicate; DP only attaches body.
            hits = hits
                .into_iter()
                .map(|h| self.attach_body_for_rls(tid, collection, rls_filters, h))
                .collect();
        } else {
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

        // Over-fetch when RLS is active so the CP-side post-filter has
        // headroom to still return `top_k` after rejecting candidates.
        let fetch_k = if rls_filters.is_empty() {
            top_k
        } else {
            top_k.saturating_mul(2).max(20)
        };

        let mut all_results: Vec<Vec<crate::engine::vector::hnsw::SearchResult>> = Vec::new();

        for (key, coll) in &self.vector_collections {
            if key.0 != tenant_id {
                continue;
            }
            if key == &plain_key || key.1.starts_with(&field_prefix) {
                if coll.is_empty() || coll.dim() != query_vector.len() {
                    continue;
                }
                let ef = effective_ef(ef_search, fetch_k);
                let results = match filter_bitmap {
                    Some(surrogate_bm) => {
                        let local_bm = surrogate_bitmap_to_global_ids(coll, surrogate_bm);
                        let mut buf = Vec::with_capacity(local_bm.serialized_size());
                        if local_bm.serialize_into(&mut buf).is_ok() {
                            coll.search_with_bitmap_bytes(query_vector, fetch_k, ef, &buf)
                        } else {
                            coll.search(query_vector, fetch_k, ef)
                        }
                    }
                    None => coll.search(query_vector, fetch_k, ef),
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
                .map(|r| build_search_hit(doc_source, r.id, r.distance))
                .map(|hit| self.attach_body_for_rls(tid, collection, rls_filters, hit))
                .take(fetch_k)
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

        // Surface fused results with surrogate-as-id; CP fills doc_id and
        // applies the RLS predicate at the response boundary.
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
                let hit = build_search_hit(source, local_id, f.rrf_score as f32);
                Some(self.attach_body_for_rls(tid, collection, rls_filters, hit))
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

#[cfg(test)]
mod tests {
    use nodedb_types::{Surrogate, SurrogateBitmap};

    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::hnsw::HnswParams;

    use super::surrogate_bitmap_to_global_ids;

    /// Build a `VectorCollection` with `n` vectors of dimension 1.
    /// Vector `i` is `[i as f32]` and is bound to `Surrogate(i as u32 + 1)`
    /// (surrogates are 1-based to distinguish them from local IDs).
    fn make_collection_with_surrogates(n: usize) -> VectorCollection {
        let mut coll = VectorCollection::new(1, HnswParams::default());
        for i in 0..n {
            let surrogate = Surrogate(i as u32 + 1);
            coll.insert_with_surrogate(vec![i as f32], surrogate);
        }
        coll
    }

    /// Verify the oversample-based fetch_k arithmetic in isolation.
    /// oversample=3, top_k=10, no RLS → fetch_k = 30.
    /// oversample=3, top_k=10, RLS active → fetch_k = max(10*2*3, 20) = 60.
    #[test]
    fn oversample_fetch_k_arithmetic() {
        let top_k: usize = 10;

        // No RLS, oversample=3.
        let oversample: usize = 3;
        let fetch_k_no_rls = top_k.saturating_mul(oversample);
        assert_eq!(
            fetch_k_no_rls, 30,
            "no-RLS oversample=3 fetch_k should be 30"
        );

        // RLS active, oversample=3.
        let rls_active = true;
        let fetch_k_rls = if rls_active {
            top_k.saturating_mul(2).saturating_mul(oversample).max(20)
        } else {
            top_k.saturating_mul(oversample)
        };
        assert_eq!(fetch_k_rls, 60, "RLS oversample=3 fetch_k should be 60");

        // oversample=1 (default) → no change from baseline.
        let oversample_default: usize = 1;
        let fetch_k_default = top_k.saturating_mul(oversample_default);
        assert_eq!(
            fetch_k_default, 10,
            "oversample=1 fetch_k should equal top_k"
        );
    }

    #[test]
    fn surrogate_bitmap_translates_to_correct_global_ids() {
        let coll = make_collection_with_surrogates(10);

        // Allow only surrogates 1, 3, 5 (global vector IDs 0, 2, 4).
        let mut bm = SurrogateBitmap::new();
        bm.insert(Surrogate(1));
        bm.insert(Surrogate(3));
        bm.insert(Surrogate(5));

        let local_bm = surrogate_bitmap_to_global_ids(&coll, &bm);

        assert!(local_bm.contains(0), "Surrogate(1) → global_id 0");
        assert!(local_bm.contains(2), "Surrogate(3) → global_id 2");
        assert!(local_bm.contains(4), "Surrogate(5) → global_id 4");
        assert!(
            !local_bm.contains(1),
            "Surrogate(2) not in bitmap → global_id 1 absent"
        );
        assert!(
            !local_bm.contains(3),
            "Surrogate(4) not in bitmap → global_id 3 absent"
        );
        assert_eq!(local_bm.len(), 3);
    }

    #[test]
    fn non_member_surrogates_never_appear_in_search_results() {
        // Insert 20 vectors, bind each to a unique surrogate.
        let coll = make_collection_with_surrogates(20);

        // Permit only even surrogates: Surrogate(2), Surrogate(4), ..., Surrogate(20).
        // Corresponding global IDs: 1, 3, ..., 19.
        let mut surrogate_bm = SurrogateBitmap::new();
        for i in (2u32..=20).step_by(2) {
            surrogate_bm.insert(Surrogate(i));
        }

        // Translate to local IDs and serialise for HNSW.
        let local_bm = surrogate_bitmap_to_global_ids(&coll, &surrogate_bm);
        let mut buf = Vec::new();
        local_bm.serialize_into(&mut buf).unwrap();

        // Search for nearest neighbours — all results must be even surrogates.
        let results = coll.search_with_bitmap_bytes(&[10.0], 5, 64, &buf);

        assert!(!results.is_empty(), "expected at least one result");
        for r in &results {
            // `r.id` is the global vector ID; the bound surrogate is global_id + 1.
            let surrogate = Surrogate(r.id + 1);
            assert!(
                surrogate_bm.contains(surrogate),
                "result surrogate {:?} (global_id={}) is not in the filter bitmap",
                surrogate,
                r.id
            );
        }
    }

    #[test]
    fn empty_surrogate_bitmap_returns_empty_results() {
        let coll = make_collection_with_surrogates(10);
        let empty_bm = SurrogateBitmap::new();

        let local_bm = surrogate_bitmap_to_global_ids(&coll, &empty_bm);
        let mut buf = Vec::new();
        local_bm.serialize_into(&mut buf).unwrap();

        let results = coll.search_with_bitmap_bytes(&[5.0], 5, 64, &buf);
        assert!(results.is_empty(), "empty bitmap should yield no results");
    }
}
