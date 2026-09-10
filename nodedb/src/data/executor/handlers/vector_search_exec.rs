// SPDX-License-Identifier: BUSL-1.1

//! CoreLoop methods for vector search execution.

use roaring::RoaringBitmap;
use tracing::{debug, warn};

use super::vector_search::{
    VectorSearchParams, build_search_hit, effective_ef, encode_hits_response,
    surrogate_bitmap_to_global_ids,
};
use super::vector_search_ann::{ResolvedAnnOptions, apply_ann_options, quantization_matches};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_types::Surrogate;

/// Parameters for [`CoreLoop::search_ivf`].
struct SearchIvfParams<'a> {
    task: &'a ExecutionTask,
    tid: u64,
    collection: &'a str,
    index_key: &'a (nodedb_types::DatabaseId, crate::types::TenantId, String),
    ivf: &'a crate::engine::vector::ivf::IvfPqIndex,
    query_vector: &'a [f32],
    top_k: usize,
    filter_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    rls_filters: &'a [u8],
}

impl CoreLoop {
    /// Fetch the document body via the sparse engine (keyed by
    /// surrogate-hex) and attach it to the hit. Used both by the RLS path
    /// (the Control Plane evaluates the predicate against `body`) and by
    /// the slow-path SELECT (the Control Plane response translator flattens
    /// the body's fields into the hit JSON so payload columns surface to
    /// the client). When `attach == false` the hit is returned unchanged.
    ///
    /// The bytes are normalized to a standard msgpack map through the shared
    /// sparse-body normalizer, resolved from the collection's registered kind.
    /// A vector-primary collection's rows are `zerompk` TAGGED sidecars and a
    /// classic collection's are ordinary document bodies; the two are
    /// indistinguishable byte-wise, and both the RLS filter evaluator and the
    /// Control-Plane flatten read the attached body as one shape.
    #[inline]
    pub(in crate::data::executor) fn attach_body(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        attach: bool,
        mut hit: super::super::response_codec::VectorSearchHit,
    ) -> super::super::response_codec::VectorSearchHit {
        if !attach {
            return hit;
        }
        let hex = surrogate_to_doc_id(Surrogate::new(hit.id));
        if let Ok(Some(bytes)) = self.sparse.get(database_id, tid, collection, &hex) {
            let format = self.sparse_body_format(
                crate::types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tid),
                collection,
            );
            // Owned: the hit outlives `bytes`, which is the storage read's
            // local buffer.
            hit.body = Some(
                crate::data::executor::scan_normalize::sparse_body_to_msgpack(
                    &bytes,
                    format.as_format_ref(),
                )
                .into_owned(),
            );
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
            metric,
            filter_bitmap,
            field_name,
            rls_filters,
            inline_prefilter_plan,
            ann_options,
            skip_payload_fetch,
            payload_filters,
        } = params;
        // RLS requires body fetch regardless of projection. If RLS filters are
        // active, ignore the skip flag and record why at debug level.
        let skip_payload_fetch = if skip_payload_fetch && !rls_filters.is_empty() {
            debug!(
                core = self.core_id,
                %collection,
                reason = "rls",
                "skip_payload_fetch suppressed: RLS filters present"
            );
            false
        } else {
            skip_payload_fetch
        };

        let ResolvedAnnOptions {
            ef_search,
            oversample,
        } = apply_ann_options(self.core_id, collection, ef_search, ann_options);

        // Materialize cross-engine prefilter sub-plan (e.g. ARRAY_SLICE
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

        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);

        // Check for IVF-PQ index first.
        if let Some(ivf) = self.ivf_indexes.get(&index_key) {
            return self.search_ivf(SearchIvfParams {
                task,
                tid,
                collection,
                index_key: &index_key,
                ivf,
                query_vector,
                top_k,
                filter_bitmap,
                rls_filters,
            });
        }

        // Default: HNSW collection.
        // If the specific field-named index does not exist, fall back to the
        // empty-field index. This handles data synced from NodeDB-Lite (which
        // uses collection-level storage, not named-field storage) being
        // searched via a field-specific SQL query (e.g. vector_distance(embedding, ...)).
        let effective_key =
            if !self.vector_collections.contains_key(&index_key) && !field_name.is_empty() {
                let fallback_key = CoreLoop::vector_index_key(database_id, tid, collection, "");
                if self.vector_collections.contains_key(&fallback_key) {
                    fallback_key
                } else {
                    index_key
                }
            } else {
                index_key
            };
        let Some(collection_ref) = self.vector_collections.get(&effective_key) else {
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

        // Derive payload bitmap (node-id space) from `(field, value)`
        // equalities by intersecting per-field equality bitmaps. Returns
        // `None` when no payload filters were requested or any filter
        // references a field with no registered payload index.
        let payload_bm: Option<RoaringBitmap> = if payload_filters.is_empty() {
            None
        } else {
            let preds: Vec<nodedb_vector::collection::FilterPredicate> = payload_filters
                .iter()
                .map(|atom| match atom {
                    nodedb_types::PayloadAtom::Eq(f, v) => {
                        nodedb_vector::collection::FilterPredicate::Eq {
                            field: f.to_ascii_lowercase(),
                            value: v.clone(),
                        }
                    }
                    nodedb_types::PayloadAtom::In(f, vs) => {
                        nodedb_vector::collection::FilterPredicate::In {
                            field: f.to_ascii_lowercase(),
                            values: vs.clone(),
                        }
                    }
                    nodedb_types::PayloadAtom::Range {
                        field,
                        low,
                        low_inclusive,
                        high,
                        high_inclusive,
                    } => nodedb_vector::collection::FilterPredicate::Range {
                        field: field.to_ascii_lowercase(),
                        low: low.clone(),
                        low_inclusive: *low_inclusive,
                        high: high.clone(),
                        high_inclusive: *high_inclusive,
                    },
                    _ => nodedb_vector::collection::FilterPredicate::And(vec![]),
                })
                .collect();
            let conj = nodedb_vector::collection::FilterPredicate::And(preds);
            collection_ref.payload.pre_filter(&conj)
        };

        let combined_bm: Option<RoaringBitmap> = match (filter_bitmap, payload_bm) {
            (Some(surrogate_bm), Some(pbm)) => {
                let mut bm = surrogate_bitmap_to_global_ids(collection_ref, surrogate_bm);
                bm &= pbm;
                Some(bm)
            }
            (Some(surrogate_bm), None) => {
                Some(surrogate_bitmap_to_global_ids(collection_ref, surrogate_bm))
            }
            (None, Some(pbm)) => Some(pbm),
            (None, None) => None,
        };

        let results = match combined_bm {
            Some(local_bm) => {
                let mut buf = Vec::with_capacity(local_bm.serialized_size());
                if local_bm.serialize_into(&mut buf).is_ok() {
                    collection_ref.search_with_bitmap_bytes_and_metric(
                        query_vector,
                        fetch_k,
                        ef,
                        &buf,
                        metric,
                    )
                } else {
                    collection_ref.search_with_metric(query_vector, fetch_k, ef, metric)
                }
            }
            None => collection_ref.search_with_metric(query_vector, fetch_k, ef, metric),
        };

        // Pure-vector fast path: projection contains only id/distance.
        // Skip the sparse-store body fetch entirely.
        if skip_payload_fetch {
            let mut hits: Vec<_> = results
                .iter()
                .map(|r| build_search_hit(Some(collection_ref), r.id, r.distance))
                .collect();
            // Read-your-own-writes for vector search: fold this
            // transaction's staged vector inserts into the base HNSW/IVF
            // result before truncation, so a vector inserted earlier in the
            // same transaction is ranked in by true distance before COMMIT.
            if let Some(txn_id) = task.request.txn_id {
                if let Err(e) = self.merge_vector_overlay_into_search(
                    super::transaction::overlay::VectorMergeParams {
                        txn_id,
                        database_id: task.request.database_id,
                        tid: crate::types::TenantId::new(tid),
                        collection,
                        field_name,
                        query_vector,
                        metric,
                        top_k,
                        filter_bitmap,
                        payload_filters,
                    },
                    &mut hits,
                ) {
                    return self.response_error(task, e);
                }
            } else {
                hits.truncate(top_k);
            }
            if let Some(ref m) = self.metrics {
                m.record_vector_search(0);
                m.record_query_by_engine("vector");
            }
            return encode_hits_response(self, task, &hits);
        }

        // RLS evaluation lives at the Control-Plane response boundary
        // (`response_translate::vector`). DP attaches the document body
        // when filters are active so CP can run the predicate without
        // a follow-up round-trip; CP applies the filter and truncates to
        // `top_k`. Data Plane stays pure SIMD + sparse-fetch.
        // Attach body bytes whenever skip_payload_fetch is false (slow path)
        // OR when RLS filters need them; the CP response translator flattens
        // the bytes' fields into the hit JSON for client column projection.
        let attach = !skip_payload_fetch || !rls_filters.is_empty();
        let mut hits: Vec<_> = results
            .iter()
            .map(|r| build_search_hit(Some(collection_ref), r.id, r.distance))
            .map(|hit| {
                self.attach_body(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    attach,
                    hit,
                )
            })
            .collect();
        let truncate_to = if rls_filters.is_empty() {
            top_k
        } else {
            fetch_k
        };
        // Read-your-own-writes for vector search: fold this transaction's
        // staged vector inserts into the base HNSW/IVF result before
        // truncation, so a vector inserted earlier in the same transaction
        // is ranked in by true distance before COMMIT.
        if let Some(txn_id) = task.request.txn_id {
            if let Err(e) = self.merge_vector_overlay_into_search(
                super::transaction::overlay::VectorMergeParams {
                    txn_id,
                    database_id: task.request.database_id,
                    tid: crate::types::TenantId::new(tid),
                    collection,
                    field_name,
                    query_vector,
                    metric,
                    top_k: truncate_to,
                    filter_bitmap,
                    payload_filters,
                },
                &mut hits,
            ) {
                return self.response_error(task, e);
            }
        } else {
            hits.truncate(truncate_to);
        }
        if let Some(ref m) = self.metrics {
            m.record_vector_search(0);
            m.record_query_by_engine("vector");
        }
        encode_hits_response(self, task, &hits)
    }

    /// Search an IVF-PQ index with optional bitmap post-filtering.
    fn search_ivf(&self, params: SearchIvfParams<'_>) -> Response {
        let SearchIvfParams {
            task,
            tid,
            collection,
            index_key,
            ivf,
            query_vector,
            top_k,
            filter_bitmap,
            rls_filters,
        } = params;
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
                .map(|h| {
                    self.attach_body(task.request.database_id.as_u64(), tid, collection, true, h)
                })
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
}
