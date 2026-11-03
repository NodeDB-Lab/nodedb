// SPDX-License-Identifier: BUSL-1.1

//! `CoreLoop::execute_vector_multi_search` -- multi-vector-field search with
//! RRF fusion. Extracted from `vector_search_exec.rs` to keep file sizes
//! within the 500-line limit.
//!
//! Inside a transaction block each field's ranking merges the block's staged
//! writes before the fusion, as a single-field search does: a staged put
//! ranks by its staged vector, and a staged delete hides its row.

use nodedb_types::StorageKey;
use tracing::debug;

use super::hybrid_key::HybridFusionKey;
use super::vector_search::{
    VectorMultiSearchParams, build_search_hit, effective_ef, encode_hits_response,
};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec::VectorSearchHit;
use crate::engine::vector::collection::VectorCollection;
use crate::query::fusion::{RankedResult, reciprocal_rank_fusion};

impl CoreLoop {
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
            system_time,
        } = params;
        debug!(core = self.core_id, %collection, top_k, "vector multi-search");
        let cut = match super::search_cut::SearchCut::of(system_time) {
            Ok(cut) => cut,
            Err(e) => return self.response_error(task, e),
        };

        let database_id = task.request.database_id.as_u64();
        let db = nodedb_types::DatabaseId::new(database_id);
        let tenant_id = crate::types::TenantId::new(tid);
        let plain_key = CoreLoop::vector_index_key(database_id, tid, collection, "");
        // A named-field key looks like `"{collection}:{field_name}"` in the String part.
        let field_prefix = format!("{collection}:");

        // Over-fetch when RLS is active so the CP-side post-filter has
        // headroom to still return `top_k` after rejecting candidates.
        let fetch_k = super::vector_search_window::first_window(top_k, !rls_filters.is_empty());

        // Each searched field: its name, its collection, and its hits.
        let mut fields: Vec<FieldHits<'_>> = Vec::new();
        // The width of a field index the query could not be compared with.
        // Fields of other widths are skipped; when no field has the query's
        // width, the query is the caller's data error.
        let mut other_width: Option<usize> = None;
        let mut any_field_of_width = false;

        for (key, coll) in &self.vector_collections {
            if key.0 != db || key.1 != tenant_id {
                continue;
            }
            let field = if key == &plain_key {
                ""
            } else if let Some(field) = key.2.strip_prefix(&field_prefix) {
                field
            } else {
                continue;
            };
            if coll.dim() != query_vector.len() {
                other_width = Some(coll.dim());
                continue;
            }
            any_field_of_width = true;
            let searched = !(coll.is_empty() && cut.at.is_none());
            let results = if !searched {
                Vec::new()
            } else {
                let ef = effective_ef(ef_search, fetch_k);
                match super::vector_search::search_vector_leg(
                    coll,
                    query_vector,
                    fetch_k,
                    ef,
                    filter_bitmap,
                    cut.at,
                ) {
                    Ok(results) => results,
                    Err(code) => return self.response_error(task, code),
                }
            };
            fields.push(FieldHits {
                field: field.to_string(),
                collection: Some(coll),
                searched,
                hits: results
                    .iter()
                    .take(fetch_k)
                    .map(|r| build_search_hit(Some(coll), r.id, r.distance))
                    .collect(),
            });
        }

        // A historical read sees committed rows only, never staged ones.
        if let Some(txn_id) = task.request.txn_id.filter(|_| cut.merge_overlay)
            && let Err(e) = self.merge_multi_search_overlay(
                MultiOverlay {
                    txn_id,
                    database_id,
                    tid,
                    collection,
                    query_vector,
                    fetch_k,
                    filter_bitmap,
                },
                &mut fields,
            )
        {
            return self.response_error(task, e);
        }
        // A field whose index ranked nothing, with no staged hit either,
        // takes no part in the fusion.
        fields.retain(|f| f.searched || !f.hits.is_empty());

        if fields.is_empty() {
            if !any_field_of_width && let Some(width) = other_width {
                return self.response_error(
                    task,
                    super::vector::dimension_mismatch(width, query_vector.len()),
                );
            }
            return self.response_error(task, ErrorCode::NotFound);
        }

        let attach = !rls_filters.is_empty();
        let hits: crate::Result<Vec<VectorSearchHit>> = if fields.len() == 1 {
            // Single field: its own ranking.
            fields
                .into_iter()
                .flat_map(|f| f.hits)
                .map(|hit| self.attach_body(database_id, tid, collection, attach, cut.at, hit))
                .collect()
        } else {
            // RRF across fields, fused on each row's surrogate. Every field's
            // collection numbers its nodes on its own, so a local id names a
            // row only within the field that ranked it.
            let ranked_lists: Vec<Vec<RankedResult<FieldFusionKey>>> = fields
                .iter()
                .enumerate()
                .map(|(field, f)| {
                    f.hits
                        .iter()
                        .enumerate()
                        .map(|(rank, hit)| RankedResult {
                            document_id: FieldFusionKey::of(hit.id, field),
                            rank,
                            score: hit.distance,
                            source: "vector",
                        })
                        .collect()
                })
                .collect();
            reciprocal_rank_fusion(&ranked_lists, None, top_k)
                .into_iter()
                .map(|f| {
                    let hit = VectorSearchHit {
                        id: f.document_id.hit_key(),
                        distance: f.rrf_score as f32,
                        doc_id: None,
                        body: None,
                    };
                    self.attach_body(database_id, tid, collection, attach, cut.at, hit)
                })
                .collect()
        };
        let hits = match hits {
            Ok(hits) => hits,
            Err(e) => return self.response_error(task, e),
        };
        if let Some(ref m) = self.metrics {
            m.record_vector_search(0);
            m.record_query_by_engine("vector");
        }
        encode_hits_response(self, task, &hits)
    }
}

/// One searched field of a multi-field search: its name, its index (`None`
/// for a field only staged rows carry), and its hits, nearest first.
struct FieldHits<'a> {
    field: String,
    collection: Option<&'a VectorCollection>,
    /// Whether the field's index was searched: it holds a live node, or the
    /// search reads at a cut.
    searched: bool,
    hits: Vec<VectorSearchHit>,
}

/// Inputs of [`CoreLoop::merge_multi_search_overlay`].
struct MultiOverlay<'a> {
    txn_id: crate::types::TxnId,
    database_id: u64,
    tid: u64,
    collection: &'a str,
    query_vector: &'a [f32],
    fetch_k: usize,
    filter_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
}

impl CoreLoop {
    /// Merge the transaction's staged writes into every field's hits, the
    /// way a single-field search merges them. A vector field the collection
    /// declares with no index yet joins with the staged rows alone. Each
    /// field ranks staged vectors under its own index metric.
    fn merge_multi_search_overlay<'a>(
        &'a self,
        params: MultiOverlay<'_>,
        fields: &mut Vec<FieldHits<'a>>,
    ) -> crate::Result<()> {
        let MultiOverlay {
            txn_id,
            database_id,
            tid,
            collection,
            query_vector,
            fetch_k,
            filter_bitmap,
        } = params;
        for field in self.document_vector_field_names(database_id, tid, collection) {
            if !fields.iter().any(|f| f.field == field) {
                fields.push(FieldHits {
                    field,
                    collection: None,
                    searched: false,
                    hits: Vec::new(),
                });
            }
        }
        for f in fields.iter_mut() {
            let metric = match f.collection {
                Some(coll) => coll.params().metric,
                None => {
                    let config_key =
                        self.vector_field_config_key(database_id, tid, collection, &f.field);
                    super::vector_settle::vector_index_config_for(
                        &self.index_configs,
                        &self.vector_params,
                        &config_key,
                    )
                    .hnsw
                    .metric
                }
            };
            self.merge_vector_overlay_into_search(
                super::transaction::overlay::VectorMergeParams {
                    txn_id,
                    database_id: crate::types::DatabaseId::new(database_id),
                    tid: crate::types::TenantId::new(tid),
                    collection,
                    field_name: &f.field,
                    query_vector,
                    metric,
                    top_k: fetch_k,
                    filter_bitmap,
                    payload_filters: &[],
                },
                &mut f.hits,
            )?;
        }
        Ok(())
    }
}

/// The key a multi-field search fuses on. A bound hit fuses under its row's
/// storage key, so the same row ranked by two fields fuses into one result.
/// A headless hit has no row identity and fuses with nothing: its key names
/// the field that ranked it and its local id there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum FieldFusionKey {
    Bound(StorageKey),
    Headless { field: usize, local_id: u32 },
}

impl FieldFusionKey {
    fn of(hit: HybridFusionKey, field: usize) -> Self {
        match hit {
            HybridFusionKey::Bound(key) => Self::Bound(key),
            HybridFusionKey::Headless(local_id) => Self::Headless { field, local_id },
        }
    }

    /// The hit id the response carries.
    fn hit_key(self) -> HybridFusionKey {
        match self {
            Self::Bound(key) => HybridFusionKey::Bound(key),
            Self::Headless { local_id, .. } => HybridFusionKey::Headless(local_id),
        }
    }
}
