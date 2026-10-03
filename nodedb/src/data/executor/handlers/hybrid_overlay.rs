// SPDX-License-Identifier: BUSL-1.1

//! Shared read-your-own-writes support for the two hybrid search handlers
//! (`text_search_hybrid.rs` = vector + text, `text_search_triple.rs` =
//! vector + text + graph).
//!
//! The text leg ranks the transaction's staged rows in the BM25 search
//! itself, before its top-k cut, through the same staged view the
//! single-source text search reads ([`CoreLoop::hybrid_text_leg`]). The
//! vector leg reads committed state, so the transaction's staged writes are
//! folded into it by the single-source vector overlay merge
//! ([`CoreLoop::merge_vector_overlay_into_search`]): staged tombstones drop
//! the stale committed entry and staged puts over an existing surrogate
//! replace it.
//!
//! The graph leg's RYOW is a separate concern and is deliberately not touched
//! here — the triple handler still reads committed graph state.

use nodedb_fts::posting::TextSearchResult;
use nodedb_fts::{FtsSearchParams, IndexScope};
use nodedb_types::SurrogateBitmap;

use super::hybrid_key::HybridFusionKey;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::VectorMergeParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::vector::DistanceMetric;
use crate::engine::vector::SearchResult;
use crate::engine::vector::collection::VectorCollection;
use crate::query::fusion::RankedResult;
use crate::types::{DatabaseId, TenantId, TxnId};

/// The two RRF-ready legs of a hybrid search, keyed on one fusion key space.
pub(in crate::data::executor) type HybridRankedLegs = (
    Vec<RankedResult<HybridFusionKey>>,
    Vec<RankedResult<HybridFusionKey>>,
);

/// Scope for one hybrid overlay splice: the active transaction, its
/// `(database, tenant, collection)` target, the vector query input, the
/// vector leg's over-fetch bound, and any surrogate prefilter applied to it.
/// Bundled to keep the entry point to a single parameter.
pub(in crate::data::executor) struct HybridOverlayParams<'a> {
    pub txn_id: TxnId,
    pub database_id: DatabaseId,
    pub tid: TenantId,
    pub collection: &'a str,
    /// Vector column of the vector leg. Empty re-scores every declared
    /// vector field.
    pub vector_field: &'a str,
    pub query_vector: &'a [f32],
    pub fetch_k: usize,
    /// Rows both legs may return: the plan's prefilter, the residual
    /// filters, and RLS, combined.
    pub filter_bitmap: Option<&'a SurrogateBitmap>,
}

impl CoreLoop {
    /// The text leg of a hybrid search: BM25 over `index`, with the issuing
    /// transaction's staged rows ranked in before the cut. Empty when the
    /// collection holds no text.
    pub(in crate::data::executor) fn hybrid_text_leg(
        &self,
        task: &ExecutionTask,
        tid: u64,
        index: Option<IndexScope<'_>>,
        params: FtsSearchParams<'_>,
    ) -> crate::Result<Vec<TextSearchResult>> {
        let Some(index) = index else {
            return Ok(Vec::new());
        };
        let staged = self.text_staged_view(task, tid, index)?;
        self.inverted.search_staged(
            task.request.database_id.as_u64(),
            TenantId::new(tid),
            index,
            params,
            staged.as_ref(),
        )
    }

    /// Build the vector and text RRF-ranked lists for a hybrid search, folding
    /// the transaction's staged document writes into the vector leg.
    ///
    /// `vector_results` are the raw HNSW/IVF hits, whose local ids are
    /// resolved to surrogates via `vector_collection`. `text_results` already
    /// rank the staged rows. Emits `(vector_ranked, text_ranked)` keyed by
    /// [`HybridFusionKey`], the shared RRF key space the caller fuses on.
    pub(in crate::data::executor) fn hybrid_ranked_with_overlay(
        &self,
        params: HybridOverlayParams<'_>,
        vector_results: &[SearchResult],
        vector_collection: Option<&VectorCollection>,
        text_results: &[TextSearchResult],
    ) -> crate::Result<HybridRankedLegs> {
        let mut vector_hits: Vec<_> = vector_results
            .iter()
            .map(|r| super::vector_search::build_search_hit(vector_collection, r.id, r.distance))
            .collect();

        let db = params.database_id.as_u64();
        let tid_u64 = params.tid.as_u64();

        // Vector leg RYOW: re-score staged docs for each declared vector field
        // via the vector-only overlay merge. The merge skips any staged body
        // that lacks the field or whose dimensionality differs from the query
        // vector, so declaring several fields is safe. Metric comes from the
        // field's committed index (or its DDL params), falling back to L2 when
        // neither is registered yet.
        // A named vector column re-scores that field only.
        let mut fields: Vec<String> = if params.vector_field.is_empty() {
            self.strict_vector_fields(db, tid_u64, params.collection)
                .into_iter()
                .map(|(field, _dim)| field)
                .collect()
        } else {
            vec![params.vector_field.to_string()]
        };
        if fields.is_empty() {
            fields = self.schemaless_vector_field_names(db, tid_u64, params.collection);
        }
        for field in &fields {
            let key = Self::vector_index_key(db, tid_u64, params.collection, field);
            let metric = self
                .vector_collections
                .get(&key)
                .map(|c| c.params().metric)
                .or_else(|| self.vector_params.get(&key).map(|p| p.metric))
                .unwrap_or(DistanceMetric::L2);
            self.merge_vector_overlay_into_search(
                VectorMergeParams {
                    txn_id: params.txn_id,
                    database_id: params.database_id,
                    tid: params.tid,
                    collection: params.collection,
                    field_name: field,
                    query_vector: params.query_vector,
                    metric,
                    top_k: params.fetch_k,
                    filter_bitmap: params.filter_bitmap,
                    payload_filters: &[],
                },
                &mut vector_hits,
            )?;
        }

        // Rebuild the RRF-ready ranked lists. Both legs key on
        // `HybridFusionKey`, matching the committed-only construction in the
        // handlers.
        let vector_ranked = vector_hits
            .iter()
            .enumerate()
            .map(|(rank, hit)| RankedResult {
                document_id: hit.id,
                rank,
                score: hit.distance,
                source: "vector",
            })
            .collect();
        let text_ranked = text_results
            .iter()
            .enumerate()
            .map(|(rank, r)| RankedResult {
                document_id: HybridFusionKey::for_surrogate(r.doc_id),
                rank,
                score: r.score,
                source: "text",
            })
            .collect();
        Ok((vector_ranked, text_ranked))
    }
}
