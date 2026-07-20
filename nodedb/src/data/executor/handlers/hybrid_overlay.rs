// SPDX-License-Identifier: BUSL-1.1

//! Shared read-your-own-writes overlay splice for the two hybrid search
//! handlers (`text_search_hybrid.rs` = vector + text, `text_search_triple.rs`
//! = vector + text + graph).
//!
//! The vector and text legs of a hybrid search read only committed state. Inside
//! a transaction the same query must ALSO observe the transaction's own staged
//! document writes (read-your-own-writes), exactly as the single-source
//! vector-only and FTS-only handlers already do. This module folds those staged
//! writes into the vector and text legs by REUSING the single-source overlay
//! merges — [`CoreLoop::merge_vector_overlay_into_search`] and
//! [`CoreLoop::merge_fts_overlay_into_results`] — rather than duplicating the
//! staged-document scoring. It then rebuilds the RRF-ready ranked lists from the
//! merged legs so a same-transaction INSERT / UPDATE / DELETE is reflected in
//! the fused result.
//!
//! A vector or FTS posting is an inline side effect of the document write, not a
//! stageable write of its own, so both merges re-read the transaction's staged
//! DOCUMENT BODIES and re-score them in memory: the vector leg extracts the
//! declared vector field and re-computes distance under the index's metric; the
//! text leg re-tokenizes and BM25-scores against the base corpus stats. Staged
//! tombstones remove the stale committed entry and staged puts over an existing
//! surrogate replace it, mirroring the single-source paths.
//!
//! The graph leg's RYOW is a separate concern and is deliberately not touched
//! here — the triple handler still reads committed graph state.

use nodedb_fts::posting::TextSearchResult;
use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{FtsMergeParams, VectorMergeParams};
use crate::engine::document::store::surrogate_to_doc_id;
use crate::engine::vector::DistanceMetric;
use crate::engine::vector::SearchResult;
use crate::engine::vector::collection::VectorCollection;
use crate::query::fusion::RankedResult;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Scope for one hybrid overlay splice: the active transaction, its
/// `(database, tenant, collection)` target, the vector and text query inputs,
/// the per-leg over-fetch bound, and any surrogate prefilter applied to the
/// vector leg. Bundled to keep the entry point to a single parameter.
pub(in crate::data::executor) struct HybridOverlayParams<'a> {
    pub txn_id: TxnId,
    pub database_id: DatabaseId,
    pub tid: TenantId,
    pub collection: &'a str,
    pub query_vector: &'a [f32],
    pub query_text: &'a str,
    pub fetch_k: usize,
    pub filter_bitmap: Option<&'a SurrogateBitmap>,
}

impl CoreLoop {
    /// Build the vector and text RRF-ranked lists for a hybrid search, folding
    /// the transaction's staged document writes into both legs.
    ///
    /// The base committed legs are `vector_results` (raw HNSW/IVF hits, whose
    /// local ids are resolved to surrogates via `vector_collection`) and
    /// `text_results` (base BM25 hits). This reuses the exact single-source
    /// overlay merges to re-score the staged puts and drop the staged
    /// tombstones, then emits `(vector_ranked, text_ranked)` keyed by
    /// surrogate-hex doc-id — the shared RRF key space the caller fuses on.
    pub(in crate::data::executor) fn hybrid_ranked_with_overlay(
        &self,
        params: HybridOverlayParams<'_>,
        vector_results: &[SearchResult],
        vector_collection: Option<&VectorCollection>,
        text_results: &[TextSearchResult],
    ) -> (Vec<RankedResult>, Vec<RankedResult>) {
        // Base committed legs, in the shape each single-source overlay merge
        // consumes: vector hits carry the surrogate-resolved id, text scores
        // carry the FTS surrogate + score + fuzzy flag.
        let mut vector_hits: Vec<_> = vector_results
            .iter()
            .map(|r| {
                let mut hit =
                    super::vector_search::build_search_hit(vector_collection, r.id, r.distance);
                // Pin the committed-path fusion doc_id now (surrogate hex, or
                // the `__local_{id}` sentinel for a headless row) so a headless
                // base hit's raw local id is never later misread as a global
                // surrogate. Staged hits added by the merge carry `doc_id:
                // None` and fall back to their (real) surrogate when the ranked
                // list is built below — matching the autocommit branch exactly.
                hit.doc_id = Some(super::vector_search::vector_leg_doc_id(
                    vector_collection,
                    r.id,
                ));
                hit
            })
            .collect();
        let mut text_scored: Vec<(Surrogate, f32, bool)> = text_results
            .iter()
            .map(|r| (r.doc_id, r.score, r.fuzzy))
            .collect();

        let db = params.database_id.as_u64();
        let tid_u64 = params.tid.as_u64();

        // Vector leg RYOW: re-score staged docs for each declared vector field
        // via the vector-only overlay merge. The merge skips any staged body
        // that lacks the field or whose dimensionality differs from the query
        // vector, so declaring several fields is safe. Metric comes from the
        // field's committed index (or its DDL params), falling back to L2 when
        // neither is registered yet.
        let mut fields: Vec<String> = self
            .strict_vector_fields(db, tid_u64, params.collection)
            .into_iter()
            .map(|(field, _dim)| field)
            .collect();
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
            );
        }

        // Text leg RYOW: re-score staged docs via the FTS-only overlay merge.
        self.merge_fts_overlay_into_results(
            FtsMergeParams {
                txn_id: params.txn_id,
                database_id: params.database_id,
                tid: params.tid,
                collection: params.collection,
                query: params.query_text,
                top_k: params.fetch_k,
            },
            &mut text_scored,
        );

        // Rebuild the RRF-ready ranked lists from the merged legs. Both keys are
        // surrogate-hex doc-ids so the vector and text legs fuse on one key
        // space (matching the committed-only construction in the handlers).
        let vector_ranked = vector_hits
            .iter()
            .enumerate()
            .map(|(rank, hit)| RankedResult {
                // Base hits carry their committed-path doc_id (surrogate hex or
                // `__local_` sentinel); staged hits added by the merge have
                // `doc_id: None` and resolve to their real surrogate.
                document_id: hit
                    .doc_id
                    .clone()
                    .unwrap_or_else(|| surrogate_to_doc_id(Surrogate::new(hit.id))),
                rank,
                score: hit.distance,
                source: "vector",
            })
            .collect();
        let text_ranked = text_scored
            .iter()
            .enumerate()
            .map(|(rank, (surrogate, score, _fuzzy))| RankedResult {
                document_id: surrogate_to_doc_id(*surrogate),
                rank,
                score: *score,
                source: "text",
            })
            .collect();
        (vector_ranked, text_ranked)
    }
}
