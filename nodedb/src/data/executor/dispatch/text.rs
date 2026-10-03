// SPDX-License-Identifier: BUSL-1.1

//! Text (FTS) operation dispatch.

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::TextOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::text_search::TextSearchParams;
use crate::data::executor::handlers::text_search_hybrid::HybridSearchParams;
use crate::data::executor::handlers::text_search_scan::{PhraseSearchParams, ScoreScanParams};
use crate::data::executor::handlers::text_search_triple::HybridSearchTripleParams;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_text(&mut self, task: &ExecutionTask, op: &TextOp) -> Response {
        let tid = task.request.tenant_id.as_u64();
        match op {
            TextOp::Search {
                collection,
                field,
                query,
                top_k,
                mode,
                fuzzy,
                prefilter,
                filters,
                rls_filters,
                scores,
            } => self.execute_text_search(
                task,
                TextSearchParams {
                    tid,
                    collection: collection.as_str(),
                    field: field.as_deref(),
                    query,
                    top_k: *top_k,
                    mode: *mode,
                    fuzzy: *fuzzy,
                    prefilter: prefilter.as_ref(),
                    filters,
                    rls_filters,
                    scores,
                },
            ),

            TextOp::BM25ScoreScan {
                collection,
                filters,
                rls_filters,
                scores,
                bound,
            } => self.execute_bm25_score_scan(
                task,
                ScoreScanParams {
                    tid,
                    collection: collection.as_str(),
                    filters,
                    rls_filters,
                    scores,
                    bound: bound.as_ref(),
                },
            ),

            TextOp::PhraseSearch {
                collection,
                field,
                terms,
                top_k,
                prefilter,
                filters,
                rls_filters,
                scores,
            } => self.execute_phrase_search(
                task,
                PhraseSearchParams {
                    tid,
                    collection: collection.as_str(),
                    field: field.as_deref(),
                    terms,
                    top_k: *top_k,
                    prefilter: prefilter.as_ref(),
                    filters,
                    rls_filters,
                    scores,
                },
            ),

            TextOp::HybridSearch {
                collection,
                vector_field,
                query_vector,
                text_field,
                query_text,
                filters,
                top_k,
                ef_search,
                mode,
                fuzzy,
                vector_weight,
                filter_bitmap,
                rls_filters,
                score_alias,
            } => self.execute_hybrid_search(
                task,
                HybridSearchParams {
                    tid,
                    collection: collection.as_str(),
                    vector_field,
                    query_vector,
                    text_field: text_field.as_deref(),
                    query_text,
                    filters,
                    top_k: *top_k,
                    ef_search: *ef_search,
                    mode: *mode,
                    fuzzy: *fuzzy,
                    vector_weight: *vector_weight,
                    filter_bitmap: filter_bitmap.as_ref(),
                    rls_filters,
                    score_alias: score_alias.as_deref(),
                },
            ),

            TextOp::FtsIndexDoc {
                collection,
                surrogate,
                fields,
                provenance,
            } => self.execute_fts_index_doc(
                task,
                tid,
                collection.as_str(),
                *surrogate,
                fields,
                provenance.as_ref(),
            ),

            TextOp::FtsDeleteDoc {
                collection,
                surrogate,
                provenance,
            } => self.execute_fts_delete_doc(
                task,
                tid,
                collection.as_str(),
                *surrogate,
                provenance.as_ref(),
            ),

            TextOp::HybridSearchTriple {
                collection,
                vector_field,
                query_vector,
                text_field,
                query_text,
                filters,
                graph_seed_id,
                graph_depth,
                graph_edge_label,
                top_k,
                ef_search,
                mode,
                fuzzy,
                rrf_k,
                filter_bitmap,
                rls_filters,
                score_alias,
            } => self.execute_hybrid_search_triple(
                task,
                HybridSearchTripleParams {
                    tid,
                    collection: collection.as_str(),
                    vector_field,
                    query_vector,
                    text_field: text_field.as_deref(),
                    query_text,
                    filters,
                    graph_seed_id,
                    graph_depth: *graph_depth,
                    graph_edge_label: graph_edge_label.as_deref(),
                    top_k: *top_k,
                    ef_search: *ef_search,
                    mode: *mode,
                    fuzzy: *fuzzy,
                    rrf_k: *rrf_k,
                    filter_bitmap: filter_bitmap.as_ref(),
                    rls_filters,
                    score_alias: score_alias.as_deref(),
                },
            ),

            TextOp::SetTextConfig {
                collection,
                analyzer_name,
                fuzzy_default,
            } => self.execute_set_text_config(
                task,
                tid,
                collection.as_str(),
                analyzer_name.as_deref(),
                *fuzzy_default,
            ),
        }
    }
}
