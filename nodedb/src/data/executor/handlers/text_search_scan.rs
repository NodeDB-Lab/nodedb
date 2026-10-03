// SPDX-License-Identifier: BUSL-1.1

//! Phrase search and BM25-score-scan handlers for the Data Plane CoreLoop.

use std::ops::ControlFlow;

use tracing::debug;

use nodedb_physical::physical_plan::{ScoreScanBound, TextScoreSpec};
use nodedb_types::{StorageKey, Surrogate};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::decode::decode_scanned_row;
use crate::data::executor::handlers::text_rows::{TextRowGate, combine_eligible, text_row_image};
use crate::data::executor::handlers::text_score_sink::ScoreScanSink;
use crate::data::executor::handlers::text_search::HydrateTextHitsParams;
use crate::data::executor::handlers::transaction::overlay::{Staged, staged_phrase_hits};
use crate::data::executor::response_codec::DocumentRow;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// Parameters for [`CoreLoop::execute_phrase_search`].
pub(in crate::data::executor) struct PhraseSearchParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    /// Field index the phrase reads. `None` reads the whole-document index.
    pub field: Option<&'a str>,
    pub terms: &'a [String],
    /// Hits returned. `usize::MAX` returns every match.
    pub top_k: usize,
    pub prefilter: Option<&'a nodedb_types::SurrogateBitmap>,
    /// Residual WHERE predicates (`Vec<ScanFilter>`), applied before ranking.
    pub filters: &'a [u8],
    /// RLS filters (`Vec<ScanFilter>`), applied before ranking.
    pub rls_filters: &'a [u8],
    pub scores: &'a [TextScoreSpec],
}

/// Parameters for [`CoreLoop::execute_bm25_score_scan`].
pub(in crate::data::executor) struct ScoreScanParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    /// Residual WHERE predicates (`Vec<ScanFilter>`).
    pub filters: &'a [u8],
    /// RLS filters (`Vec<ScanFilter>`). A row that fails one is not emitted.
    pub rls_filters: &'a [u8],
    pub scores: &'a [TextScoreSpec],
    /// The query's LIMIT, and the score order it keeps, pushed into the scan.
    pub bound: Option<&'a ScoreScanBound>,
}

impl CoreLoop {
    /// Execute an exact phrase search.
    ///
    /// Returns only documents where `terms` appear as a contiguous sequence.
    /// Scoring is positional: documents with the phrase nearer the start rank higher.
    pub(in crate::data::executor) fn execute_phrase_search(
        &self,
        task: &ExecutionTask,
        params: PhraseSearchParams<'_>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid = params.tid,
            collection = %params.collection,
            field = ?params.field,
            term_count = params.terms.len(),
            top_k = params.top_k,
            "phrase search"
        );

        let _scan_guard = match self.acquire_scan_guard(task, params.tid, params.collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let rows = match self.phrase_search_rows(task, &params) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };
        self.text_rows_response(task, rows)
    }

    /// Restrict candidates to the rows the residual filters and RLS admit,
    /// match the phrase over the index with the transaction's hidden rows
    /// excluded, add its staged matches, cut to `top_k`, and hydrate.
    fn phrase_search_rows(
        &self,
        task: &ExecutionTask,
        p: &PhraseSearchParams<'_>,
    ) -> crate::Result<Vec<DocumentRow>> {
        let tenant_id = TenantId::new(p.tid);
        let database_id = task.request.database_id.as_u64();
        let Some(index) = self.text_index(task, p.tid, p.collection, p.field)? else {
            return Ok(Vec::new());
        };
        let gate = TextRowGate::new(p.filters, p.rls_filters)?;
        let eligible = combine_eligible(
            p.prefilter,
            self.text_eligible_rows(task, p.tid, p.collection, &gate)?,
        );
        let staged = self.text_staged_view(task, p.tid, index)?;

        let mut hits: Vec<(Surrogate, f32, bool)> =
            if staged.as_ref().is_some_and(|view| view.hides_all()) {
                Vec::new()
            } else {
                self.inverted
                    .phrase_search(
                        database_id,
                        tenant_id,
                        index,
                        crate::engine::sparse::inverted::PhraseSearchParams {
                            terms: p.terms,
                            top_k: p.top_k,
                            prefilter: eligible.as_ref(),
                            exclude: staged.as_ref().map(|view| view.hidden()),
                        },
                    )?
                    .iter()
                    .map(|r| (r.doc_id, r.score, false))
                    .collect()
            };
        if let Some(view) = staged.as_ref() {
            let phrase =
                self.canonical_phrase_terms(database_id, tenant_id, p.collection, p.terms)?;
            hits.extend(staged_phrase_hits(view, &phrase, eligible.as_ref()));
            hits.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.0.cmp(&b.0))
            });
            hits.truncate(p.top_k);
        }

        let scores =
            self.text_score_columns(task, p.tid, p.collection, p.scores, eligible.as_ref())?;
        self.hydrate_text_hits(
            hits,
            HydrateTextHitsParams {
                database_id,
                tid: p.tid,
                collection: p.collection,
                txn_id: task.request.txn_id,
                scores: &scores,
            },
        )
    }

    /// Execute a score scan: every row the residual filters and RLS admit,
    /// each with its score columns. A row a column's index holds but its
    /// query does not match carries `0.0` there. A row the index does not
    /// hold carries `null`.
    pub(in crate::data::executor) fn execute_bm25_score_scan(
        &self,
        task: &ExecutionTask,
        params: ScoreScanParams<'_>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid = params.tid,
            collection = %params.collection,
            score_columns = params.scores.len(),
            bound = ?params.bound,
            "bm25 score scan"
        );

        let _scan_guard = match self.acquire_scan_guard(task, params.tid, params.collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let rows = match self.score_scan_rows(task, &params) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };
        self.text_rows_response(task, rows)
    }

    /// Stream every current row the filters and RLS admit into the sink,
    /// which scores rows in batches and keeps the bound. A row the issuing
    /// transaction staged is emitted from its staged body. A staged tombstone
    /// or TRUNCATE hides base rows.
    ///
    /// The admitted rows are found first when a gate applies: the score
    /// columns decide their AND-mode fallback over exactly those rows.
    fn score_scan_rows(
        &self,
        task: &ExecutionTask,
        p: &ScoreScanParams<'_>,
    ) -> crate::Result<Vec<DocumentRow>> {
        let database_id = task.request.database_id;
        let tenant = TenantId::new(p.tid);
        let gate = TextRowGate::new(p.filters, p.rls_filters)?;
        let eligible = self.text_eligible_rows(task, p.tid, p.collection, &gate)?;
        let columns =
            self.text_score_columns(task, p.tid, p.collection, p.scores, eligible.as_ref())?;
        let mut sink = ScoreScanSink::new(&columns, p.bound);
        if sink.is_full() {
            return sink.finish();
        }
        // The body encoding of this collection's sparse rows, resolved from
        // its registered kind. A vector-primary sidecar decoded as a document
        // body renders `[4,"alice"]`.
        let format = self.sparse_body_format(database_id, tenant, p.collection);
        let coll_key = (database_id, tenant, p.collection.to_string());
        let overlay = match task.request.txn_id {
            Some(txn_id) => {
                // Read-your-own-writes refreshes the lease (see the reaper).
                self.touch_overlay(txn_id);
                self.txn_overlays.get(&txn_id)
            }
            None => None,
        };

        let mut emit = |key: &StorageKey, bytes: &[u8]| -> crate::Result<ControlFlow<()>> {
            if eligible
                .as_ref()
                .is_some_and(|rows| !rows.contains(key.surrogate()))
            {
                return Ok(ControlFlow::Continue(()));
            }
            let image = text_row_image(key, bytes, format.as_format_ref());
            let value = decode_scanned_row(bytes, Some(image.as_slice()), format.as_format_ref())?;
            sink.push(
                key.surrogate(),
                DocumentRow {
                    id: key.to_string(),
                    data: value,
                },
            )
        };

        let mut stopped = false;
        if overlay.is_none_or(|o| o.base_visible(&coll_key)) {
            self.for_each_text_base_row(
                database_id.as_u64(),
                p.tid,
                p.collection,
                |key, bytes| {
                    // A row the transaction staged is emitted from its staged body.
                    if overlay.is_some_and(|o| o.get(&coll_key, key.surrogate().as_u32()).is_some())
                    {
                        return Ok(ControlFlow::Continue(()));
                    }
                    let flow = emit(key, bytes)?;
                    stopped = flow.is_break();
                    Ok(flow)
                },
            )?;
        }
        if let Some(overlay) = overlay
            && !stopped
        {
            for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
                if let Staged::Put(body) = staged
                    && emit(&StorageKey::for_surrogate(Surrogate::new(surrogate)), body)?
                        .is_break()
                {
                    break;
                }
            }
        }
        sink.finish()
    }

    /// Encode hydrated text rows as the response payload.
    fn text_rows_response(&self, task: &ExecutionTask, rows: Vec<DocumentRow>) -> Response {
        if let Some(ref m) = self.metrics {
            m.record_fts_search(0);
        }
        match super::super::response_codec::encode(&rows) {
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
