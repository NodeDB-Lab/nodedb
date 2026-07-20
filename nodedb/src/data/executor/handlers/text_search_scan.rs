// SPDX-License-Identifier: BUSL-1.1

//! Phrase search and BM25-score-scan handlers for the Data Plane CoreLoop.

use std::collections::HashMap;

use tracing::debug;

use nodedb_fts::FtsSearchParams;
use nodedb_fts::posting::QueryMode;

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::handlers::text_search::HydrateTextHitsParams;
use crate::data::executor::handlers::transaction::overlay::FtsMergeParams;
use crate::data::executor::response_codec::DocumentRow;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// Upper bound on hits fetched by `BM25ScoreScan` to populate per-row scores.
/// The downstream BMW scorer pre-allocates `Vec::with_capacity(top_k)`, so
/// `usize::MAX` would overflow on element-size multiplication. One million is
/// well above any realistic collection size for in-process score injection
/// while staying safely allocatable.
const BM25_SCAN_MAX_HITS: usize = 1_000_000;

impl CoreLoop {
    /// Execute an exact phrase search.
    ///
    /// Returns only documents where `terms` appear as a contiguous sequence.
    /// Scoring is positional: documents with the phrase nearer the start rank higher.
    pub(in crate::data::executor) fn execute_phrase_search(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        terms: &[String],
        top_k: usize,
        prefilter: Option<&nodedb_types::SurrogateBitmap>,
    ) -> Response {
        let tenant_id = TenantId::new(tid);
        debug!(core = self.core_id, tid, %collection, term_count = terms.len(), top_k, "phrase search");

        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let results = match self.inverted.phrase_search(
            task.request.database_id.as_u64(),
            tenant_id,
            collection,
            crate::engine::sparse::inverted::PhraseSearchParams {
                terms,
                top_k,
                prefilter,
            },
        ) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Read-your-own-writes for FTS phrase search: fold staged document
        // bodies into the base result with FAITHFUL phrase semantics. The
        // staged doc's own analyzed token positions are self-contained, so
        // the merge verifies the phrase's terms occur as a contiguous,
        // in-order run (zero slop — the same adjacency the durable phrase
        // search enforces on stored postings), NOT mere term presence.
        let mut merged: Vec<(nodedb_types::Surrogate, f32, bool)> =
            results.iter().map(|r| (r.doc_id, r.score, false)).collect();
        if let Some(txn_id) = task.request.txn_id {
            self.merge_fts_phrase_overlay_into_results(
                FtsMergeParams {
                    txn_id,
                    database_id: task.request.database_id,
                    tid: tenant_id,
                    collection,
                    query: "",
                    top_k,
                },
                terms,
                &mut merged,
            );
        }

        let strict_schema = self.strict_schema_for(task.request.database_id, tenant_id, collection);
        let rows = self.hydrate_text_hits(
            merged,
            HydrateTextHitsParams {
                database_id: task.request.database_id.as_u64(),
                tid,
                collection,
                top_k,
                rls_filters: &[],
                strict_schema: strict_schema.as_ref(),
                txn_id: task.request.txn_id,
            },
        );
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

    /// Execute a full-collection scan with BM25 score injected per row.
    ///
    /// Runs an FTS search to build a surrogate → score map, then scans every
    /// document in the collection. Each document is returned with `score_alias`
    /// injected as an additional field. Documents whose surrogate does not appear
    /// in the score map receive `null` for the score column.
    pub(in crate::data::executor) fn execute_bm25_score_scan(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        query: &str,
        score_alias: &str,
        fuzzy: bool,
    ) -> Response {
        let tenant_id = TenantId::new(tid);
        debug!(core = self.core_id, tid, %collection, %query, %score_alias, "bm25 score scan");

        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        // Build a surrogate → score map from FTS hits. Bounded top_k: heap
        // allocation in BMW search is `Vec::with_capacity(top_k)`, so a literal
        // `usize::MAX` overflows on `top_k * size_of::<Element>()`.
        let mut score_map: HashMap<nodedb_types::Surrogate, f32> = match self.inverted.search(
            task.request.database_id.as_u64(),
            tenant_id,
            collection,
            FtsSearchParams {
                query,
                top_k: BM25_SCAN_MAX_HITS,
                fuzzy_enabled: fuzzy,
                mode: QueryMode::And,
                prefilter: None,
            },
        ) {
            Ok(hits) => hits.into_iter().map(|h| (h.doc_id, h.score)).collect(),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Read-your-own-writes for FTS: fold staged document bodies into
        // the score map (staged put re-scored/added, staged tombstone
        // removed) before the collection scan renders rows below.
        if let Some(txn_id) = task.request.txn_id {
            self.merge_fts_overlay_into_score_map(
                FtsMergeParams {
                    txn_id,
                    database_id: task.request.database_id,
                    tid: tenant_id,
                    collection,
                    query,
                    top_k: BM25_SCAN_MAX_HITS,
                },
                &mut score_map,
            );
        }

        // Retrieve the strict schema (if any) so binary-tuple rows decode correctly.
        let config_key = (task.request.database_id, tenant_id, collection.to_string());
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Scan all documents and inject the score field.
        let scan_result = self.sparse.scan_documents(
            task.request.database_id.as_u64(),
            tid,
            collection,
            BM25_SCAN_MAX_HITS,
        );
        let mut docs = match scan_result {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Read-your-own-writes: fold staged document bodies into the row
        // list, gating staged-row membership on the FTS match. `score_map`
        // above was already narrowed to matching surrogates (staged puts
        // that match inserted, non-matches / tombstones removed), so a
        // staged doc appears as a row ONLY when it is in `score_map` —
        // `text_match(...)` as a predicate must not surface a staged doc
        // that does not contain the query term. A staged tombstone or a
        // staged update that dropped the term removes its base row too.
        if let Some(txn_id) = task.request.txn_id {
            self.merge_fts_rows_from_score_map(
                FtsMergeParams {
                    txn_id,
                    database_id: task.request.database_id,
                    tid: tenant_id,
                    collection,
                    query,
                    top_k: BM25_SCAN_MAX_HITS,
                },
                &mut docs,
                &score_map,
            );
        }

        let mut rows: Vec<DocumentRow> = Vec::with_capacity(docs.len());
        for (hex_key, bytes) in &docs {
            let mut value = decode_scanned_document(bytes, strict_schema.as_ref());
            // Inject score into the document object.
            if let serde_json::Value::Object(ref mut map) = value {
                let score = crate::engine::document::store::doc_id_to_surrogate(hex_key)
                    .and_then(|s| score_map.get(&s).copied());
                match score {
                    Some(s) => {
                        map.insert(
                            score_alias.to_string(),
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(s as f64)
                                    .unwrap_or_else(|| serde_json::Number::from(0)),
                            ),
                        );
                    }
                    None => {
                        map.insert(score_alias.to_string(), serde_json::Value::Null);
                    }
                }
            }
            rows.push(DocumentRow {
                id: hex_key.clone(),
                data: value,
            });
        }

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
