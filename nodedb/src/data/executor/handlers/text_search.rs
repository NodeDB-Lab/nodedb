// SPDX-License-Identifier: BUSL-1.1

//! Text search handler and shared hydration helper for the Data Plane CoreLoop.

use tracing::debug;

use nodedb_fts::FtsSearchParams;
use nodedb_fts::posting::QueryMode;

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::handlers::transaction::overlay::FtsMergeParams;
use crate::data::executor::response_codec::DocumentRow;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Parameters for [`CoreLoop::execute_text_search`].
pub(in crate::data::executor) struct TextSearchParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub query: &'a str,
    pub top_k: usize,
    pub fuzzy: bool,
    pub prefilter: Option<&'a nodedb_types::SurrogateBitmap>,
    pub rls_filters: &'a [u8],
}

/// Parameters for the internal [`CoreLoop::hydrate_text_hits`] helper.
///
/// Shared by `text_search.rs`, `text_search_scan.rs`, and any future handler
/// that needs to resolve FTS surrogates back to document rows.
pub(in crate::data::executor) struct HydrateTextHitsParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub top_k: usize,
    pub rls_filters: &'a [u8],
    pub strict_schema: Option<&'a nodedb_types::columnar::StrictSchema>,
    /// The issuing transaction, when this read runs inside `BEGIN..COMMIT`.
    /// A matched surrogate's body is resolved from this transaction's
    /// staging overlay first (a doc inserted/updated in THIS transaction is
    /// not yet in base storage), falling back to base storage otherwise.
    pub txn_id: Option<TxnId>,
}

impl CoreLoop {
    /// Execute a full-text search using BM25 + optional fuzzy matching.
    pub(in crate::data::executor) fn execute_text_search(
        &self,
        task: &ExecutionTask,
        params: TextSearchParams<'_>,
    ) -> Response {
        let TextSearchParams {
            tid,
            collection,
            query,
            top_k,
            fuzzy,
            prefilter,
            rls_filters,
        } = params;
        let tenant_id = TenantId::new(tid);
        debug!(core = self.core_id, tid, %collection, %query, top_k, fuzzy, "text search");

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        // Fetch extra candidates when RLS is active.
        let fetch_k = if rls_filters.is_empty() {
            top_k
        } else {
            top_k.saturating_mul(2).max(20)
        };

        let results = match self.inverted.search(
            task.request.database_id.as_u64(),
            tenant_id,
            collection,
            FtsSearchParams {
                query,
                top_k: fetch_k,
                fuzzy_enabled: fuzzy,
                mode: QueryMode::And,
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

        // Read-your-own-writes for FTS: fold this transaction's staged
        // document bodies into the base search result before hydration, so
        // a doc inserted/updated earlier in the same transaction appears
        // (and one deleted is excluded) before COMMIT.
        let mut merged: Vec<(nodedb_types::Surrogate, f32, bool)> = results
            .iter()
            .map(|r| (r.doc_id, r.score, r.fuzzy))
            .collect();
        if let Some(txn_id) = task.request.txn_id {
            self.merge_fts_overlay_into_results(
                FtsMergeParams {
                    txn_id,
                    database_id: task.request.database_id,
                    tid: tenant_id,
                    collection,
                    query,
                    top_k: fetch_k,
                },
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
                rls_filters,
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

    pub(in crate::data::executor) fn strict_schema_for(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> Option<nodedb_types::columnar::StrictSchema> {
        let key = (database_id, tenant_id, collection.to_string());
        self.doc_configs.get(&key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        })
    }

    pub(in crate::data::executor) fn hydrate_text_hits<I>(
        &self,
        hits: I,
        params: HydrateTextHitsParams<'_>,
    ) -> Vec<DocumentRow>
    where
        I: IntoIterator<Item = (nodedb_types::Surrogate, f32, bool)>,
    {
        let HydrateTextHitsParams {
            database_id,
            tid,
            collection,
            top_k,
            rls_filters,
            strict_schema,
            txn_id,
        } = params;
        // Read-your-own-writes for the projection step: a matched surrogate
        // added by the overlay merge (a doc inserted/updated in THIS
        // transaction) has no body in base storage yet, so its columns must
        // be projected from the staged `Put` bytes. Resolve every matched
        // surrogate's body from the transaction's overlay first, falling
        // back to base storage — mirroring the index-lookup fetch fix.
        let coll_key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        let mut rows: Vec<DocumentRow> = Vec::new();
        for (surrogate, score, fuzzy) in hits {
            if rows.len() >= top_k {
                break;
            }
            let hex_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
            let bytes_opt = match self.overlay_or_base_body(txn_id, &coll_key, &hex_key, || {
                self.sparse.get(database_id, tid, collection, &hex_key)
            }) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(
                        err = %e,
                        %hex_key,
                        %collection,
                        "sparse store error during text hit hydration; skipping row"
                    );
                    continue;
                }
            };
            // When the sparse store has no body for this surrogate the document
            // was indexed for FTS without a corresponding document write (e.g.
            // FtsIndex frames synced from Lite). Return a minimal row containing
            // only the surrogate-derived ID so callers that only project `id`
            // (the common case in sync interop tests and CDC pipelines) still
            // receive a result. RLS filters are skipped when there is no body.
            let mut value = if let Some(ref bytes) = bytes_opt {
                if !rls_filters.is_empty()
                    && !super::rls_eval::rls_check_msgpack_bytes(rls_filters, bytes)
                {
                    continue;
                }
                decode_scanned_document(bytes, strict_schema)
            } else {
                serde_json::Value::Object(serde_json::Map::new())
            };
            if let serde_json::Value::Object(ref mut map) = value {
                map.insert(
                    "score".to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(score as f64)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                map.insert("fuzzy".to_string(), serde_json::Value::Bool(fuzzy));
            }
            rows.push(DocumentRow {
                id: hex_key,
                data: value,
            });
        }
        rows
    }
}
