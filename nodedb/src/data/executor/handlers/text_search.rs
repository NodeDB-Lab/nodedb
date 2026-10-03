// SPDX-License-Identifier: BUSL-1.1

//! Text search handler and shared hydration helper for the Data Plane CoreLoop.

use tracing::debug;

use nodedb_fts::FtsSearchParams;
use nodedb_fts::posting::QueryMode;
use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::bridge::envelope::{ErrorCode, Response};

use nodedb_physical::physical_plan::TextScoreSpec;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::decode::decode_scanned_row;
use crate::data::executor::handlers::text_rows::{TextRowGate, combine_eligible, text_row_image};
use crate::data::executor::handlers::text_score_columns::ScoreColumns;
use crate::data::executor::response_codec::DocumentRow;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Parameters for [`CoreLoop::execute_text_search`].
pub(in crate::data::executor) struct TextSearchParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    /// Field index the query reads. `None` reads the whole-document index.
    pub field: Option<&'a str>,
    pub query: &'a str,
    /// Hits returned. `usize::MAX` returns every match.
    pub top_k: usize,
    /// Boolean combination of the query terms.
    pub mode: nodedb_types::text_search::QueryMode,
    pub fuzzy: bool,
    pub prefilter: Option<&'a SurrogateBitmap>,
    /// Residual WHERE predicates (`Vec<ScanFilter>`), applied before ranking.
    pub filters: &'a [u8],
    /// RLS filters (`Vec<ScanFilter>`), applied before ranking.
    pub rls_filters: &'a [u8],
    pub scores: &'a [TextScoreSpec],
}

/// Parameters for the internal [`CoreLoop::hydrate_text_hits`] helper.
///
/// Shared by `text_search.rs`, `text_search_scan.rs`, and any future handler
/// that needs to resolve FTS surrogates back to document rows.
///
/// There is deliberately no encoding field: the body encoding is resolved
/// inside [`CoreLoop::hydrate_text_hits`] from `database_id` / `tid` /
/// `collection`, the same way every other sparse reader resolves it, so a
/// caller cannot hand this helper the wrong one (or omit it and get the
/// schemaless default for a strict or vector-primary collection).
pub(in crate::data::executor) struct HydrateTextHitsParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// The issuing transaction, when this read runs inside `BEGIN..COMMIT`.
    /// A matched surrogate's body is resolved from this transaction's
    /// staging overlay first (a doc inserted/updated in THIS transaction is
    /// not yet in base storage), falling back to base storage otherwise.
    pub txn_id: Option<TxnId>,
    /// Score columns injected into each hydrated row.
    pub scores: &'a ScoreColumns<'a>,
}

impl CoreLoop {
    /// Execute a full-text search using BM25 + optional fuzzy matching.
    pub(in crate::data::executor) fn execute_text_search(
        &self,
        task: &ExecutionTask,
        params: TextSearchParams<'_>,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid = params.tid,
            collection = %params.collection,
            field = ?params.field,
            query = %params.query,
            top_k = params.top_k,
            mode = params.mode.as_str(),
            fuzzy = params.fuzzy,
            "text search"
        );

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, params.tid, params.collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let rows = match self.text_search_rows(task, &params) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };

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

    /// The hydrated hits of a text search, in this order: resolve the field's
    /// index, restrict candidates to the rows the residual filters and RLS
    /// admit, rank with the transaction's staged rows folded in, hydrate, and
    /// inject the score columns.
    ///
    /// RLS needs no check after ranking: the eligible rows are read on this
    /// core in the same task as the ranked rows are hydrated, so no write can
    /// land between the two, and a row with no body is never eligible under a
    /// gate.
    fn text_search_rows(
        &self,
        task: &ExecutionTask,
        p: &TextSearchParams<'_>,
    ) -> crate::Result<Vec<DocumentRow>> {
        let tenant_id = TenantId::new(p.tid);
        let Some(index) = self.text_index(task, p.tid, p.collection, p.field)? else {
            return Ok(Vec::new());
        };
        let gate = TextRowGate::new(p.filters, p.rls_filters)?;
        let eligible = combine_eligible(
            p.prefilter,
            self.text_eligible_rows(task, p.tid, p.collection, &gate)?,
        );
        let staged = self.text_staged_view(task, p.tid, index)?;
        let hits = self.inverted.search_staged(
            task.request.database_id.as_u64(),
            tenant_id,
            index,
            FtsSearchParams {
                query: p.query,
                top_k: p.top_k,
                fuzzy_enabled: p.fuzzy,
                mode: QueryMode::from(p.mode),
                prefilter: eligible.as_ref(),
            },
            staged.as_ref(),
        )?;

        let scores =
            self.text_score_columns(task, p.tid, p.collection, p.scores, eligible.as_ref())?;
        self.hydrate_text_hits(
            hits.iter().map(|r| (r.doc_id, r.score, r.fuzzy)),
            HydrateTextHitsParams {
                database_id: task.request.database_id.as_u64(),
                tid: p.tid,
                collection: p.collection,
                txn_id: task.request.txn_id,
                scores: &scores,
            },
        )
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

    /// Resolve ranked hits to rows, in hit order, each carrying `score`,
    /// `fuzzy`, its `id`, and the score columns.
    pub(in crate::data::executor) fn hydrate_text_hits<I>(
        &self,
        hits: I,
        params: HydrateTextHitsParams<'_>,
    ) -> crate::Result<Vec<DocumentRow>>
    where
        I: IntoIterator<Item = (Surrogate, f32, bool)>,
    {
        let HydrateTextHitsParams {
            database_id,
            tid,
            collection,
            txn_id,
            scores,
        } = params;
        // Read-your-own-writes for the projection step: a matched surrogate
        // the transaction staged has no body in base storage yet, so its
        // columns come from the staged bytes.
        let coll_key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        // Resolved once, from the collection's registered kind — never from
        // the bytes. A vector-primary sidecar and a schemaless document body
        // are both valid MessagePack maps with the same header, so sniffing
        // necessarily mis-reads one of them.
        let format =
            self.sparse_body_format(DatabaseId::new(database_id), TenantId::new(tid), collection);
        let mut rows: Vec<DocumentRow> = Vec::new();
        let mut surrogates: Vec<Surrogate> = Vec::new();
        for (surrogate, score, fuzzy) in hits {
            let storage_key = nodedb_types::StorageKey::for_surrogate(surrogate);
            let bytes_opt = self.overlay_or_base_body(txn_id, &coll_key, &storage_key, || {
                self.text_base_body(database_id, tid, collection, &storage_key)
            })?;
            // A surrogate with no body was indexed for FTS without a document
            // write (FtsIndex frames synced from Lite). Its row is the
            // surrogate-derived key alone. Under a filter or RLS gate such a
            // row is never eligible, so it never reaches here.
            let mut value = match bytes_opt {
                Some(ref bytes) => {
                    // The image carries `id` from the storage key, the same
                    // image the document scan returns.
                    let image = text_row_image(&storage_key, bytes, format.as_format_ref());
                    decode_scanned_row(bytes, Some(image.as_slice()), format.as_format_ref())?
                }
                None => serde_json::Value::Object(serde_json::Map::new()),
            };
            if let serde_json::Value::Object(ref mut map) = value {
                map.insert(
                    "score".to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(f64::from(score))
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                map.insert("fuzzy".to_string(), serde_json::Value::Bool(fuzzy));
            }
            surrogates.push(surrogate);
            rows.push(DocumentRow {
                id: storage_key.to_string(),
                data: value,
            });
        }
        scores.inject(&surrogates, &mut rows)?;
        Ok(rows)
    }
}
