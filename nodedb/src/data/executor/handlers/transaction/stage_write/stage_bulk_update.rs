// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for predicate `UPDATE ... WHERE <predicate>`
//! (`DocumentOp::BulkUpdate`) inside a transaction. A `RETURNING` clause does
//! not change what is staged — the matched rows' post-images are recorded
//! identically; the clause only governs the client response shape.
//!
//! Mirrors the point-write staging in `dispatch.rs`: the matched rows are
//! evaluated against BASE ∪ OVERLAY (via [`CoreLoop::merge_overlay_into_scan`])
//! so a same-transaction earlier write to a row is observed, each matched
//! row's update is applied with the SAME per-row primitive point `UPDATE`
//! uses ([`CoreLoop::stage_apply_update`]), and the result is staged into the
//! overlay as a `Put` — never written durably here. COMMIT's buffered plan
//! replay remains the sole durable apply.

use nodedb_physical::physical_plan::UpdateValue;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::generated;
use crate::data::executor::handlers::transaction::overlay::MAX_TXN_OVERLAY_BYTES;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Routing identity + payload for one staged `BulkUpdate`, bundled to keep
/// the entry point within the argument-count budget.
pub(in crate::data::executor) struct StageBulkUpdateParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
    pub updates: &'a [(String, UpdateValue)],
}

impl CoreLoop {
    /// Stage a predicate `UPDATE` at statement time: resolve the current
    /// BASE ∪ OVERLAY matching set, apply the SET-list to each match, and
    /// record the new body as a staged `Put`. Returns `{"affected": N}` in
    /// the same shape `execute_bulk_update` returns for the autocommit path.
    pub(in crate::data::executor) fn stage_bulk_update(
        &mut self,
        params: StageBulkUpdateParams<'_>,
    ) -> Response {
        let StageBulkUpdateParams {
            task,
            tid,
            txn_id,
            collection,
            filter_bytes,
            updates,
        } = params;
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());

        // Reject direct updates to generated columns, matching the durable
        // and point-write staging paths.
        let config_key = (TenantId::new(tid), collection.to_string());
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) =
                generated::check_generated_readonly(updates, &config.enforcement.generated_columns)
        {
            return self.response_error(task, e);
        }

        let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize filters: {e}"),
                        },
                    );
                }
            }
        };

        // BASE matching set: the same scan-and-filter primitive the
        // autocommit bulk handler uses, then fetch each matched row's
        // current body (also the same lookup the autocommit path uses).
        let mut rows = match self.stage_bulk_base_rows(
            task,
            database_id.as_u64(),
            tid,
            collection,
            &filters,
        ) {
            Ok(rows) => rows,
            Err(resp) => return resp,
        };

        // Fold the transaction's own staged writes into the base result:
        // drops tombstoned rows, re-checks staged puts against the predicate
        // (an earlier staged update may have moved a row in or out), and
        // appends overlay-only rows that now match.
        {
            let matches = self.strict_aware_matcher(tid, collection, &filters);
            self.merge_overlay_into_scan(txn_id, &coll_key, &mut rows, &matches);
        }

        let mut affected = 0u64;
        for (row_key, current_body) in &rows {
            let Ok(surrogate) = u32::from_str_radix(row_key, 16) else {
                continue;
            };
            let new_body = match self.stage_apply_update(tid, collection, current_body, updates) {
                Ok(b) => b,
                Err(e) => return self.response_error(task, e),
            };
            if let Err(e) =
                self.stage_bulk_put_capped(txn_id, &coll_key, surrogate, row_key, new_body)
            {
                return self.response_error(task, e);
            }
            affected += 1;
        }

        match response_codec::encode_json(&serde_json::json!({ "affected": affected })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Resolve the BASE matching set for a bulk predicate stage (shared by
    /// `stage_bulk_update` and `stage_bulk_delete`): the same
    /// scan-and-filter primitive the autocommit bulk handlers use
    /// ([`CoreLoop::scan_matching_documents`]), then each matched row's
    /// current body via the same per-row lookup the autocommit path uses.
    pub(super) fn stage_bulk_base_rows(
        &self,
        task: &ExecutionTask,
        database_id: u64,
        tid: u64,
        collection: &str,
        filters: &[ScanFilter],
    ) -> Result<Vec<(String, Vec<u8>)>, Response> {
        let matching_ids = self
            .scan_matching_documents(database_id, tid, collection, filters)
            .map_err(|e| {
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            })?;
        let mut rows: Vec<(String, Vec<u8>)> = Vec::with_capacity(matching_ids.len());
        for doc_id in matching_ids {
            if let Ok(Some(bytes)) = self.sparse.get(database_id, tid, collection, &doc_id) {
                rows.push((doc_id, bytes));
            }
        }
        Ok(rows)
    }

    /// Stage a bulk-matched row's new body after enforcing the same
    /// per-transaction overlay memory cap point writes enforce
    /// (`stage_put_capped` in `dispatch.rs`): reject with the identical
    /// [`crate::Error::TxnOverlayMemoryExceeded`] once the cumulative staged
    /// byte total would exceed [`MAX_TXN_OVERLAY_BYTES`]. Rows staged before
    /// the row that trips the cap remain staged — the same
    /// stage-then-fail-on-overflow behavior `stage_put_capped` exhibits for a
    /// single point write, applied per matched row here.
    pub(in crate::data::executor) fn stage_bulk_put_capped(
        &mut self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
        doc_id: &str,
        body: Vec<u8>,
    ) -> crate::Result<()> {
        let current = self
            .txn_overlays
            .get(&txn_id)
            .map(|o| o.memory_size_estimate())
            .unwrap_or(0);
        if current.saturating_add(body.len()) > MAX_TXN_OVERLAY_BYTES {
            return Err(crate::Error::TxnOverlayMemoryExceeded {
                limit: MAX_TXN_OVERLAY_BYTES,
            });
        }
        self.txn_overlay_mut(txn_id)
            .insert_put(coll_key.clone(), surrogate, doc_id, body);
        Ok(())
    }
}
