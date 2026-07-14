// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for predicate `DELETE ... WHERE <predicate>`
//! (`DocumentOp::BulkDelete`) inside a transaction. A `RETURNING` clause does
//! not change what is staged — the matched rows are tombstoned identically;
//! the clause only governs the client response shape.
//!
//! Mirrors point `DELETE` staging in `dispatch.rs` (`stage_point_delete`):
//! each row in the BASE ∪ OVERLAY matching set (resolved via
//! [`CoreLoop::merge_overlay_into_scan`]) is recorded as a staged tombstone.
//! Nothing is written durably here — cascades (inverted index, secondary
//! indexes, graph edges) run only at COMMIT replay through the real apply
//! path, exactly as a staged point delete defers its cascade today.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Routing identity + payload for one staged `BulkDelete`, bundled to keep
/// the entry point within the argument-count budget.
pub(in crate::data::executor) struct StageBulkDeleteParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
}

impl CoreLoop {
    /// Stage a predicate `DELETE` at statement time: resolve the current
    /// BASE ∪ OVERLAY matching set and tombstone each match. Returns
    /// `{"affected": N}` in the same shape `execute_bulk_delete` returns for
    /// the autocommit path.
    pub(in crate::data::executor) fn stage_bulk_delete(
        &mut self,
        params: StageBulkDeleteParams<'_>,
    ) -> Response {
        let StageBulkDeleteParams {
            task,
            tid,
            txn_id,
            collection,
            filter_bytes,
        } = params;
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());

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
        // current body so the overlay merge can re-check staged puts.
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

        {
            let matches = self.strict_aware_matcher(tid, collection, &filters);
            self.merge_overlay_into_scan(txn_id, &coll_key, &mut rows, &matches);
        }

        let mut affected = 0u64;
        for (row_key, _body) in &rows {
            let Ok(surrogate) = u32::from_str_radix(row_key, 16) else {
                continue;
            };
            self.txn_overlay_mut(txn_id)
                .insert_tombstone(coll_key.clone(), surrogate, row_key);
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
}
