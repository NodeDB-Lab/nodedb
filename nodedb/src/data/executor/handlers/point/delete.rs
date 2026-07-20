// SPDX-License-Identifier: BUSL-1.1

//! PointDelete: remove one document plus its cascading side-effects across
//! inverted, secondary, graph, and spatial indexes.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_types::Surrogate;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        document_id: &str,
        surrogate: Surrogate,
        returning: Option<&ReturningSpec>,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "point delete");

        let database_id = task.request.database_id.as_u64();

        // Doc-store write + all index cascades, via `apply_point_delete`.
        // The doc-store transaction is committed internally before any
        // cascade runs (cascades open their own write transactions).
        let outcome = match self.apply_point_delete(PointDeleteParams {
            database_id,
            tid,
            collection,
            document_id,
            surrogate,
            user_roles: &task.request.user_roles,
            enforce: true,
        }) {
            Ok(outcome) => outcome,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let prior = outcome.prior_value;

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Record the committed delete's version against its surrogate +
        // collection, but only when a row was actually removed — a delete that
        // matched nothing changes no state and creates no OCC conflict.
        if prior.is_some() {
            self.note_surrogate_write_lsn(task, tid, collection, surrogate.as_u32());

            // Record the removed secondary-index values into the per-index
            // write-value substrate (plain cascade ∪ bitemporal tombstones).
            if let Some(lsn) = task.wal_lsn() {
                let mut tuples = outcome.secondary_index_tuples;
                tuples.extend(outcome.bitemporal_index_tuples);
                self.note_index_write_values(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                    &tuples,
                    lsn,
                );
            }
        }

        // Emit delete event to Event Plane if the row actually existed.
        // `apply_point_delete` returns the prior bytes — we thread them
        // through so CDC/trigger consumers see the pre-delete state as
        // `old_value`. A delete against a non-existent key is a true
        // no-op and emits nothing.
        if let Some(prior_bytes) = prior.as_deref() {
            let old_converted = self.resolve_event_payload(
                task.request.database_id.as_u64(),
                tid,
                collection,
                prior_bytes,
            );
            self.emit_write_event(
                task,
                collection,
                crate::event::WriteOp::Delete,
                document_id,
                None,
                Some(old_converted.as_deref().unwrap_or(prior_bytes)),
            );
        }

        if let (Some(spec), Some(prior_bytes)) = (returning, prior.as_deref()) {
            // Decode the pre-deletion document and project per spec.
            let prior_with_id =
                nodedb_query::msgpack_scan::inject_str_field(prior_bytes, "id", document_id);
            let doc = match doc_format::decode_document(&prior_with_id) {
                Some(v) => v,
                None => serde_json::json!({"id": document_id}),
            };
            match returning_rows::build_rows_payload(spec, &[doc]) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("RETURNING encode: {e}"),
                    },
                ),
            }
        } else if let Some(spec) = returning {
            // Row did not exist — return empty rows payload.
            match returning_rows::build_rows_payload(spec, &[]) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("RETURNING encode: {e}"),
                    },
                ),
            }
        } else {
            self.response_ok(task)
        }
    }
}
