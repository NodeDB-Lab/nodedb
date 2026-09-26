// SPDX-License-Identifier: BUSL-1.1

//! KV `DELETE` and `TRUNCATE` handlers.

use tracing::debug;

use super::types::KvDeleteParams;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_rows::KvStoredRow;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    /// `rls_write_check` is the compiled RLS write policy. The row a delete
    /// removes is the image the policy decides, and one rejected key fails
    /// the whole statement before any key is removed. Every removed row's
    /// pre-image is read: the policy decides it, `RETURNING` projects it, and
    /// the delete event carries it as the OLD row.
    pub(in crate::data::executor) fn execute_kv_delete(
        &mut self,
        task: &ExecutionTask,
        params: KvDeleteParams<'_>,
    ) -> Response {
        let KvDeleteParams {
            did,
            tid,
            collection,
            keys,
            rls_write_check,
            returning,
            rls_filters,
        } = params;
        debug!(core = self.core_id, %collection, count = keys.len(), "kv delete");
        let now_ms = current_ms();

        let gated = !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        );
        // Pre-images of the keys that exist, in `keys` order: the rows the
        // write gate decides, the rows `RETURNING` projects, and the OLD rows
        // the delete events carry. An absent key removes no row, so it has no
        // image, counts as not-deleted, and emits no event. A key named twice
        // removes one row.
        let mut pre_images: Vec<(&[u8], Vec<u8>)> = Vec::with_capacity(keys.len());
        let mut seen: std::collections::HashSet<&[u8]> =
            std::collections::HashSet::with_capacity(keys.len());
        for key in keys {
            if !seen.insert(key.as_slice()) {
                continue;
            }
            let Some(body) = self.kv_engine.get(did, tid, collection, key, now_ms) else {
                continue;
            };
            if gated
                && let Err(e) =
                    super::super::rls::admit_kv_row(rls_write_check, &body, key, tid, collection)
            {
                return self.response_error(task, e);
            }
            pre_images.push((key.as_slice(), body));
        }

        let count = self.kv_engine.delete(did, tid, collection, keys, now_ms);
        if let Some(ref m) = self.metrics {
            m.record_kv_delete();
        }

        // One delete event per removed row, carrying the row it removed.
        for (key, body) in &pre_images {
            self.emit_kv_write_event(
                task,
                collection,
                crate::event::WriteOp::Delete,
                key,
                None,
                Some(body.as_slice()),
            );
        }

        if let Some(spec) = returning {
            // The pre-images ARE the removed rows: the core loop runs ops
            // serially, so nothing slips between the read and the delete. A
            // delete that matched no key ships an EMPTY row set, never a
            // count, so the RETURNING renderer decodes the shape it asked for.
            let rows: Vec<KvStoredRow<'_>> = pre_images
                .iter()
                .map(|(key, body)| (*key, body.as_slice()))
                .collect();
            return self.kv_stored_returning_response(task, spec, rls_filters, &rows);
        }

        match response_codec::encode_count("deleted", count) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_truncate(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv truncate");
        let count = self.kv_engine.truncate(did, tid, collection);
        match response_codec::encode_count("deleted", count) {
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
