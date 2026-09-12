// SPDX-License-Identifier: BUSL-1.1

//! KV `DELETE` and `TRUNCATE` handlers.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    /// `rls_write_check` is the compiled RLS write policy. The row a delete
    /// removes is the image the policy decides, and a delete otherwise reads
    /// nothing at all — so a non-empty check is what makes the pre-image be
    /// read in the first place, and one rejected key fails the whole statement
    /// before any key is removed.
    pub(in crate::data::executor) fn execute_kv_delete(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        keys: &[Vec<u8>],
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> Response {
        debug!(core = self.core_id, %collection, count = keys.len(), "kv delete");
        let now_ms = current_ms();

        if !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            for key in keys {
                // An absent key removes no row, so there is no image to decide;
                // the delete simply counts it as not-deleted.
                let Some(body) = self.kv_engine.get(did, tid, collection, key, now_ms) else {
                    continue;
                };
                if let Err(e) =
                    super::super::rls::admit_kv_row(rls_write_check, &body, key, tid, collection)
                {
                    return self.response_error(task, e);
                }
            }
        }

        let count = self.kv_engine.delete(did, tid, collection, keys, now_ms);
        if let Some(ref m) = self.metrics {
            m.record_kv_delete();
        }

        // Emit delete events to Event Plane (one per deleted key).
        if count > 0 {
            for key in keys {
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Delete,
                    crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
                    None,
                    None,
                );
            }
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
