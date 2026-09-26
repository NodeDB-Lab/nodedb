// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... ON CONFLICT (key) DO UPDATE SET ...` read-modify-write handler.

use tracing::debug;

use super::types::KvInsertOnConflictUpdateParams;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// SQL `INSERT ... ON CONFLICT (key) DO UPDATE SET ...` semantics.
    /// Read-modify-write: if the key is absent, plain put; if present,
    /// merge through `merge_kv_conflict_body` (with `EXCLUDED` resolving
    /// to the would-be-inserted row) and write the result back in the
    /// stored body's shape.
    pub(in crate::data::executor) fn execute_kv_insert_on_conflict_update(
        &mut self,
        task: &ExecutionTask,
        params: KvInsertOnConflictUpdateParams<'_>,
    ) -> Response {
        let KvInsertOnConflictUpdateParams {
            did,
            tid,
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate,
            rls_write_check,
            returning,
            rls_filters,
        } = params;
        debug!(core = self.core_id, %collection, "kv insert-on-conflict-update");

        if self.kv_engine.is_over_budget() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "KV memory budget exceeded, retry later".into(),
                },
            );
        }

        // See `CoreLoop::kv_ttl_now_ms` for the precedence this resolves.
        let now_ms = self.kv_ttl_now_ms(task);
        let existing_bytes = self.kv_engine.get(did, tid, collection, key, now_ms);

        let stored_bytes: Vec<u8> = match &existing_bytes {
            None => value.to_vec(),
            Some(existing_raw) => {
                match super::super::conflict_merge::merge_kv_conflict_body(
                    existing_raw,
                    value,
                    updates,
                ) {
                    Ok(b) => b,
                    Err(e) => return self.response_error(task, e),
                }
            }
        };

        // `stored_bytes` is whichever body this op actually persists — the
        // incoming row when the key was absent, the merge when it was present.
        // Deciding the merge is the point: admitting only the incoming row
        // would clear a write whose real post-image the policy never saw.
        if let Err(e) =
            super::super::rls::admit_kv_row(rls_write_check, &stored_bytes, key, tid, collection)
        {
            return self.response_error(task, e);
        }

        self.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: did,
            tenant_id: tid,
            collection,
            key,
            value: &stored_bytes,
            ttl_ms,
            now_ms,
            surrogate,
        });
        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }

        // `ON CONFLICT DO UPDATE` onto an existing row is an Update from
        // every downstream consumer's perspective; a fresh key with no
        // prior value is an Insert. The pre-write `existing_bytes` probe
        // above is the source of truth.
        let (op, old_slice): (_, Option<&[u8]>) = match existing_bytes.as_deref() {
            Some(o) => (crate::event::WriteOp::Update, Some(o)),
            None => (crate::event::WriteOp::Insert, None),
        };
        self.emit_kv_write_event(task, collection, op, key, Some(&stored_bytes), old_slice);

        if let Some(spec) = returning {
            // The MERGED body, not the caller's: on a conflict the submitted
            // values are only part of what the row now holds, so echoing them
            // would report a row that does not exist. `stored_bytes` is the
            // exact body the put above persisted — the same bytes the write
            // gate was decided against.
            return self.kv_stored_returning_response(
                task,
                spec,
                rls_filters,
                &[(key, stored_bytes.as_slice())],
            );
        }
        let op_str = if existing_bytes.is_some() {
            "update"
        } else {
            "insert"
        };
        self.response_affected_with_op(task, 1, op_str)
    }
}
