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
    /// decode the stored value, apply the updates (with `EXCLUDED`
    /// resolving to the would-be-inserted row), and write the merged
    /// result back.
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
                let existing_val = match nodedb_types::value_from_msgpack(existing_raw) {
                    Ok(v) => v,
                    Err(_) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to decode existing KV value for ON CONFLICT \
                                         DO UPDATE"
                                    .into(),
                            },
                        );
                    }
                };
                let excluded_val = match nodedb_types::value_from_msgpack(value) {
                    Ok(v) => v,
                    Err(_) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to decode incoming KV value for ON CONFLICT \
                                         DO UPDATE"
                                    .into(),
                            },
                        );
                    }
                };
                let merged =
                    match crate::data::executor::handlers::upsert::apply_on_conflict_updates(
                        existing_val,
                        &excluded_val,
                        updates,
                    ) {
                        Ok(v) => v,
                        Err(e) => return self.response_error(task, e),
                    };
                match nodedb_types::value_to_msgpack(&merged) {
                    Ok(b) => b,
                    Err(_) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to encode merged KV value".into(),
                            },
                        );
                    }
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
        let key_str = String::from_utf8_lossy(key);
        let (op, old_slice): (_, Option<&[u8]>) = match existing_bytes.as_deref() {
            Some(o) => (crate::event::WriteOp::Update, Some(o)),
            None => (crate::event::WriteOp::Insert, None),
        };
        self.emit_write_event(
            task,
            collection,
            op,
            crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
            Some(&stored_bytes),
            old_slice,
        );

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
        self.response_ok(task)
    }
}
