// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the KV `InsertOnConflictUpdate`.
//!
//! Split out of `stage_kv.rs` to keep that file under the per-file line
//! budget, the same way `stage_kv_atomic.rs` / `stage_kv_transfer.rs` /
//! `stage_kv_ttl.rs` / `stage_kv_delete.rs` were.
//!
//! Unlike the plain `Put` / `Insert` / `InsertIfAbsent` staging in
//! `stage_kv.rs`, this op has to resolve the current row (via
//! `resolve_kv_current`, BASE ∪ OVERLAY), decode both sides, merge them
//! through the same `apply_on_conflict_updates` the base (non-staged)
//! handler uses, and re-encode -- all before the RLS write check and the
//! actual staged put, since the write policy has to decide the row image
//! staging produced, not one COMMIT re-derives later.

use nodedb_physical::physical_plan::UpdateValue;

use super::context::StageCtx;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;

impl CoreLoop {
    // ── InsertOnConflictUpdate: resolve current, merge, tag by outcome ──────

    pub(super) fn stage_kv_insert_on_conflict_update(
        &mut self,
        ctx: &StageCtx<'_>,
        key: &[u8],
        value: &[u8],
        updates: &[(String, UpdateValue)],
        ttl_ms: u64,
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> Response {
        let existing = self.resolve_kv_current(ctx, key);
        let (stored_bytes, op) = match &existing {
            None => (value.to_vec(), "insert"),
            Some(existing_raw) => {
                let existing_val = match nodedb_types::value_from_msgpack(existing_raw) {
                    Ok(v) => v,
                    Err(_) => {
                        return self.response_error(
                            ctx.task,
                            ErrorCode::Internal {
                                detail: "failed to decode existing KV value for staged \
                                         ON CONFLICT DO UPDATE"
                                    .into(),
                            },
                        );
                    }
                };
                let excluded_val = match nodedb_types::value_from_msgpack(value) {
                    Ok(v) => v,
                    Err(_) => {
                        return self.response_error(
                            ctx.task,
                            ErrorCode::Internal {
                                detail: "failed to decode incoming KV value for staged \
                                         ON CONFLICT DO UPDATE"
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
                        Err(e) => return self.response_error(ctx.task, e),
                    };
                match nodedb_types::value_to_msgpack(&merged) {
                    Ok(b) => (b, "update"),
                    Err(_) => {
                        return self.response_error(
                            ctx.task,
                            ErrorCode::Internal {
                                detail: "failed to encode merged KV value for staged \
                                         ON CONFLICT DO UPDATE"
                                    .into(),
                            },
                        );
                    }
                }
            }
        };

        // Staging is where an in-transaction statement's row image is produced,
        // so it is where the write policy has to decide it -- COMMIT installs
        // what the overlay holds rather than re-deriving it.
        if let Err(e) = self.stage_admit_write(
            rls_write_check,
            &stored_bytes,
            &ctx.document_id,
            ctx.database_id,
            ctx.tid,
            ctx.collection,
        ) {
            return self.response_error(ctx.task, e);
        }

        // `ttl_ms` applies unconditionally, on both the insert and the
        // update branch above -- mirrors `execute_kv_insert_on_conflict_update`,
        // which passes `ttl_ms` straight into `kv_engine.put(..)` regardless
        // of whether `existing_bytes` was `None` or `Some`.
        self.stage_kv_ttl_side_effect(ctx, ttl_ms);
        if let Err(e) = self.stage_put_capped(ctx, stored_bytes) {
            return self.response_error(ctx.task, e);
        }

        let payload = match response_codec::encode_json_as_msgpack(&serde_json::json!({
            "affected": 1,
            "op": op,
        })) {
            Ok(p) => p,
            Err(e) => return self.response_error(ctx.task, e),
        };
        self.response_with_payload(ctx.task, payload)
    }
}
