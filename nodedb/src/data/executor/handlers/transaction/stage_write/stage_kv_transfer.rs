// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the three remaining multi-key/multi-value KV
//! writes: `FieldSet` (single-key read-modify-write), `Transfer` (atomic
//! two-key fungible balance move), and `TransferItem` (atomic cross-collection
//! non-fungible move).
//!
//! Every handler here resolves BASE ∪ OVERLAY current values via
//! [`CoreLoop::resolve_kv_current`] and computes the new body with the SAME
//! pure function the autocommit `CoreLoop` handlers call
//! (`kv::field_compute::merge_field_updates`, `kv::transfer_compute::
//! compute_transfer`), so a staged value and its COMMIT-time durable replay
//! are never derived from different code paths -- mirrors `stage_kv_atomic.rs`'s
//! reuse of `engine_atomic_compute`.
//!
//! Like `Incr` / `IncrFloat` / `Cas` / `GetSet`, these three ops carry a
//! planner-assigned cross-engine surrogate on their plan. That surrogate binds
//! the durable identity at COMMIT-time replay (`execute_kv_field_set` /
//! `execute_kv_transfer` / `execute_kv_transfer_item`); the statement-time
//! staging overlay does not persist and keys its own slots, so
//! [`CoreLoop::kv_atomic_stage_ctx`] (shared with `stage_kv_atomic.rs`)
//! resolves a stable overlay slot per key and the plan surrogate is ignored
//! here.

use nodedb_physical::physical_plan::KvOp;

use super::context::StageCtx;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::field_compute::merge_field_updates;
use crate::data::executor::handlers::kv::transfer_compute::{TransferError, compute_transfer};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

/// The `(task, tenant, txn)` triple every KV transfer stage handler threads
/// through to build per-key [`StageCtx`]s. Bundled so the multi-key handlers
/// stay within the argument-count bound without an `#[allow]`.
struct StageKvTxn<'a> {
    task: &'a ExecutionTask,
    tid: u64,
    txn_id: TxnId,
}

impl CoreLoop {
    /// Route `FieldSet` / `Transfer` / `TransferItem` to their staging
    /// handler.
    ///
    /// Caller invariant: `op` must be one of these three variants.
    pub(in crate::data::executor) fn execute_stage_kv_transfer(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &KvOp,
    ) -> Response {
        let cx = StageKvTxn { task, tid, txn_id };
        match op {
            KvOp::FieldSet {
                collection,
                key,
                updates,
                // Durable identity binds at COMMIT-time replay; the overlay
                // keys its own slots (see module doc) and ignores it.
                surrogate: _,
            } => {
                let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key);
                self.stage_kv_field_set(&ctx, key, updates)
            }
            KvOp::Transfer {
                collection,
                source_key,
                dest_key,
                field,
                amount,
                debit_surrogate: _,
                credit_surrogate: _,
            } => self.stage_kv_transfer(&cx, collection, source_key, dest_key, field, *amount),
            KvOp::TransferItem {
                source_collection,
                dest_collection,
                item_key,
                dest_key,
                surrogate: _,
            } => self.stage_kv_transfer_item(
                &cx,
                source_collection,
                dest_collection,
                item_key,
                dest_key,
            ),
            other => unreachable!(
                "execute_stage_kv_transfer called on an unexpected KvOp; \
                 caller invariant broken: {other:?}"
            ),
        }
    }

    // ── FieldSet: single-key read-modify-write ──────────────────────────

    fn stage_kv_field_set(
        &mut self,
        ctx: &StageCtx<'_>,
        key: &[u8],
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        let current = self.resolve_kv_current(ctx, key);
        let computed = match merge_field_updates(current.as_deref(), updates) {
            Ok(c) => c,
            Err(e) => return self.response_error(ctx.task, e),
        };
        if let Err(e) = self.stage_put_capped(ctx, computed.new_value) {
            return self.response_error(ctx.task, e);
        }
        match response_codec::encode_json(&serde_json::json!({
            "fields_added": computed.fields_added,
        })) {
            Ok(payload) => self.response_with_payload(ctx.task, payload),
            Err(e) => self.response_error(ctx.task, e),
        }
    }

    // ── Transfer: two-key read-modify-write in one collection ───────────

    fn stage_kv_transfer(
        &mut self,
        cx: &StageKvTxn<'_>,
        collection: &str,
        source_key: &[u8],
        dest_key: &[u8],
        field: &str,
        amount: f64,
    ) -> Response {
        let task = cx.task;
        let source_ctx = self.kv_atomic_stage_ctx(task, cx.tid, cx.txn_id, collection, source_key);
        let Some(source_bytes) = self.resolve_kv_current(&source_ctx, source_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        let dest_ctx = self.kv_atomic_stage_ctx(task, cx.tid, cx.txn_id, collection, dest_key);
        let dest_bytes = self.resolve_kv_current(&dest_ctx, dest_key);

        let computed = match compute_transfer(&source_bytes, dest_bytes.as_deref(), field, amount) {
            Ok(c) => c,
            Err(TransferError::TypeMismatch(detail)) => {
                return self.response_error(
                    task,
                    ErrorCode::TypeMismatch {
                        collection: collection.to_string(),
                        detail,
                    },
                );
            }
            Err(TransferError::InsufficientBalance { have, need }) => {
                return self.response_error(
                    task,
                    ErrorCode::InsufficientBalance {
                        collection: collection.to_string(),
                        detail: format!("source has {have}, need {need}"),
                    },
                );
            }
        };

        if let Err(e) = self.stage_put_capped(&source_ctx, computed.new_source) {
            return self.response_error(task, e);
        }
        if let Err(e) = self.stage_put_capped(&dest_ctx, computed.new_dest) {
            return self.response_error(task, e);
        }

        let src_str = String::from_utf8_lossy(source_key);
        let dst_str = String::from_utf8_lossy(dest_key);
        match response_codec::encode_json(&serde_json::json!({
            "source_key": src_str,
            "dest_key": dst_str,
            "field": field,
            "amount": amount,
            "source_balance": computed.source_balance_after,
            "dest_balance": computed.dest_balance_after,
        })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    // ── TransferItem: cross-collection tombstone + put ──────────────────

    fn stage_kv_transfer_item(
        &mut self,
        cx: &StageKvTxn<'_>,
        source_collection: &str,
        dest_collection: &str,
        item_key: &[u8],
        dest_key: &[u8],
    ) -> Response {
        let task = cx.task;
        let source_ctx =
            self.kv_atomic_stage_ctx(task, cx.tid, cx.txn_id, source_collection, item_key);
        let Some(item_bytes) = self.resolve_kv_current(&source_ctx, item_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        self.txn_overlay_mut(cx.txn_id).insert_tombstone(
            source_ctx.coll_key.clone(),
            source_ctx.surrogate.0,
            &source_ctx.document_id,
        );

        let dest_ctx = self.kv_atomic_stage_ctx(task, cx.tid, cx.txn_id, dest_collection, dest_key);
        if let Err(e) = self.stage_put_capped(&dest_ctx, item_bytes) {
            return self.response_error(task, e);
        }

        let item_str = String::from_utf8_lossy(item_key);
        let dest_str = String::from_utf8_lossy(dest_key);
        match response_codec::encode_json(&serde_json::json!({
            "item_key": item_str,
            "dest_key": dest_str,
            "source_collection": source_collection,
            "dest_collection": dest_collection,
        })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }
}
