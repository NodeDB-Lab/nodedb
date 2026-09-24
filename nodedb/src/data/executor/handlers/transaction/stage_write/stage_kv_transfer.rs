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
//! reuse of `nodedb_physical::kv_atomic::compute`.
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

/// The per-statement inputs of a staged fungible transfer, bundled so the
/// handler stays within the argument-count bound while also carrying the
/// collection's compiled write predicate.
struct StageTransfer<'a> {
    collection: &'a str,
    source_key: &'a [u8],
    dest_key: &'a [u8],
    field: &'a str,
    amount: f64,
    /// Compiled RLS write predicate for the collection both rows live in.
    rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

/// The per-statement inputs of a staged cross-collection item move. The two
/// write predicates stay separate because the two collections carry
/// independent policies.
struct StageTransferItem<'a> {
    source_collection: &'a str,
    dest_collection: &'a str,
    item_key: &'a [u8],
    dest_key: &'a [u8],
    source_rls_write_check: &'a nodedb_types::RlsWriteCheck,
    dest_rls_write_check: &'a nodedb_types::RlsWriteCheck,
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
                if_present,
                rls_write_check,
                // The Control Plane refuses `RETURNING` inside a transaction
                // before the write is staged, so no row image is projected here.
                returning: _,
                rls_filters: _,
            } => {
                let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection.as_str(), key);
                self.stage_kv_field_set(&ctx, key, updates, *if_present, rls_write_check)
            }
            KvOp::Transfer {
                collection,
                source_key,
                dest_key,
                field,
                amount,
                debit_surrogate: _,
                credit_surrogate: _,
                rls_write_check,
            } => self.stage_kv_transfer(
                &cx,
                StageTransfer {
                    collection: collection.as_str(),
                    source_key,
                    dest_key,
                    field,
                    amount: *amount,
                    rls_write_check,
                },
            ),
            KvOp::TransferItem {
                source_collection,
                dest_collection,
                item_key,
                dest_key,
                surrogate: _,
                source_rls_write_check,
                dest_rls_write_check,
            } => self.stage_kv_transfer_item(
                &cx,
                StageTransferItem {
                    source_collection: source_collection.as_str(),
                    dest_collection: dest_collection.as_str(),
                    item_key,
                    dest_key,
                    source_rls_write_check,
                    dest_rls_write_check,
                },
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
        if_present: bool,
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> Response {
        let current = self.resolve_kv_current(ctx, key);

        // SQL UPDATE against an absent key is `UPDATE 0`, not a create —
        // mirrors `execute_kv_field_set`'s decision. RETURNING is already
        // refused inside a transaction, so no empty-projection branch here.
        if if_present && current.is_none() {
            return match response_codec::encode_json_as_msgpack(&serde_json::json!({
                "affected": 0,
                "fields_added": 0,
            })) {
                Ok(payload) => self.response_with_payload(ctx.task, payload),
                Err(e) => self.response_error(ctx.task, e),
            };
        }
        let computed = match merge_field_updates(ctx.collection, current.as_deref(), updates) {
            Ok(c) => c,
            Err(e) => return self.response_error(ctx.task, e),
        };
        if let Err(e) = self.stage_admit_kv_image(ctx, &computed.new_value, rls_write_check) {
            return self.response_error(ctx.task, e);
        }
        if let Err(e) = self.stage_put_capped(ctx, computed.new_value) {
            return self.response_error(ctx.task, e);
        }
        match response_codec::encode_json_as_msgpack(&serde_json::json!({
            "affected": 1,
            "fields_added": computed.fields_added,
        })) {
            Ok(payload) => self.response_with_payload(ctx.task, payload),
            Err(e) => self.response_error(ctx.task, e),
        }
    }

    // ── Transfer: two-key read-modify-write in one collection ───────────

    fn stage_kv_transfer(&mut self, cx: &StageKvTxn<'_>, params: StageTransfer<'_>) -> Response {
        let StageTransfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
            rls_write_check,
        } = params;
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

        // Both post-images are decided before either is staged: a transfer is
        // one write, so a policy that rejects the credit must not leave the
        // debit staged behind it.
        if let Err(e) =
            self.stage_admit_kv_image(&source_ctx, &computed.new_source, rls_write_check)
        {
            return self.response_error(task, e);
        }
        if let Err(e) = self.stage_admit_kv_image(&dest_ctx, &computed.new_dest, rls_write_check) {
            return self.response_error(task, e);
        }

        if let Err(e) = self.stage_put_capped(&source_ctx, computed.new_source) {
            return self.response_error(task, e);
        }
        if let Err(e) = self.stage_put_capped(&dest_ctx, computed.new_dest) {
            return self.response_error(task, e);
        }

        let src_str = String::from_utf8_lossy(source_key);
        let dst_str = String::from_utf8_lossy(dest_key);
        match response_codec::encode_json_as_msgpack(&serde_json::json!({
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
        params: StageTransferItem<'_>,
    ) -> Response {
        let StageTransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            source_rls_write_check,
            dest_rls_write_check,
        } = params;
        let task = cx.task;
        let source_ctx =
            self.kv_atomic_stage_ctx(task, cx.tid, cx.txn_id, source_collection, item_key);
        let Some(item_bytes) = self.resolve_kv_current(&source_ctx, item_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        let dest_ctx = self.kv_atomic_stage_ctx(task, cx.tid, cx.txn_id, dest_collection, dest_key);

        // The same bytes are two different images to two independent policies:
        // the row leaving the source and the row arriving at the destination.
        // Both are decided before the source is tombstoned, so a rejected move
        // never removes the row it could not deliver.
        if let Err(e) = self.stage_admit_kv_image(&source_ctx, &item_bytes, source_rls_write_check)
        {
            return self.response_error(task, e);
        }
        if let Err(e) = self.stage_admit_kv_image(&dest_ctx, &item_bytes, dest_rls_write_check) {
            return self.response_error(task, e);
        }

        self.txn_overlay_mut(cx.txn_id).insert_tombstone(
            source_ctx.coll_key.clone(),
            source_ctx.surrogate.0,
            &source_ctx.document_id,
        );

        if let Err(e) = self.stage_put_capped(&dest_ctx, item_bytes) {
            return self.response_error(task, e);
        }

        let item_str = String::from_utf8_lossy(item_key);
        let dest_str = String::from_utf8_lossy(dest_key);
        match response_codec::encode_json_as_msgpack(&serde_json::json!({
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
