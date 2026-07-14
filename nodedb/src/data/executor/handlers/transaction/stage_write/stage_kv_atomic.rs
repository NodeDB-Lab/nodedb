// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the KV read-modify-write atomics: `Incr`,
//! `IncrFloat`, `Cas`, `GetSet`, plus `BatchPut`.
//!
//! Split out of `stage_kv.rs` to keep that file under the file-size limit.
//! Every handler here follows the same shape as `stage_kv.rs`'s
//! `InsertOnConflictUpdate` handler: resolve the current value under
//! BASE ∪ OVERLAY via [`CoreLoop::resolve_kv_current`], compute the new value
//! with the SAME pure function the autocommit engine methods call
//! (`nodedb::engine::kv::atomic_compute`, see `engine_atomic_compute.rs`) so
//! a staged value and its COMMIT-time durable replay never diverge, then
//! stage the new bytes via [`CoreLoop::stage_put_capped`].
//!
//! `ttl_ms` on `Incr` / `BatchPut` lives outside the value body
//! (`KvEntry.expire_at_ms`), so it is staged separately from the value: when
//! non-zero, it is ALSO recorded in the overlay's KV TTL delta map
//! (`StagedTtl::ExpireAt`, sibling to `Staged`, populated the same way
//! `stage_kv_ttl.rs` stages `EXPIRE`) so a same-transaction `GetTtl` observes
//! it. COMMIT still replays the buffered plan through the real
//! `execute_kv_incr` / `execute_kv_batch_put` path unchanged -- the overlay
//! TTL delta only affects in-transaction reads.
//!
//! `Incr` / `IncrFloat` / `Cas` / `GetSet` carry a planner-assigned surrogate
//! on their plan (content-addressed on the key, same as `Put`/`Insert`) so the
//! durable COMMIT-time replay through `execute_kv_incr` writes each row with
//! its stable cross-engine identity. The statement-time staging overlay,
//! however, does not persist and keys its per-collection `by_surrogate` map by
//! its own `u32` slot, so it ignores the plan surrogate here. Distinct
//! keys need distinct overlay slots or a second key's staged Put would
//! silently clobber a first key's slot. [`kv_atomic_stage_ctx`] resolves a
//! stable slot: the overlay's own doc_id → surrogate binding when this key
//! was already staged earlier in the same transaction (so an `Incr` chain on
//! one key keeps landing on the same slot), otherwise a deterministic hash
//! of the raw key bytes — unique per key within the collection for any
//! realistic transaction, and never persisted (COMMIT replay uses the real
//! `KvEngine` atomic path, which ignores the overlay's surrogate entirely).

use nodedb_physical::physical_plan::KvOp;
use nodedb_types::Surrogate;

use super::context::StageCtx;
use super::stage_kv::hex_key;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::StagedTtl;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::{AtomicError, atomic_compute, current_ms};
use crate::types::TxnId;

/// FNV-1a 32-bit hash, used only to derive a stable, collection-local overlay
/// slot for a KV key that has no planner-assigned surrogate. Not a security
/// boundary and never persisted — see the module doc for why collisions are
/// immaterial (COMMIT replay never reads it).
fn synthetic_kv_surrogate(key: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &b in key {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    // 0 is reserved elsewhere (`Surrogate::ZERO`) to mean "unresolved /
    // absent"; remap the vanishingly unlikely zero hash away from it.
    if hash == 0 { 1 } else { hash }
}

impl CoreLoop {
    /// Route a stageable KV atomic op (`Incr`, `IncrFloat`, `Cas`, `GetSet`,
    /// `BatchPut`) to its staging handler.
    pub(in crate::data::executor) fn execute_stage_kv_atomic(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &KvOp,
    ) -> Response {
        match op {
            KvOp::Incr {
                collection,
                key,
                delta,
                ttl_ms,
                // The plan's cross-engine surrogate is applied by the durable
                // COMMIT-time replay through `execute_kv_incr`; the staging
                // overlay keys its own slots (see module doc) and ignores it.
                surrogate: _,
            } => {
                let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key);
                self.stage_kv_ttl_side_effect(&ctx, *ttl_ms);
                self.stage_kv_incr(&ctx, key, *delta)
            }
            KvOp::IncrFloat {
                collection,
                key,
                delta,
                surrogate: _,
            } => {
                let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key);
                self.stage_kv_incr_float(&ctx, key, *delta)
            }
            KvOp::Cas {
                collection,
                key,
                expected,
                new_value,
                surrogate: _,
            } => {
                let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key);
                self.stage_kv_cas(&ctx, key, expected, new_value)
            }
            KvOp::GetSet {
                collection,
                key,
                new_value,
                surrogate: _,
            } => {
                let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key);
                self.stage_kv_getset(&ctx, key, new_value)
            }
            KvOp::BatchPut {
                collection,
                entries,
                ttl_ms,
                // The plan's per-entry cross-engine surrogates are applied by
                // the durable COMMIT-time replay through `execute_kv_batch_put`;
                // the staging overlay keys its own slots (see module doc) and
                // ignores them here, same as `Incr`/`Cas`/`GetSet` above.
                surrogates: _,
            } => self.stage_kv_batch_put(task, tid, txn_id, collection, entries, *ttl_ms),
            other => unreachable!(
                "execute_stage_kv_atomic called on a non-atomic KvOp; \
                 caller invariant broken: {other:?}"
            ),
        }
    }

    /// Build the [`StageCtx`] for a KV atomic op, resolving a stable
    /// collection-local overlay surrogate slot (see module doc).
    ///
    /// `pub(super)` so the sibling `stage_kv_transfer.rs` module reuses the
    /// exact same surrogate-resolution logic for `FieldSet` / `Transfer` /
    /// `TransferItem`, which carry no surrogate on their plan either.
    pub(super) fn kv_atomic_stage_ctx<'a>(
        &self,
        task: &'a ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &'a str,
        key: &[u8],
    ) -> StageCtx<'a> {
        let doc_id = hex_key(key);
        let coll_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let surrogate = self
            .txn_overlays
            .get(&txn_id)
            .and_then(|o| o.surrogate_for_doc_id(&coll_key, &doc_id))
            .unwrap_or_else(|| synthetic_kv_surrogate(key));
        StageCtx::new(
            task,
            tid,
            txn_id,
            collection,
            doc_id,
            Surrogate::new(surrogate),
        )
    }

    // ── Incr / IncrFloat: read-modify-write, computed via the shared engine
    //    value-computation module so staged == commit-replay ─────────────

    fn stage_kv_incr(&mut self, ctx: &StageCtx<'_>, key: &[u8], delta: i64) -> Response {
        let current = self.resolve_kv_current(ctx, key);
        match atomic_compute::incr(current.as_deref(), delta) {
            Ok((new_i64, new_bytes)) => {
                if let Err(e) = self.stage_put_capped(ctx, new_bytes) {
                    return self.response_error(ctx.task, e);
                }
                self.kv_atomic_json_response(ctx.task, &serde_json::json!({ "value": new_i64 }))
            }
            Err(e) => self.kv_atomic_error(ctx.task, ctx.collection, e),
        }
    }

    fn stage_kv_incr_float(&mut self, ctx: &StageCtx<'_>, key: &[u8], delta: f64) -> Response {
        let current = self.resolve_kv_current(ctx, key);
        match atomic_compute::incr_float(current.as_deref(), delta) {
            Ok((new_f64, new_bytes)) => {
                if let Err(e) = self.stage_put_capped(ctx, new_bytes) {
                    return self.response_error(ctx.task, e);
                }
                self.kv_atomic_json_response(ctx.task, &serde_json::json!({ "value": new_f64 }))
            }
            Err(e) => self.kv_atomic_error(ctx.task, ctx.collection, e),
        }
    }

    // ── Cas: compare BASE ∪ OVERLAY current, stage on match ─────────────

    fn stage_kv_cas(
        &mut self,
        ctx: &StageCtx<'_>,
        key: &[u8],
        expected: &[u8],
        new_value: &[u8],
    ) -> Response {
        let current = self.resolve_kv_current(ctx, key);
        let (matches, write_bytes) = atomic_compute::cas(current.as_deref(), expected, new_value);

        if matches && let Err(e) = self.stage_put_capped(ctx, write_bytes) {
            return self.response_error(ctx.task, e);
        }

        let current_b64 = current
            .as_ref()
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        self.kv_atomic_json_response(
            ctx.task,
            &serde_json::json!({
                "success": matches,
                "current_value": current_b64,
            }),
        )
    }

    // ── GetSet: stage new value, return BASE ∪ OVERLAY old value ────────

    fn stage_kv_getset(&mut self, ctx: &StageCtx<'_>, key: &[u8], new_value: &[u8]) -> Response {
        let current = self.resolve_kv_current(ctx, key);
        let write_bytes = atomic_compute::getset(current.as_deref(), new_value);
        if let Err(e) = self.stage_put_capped(ctx, write_bytes) {
            return self.response_error(ctx.task, e);
        }
        let old_b64 = current
            .as_ref()
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        self.kv_atomic_json_response(ctx.task, &serde_json::json!({ "old_value": old_b64 }))
    }

    // ── BatchPut: stage every entry via the same per-key put primitive ──

    fn stage_kv_batch_put(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        entries: &[(Vec<u8>, Vec<u8>)],
        ttl_ms: u64,
    ) -> Response {
        for (key, value) in entries {
            let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key);
            self.stage_kv_ttl_side_effect(&ctx, ttl_ms);
            if let Err(e) = self.stage_put_capped(&ctx, value.clone()) {
                return self.response_error(task, e);
            }
        }
        match response_codec::encode_count("inserted", entries.len()) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    /// Stage the TTL side-effect a non-zero `ttl_ms` on `Incr` / `BatchPut`
    /// carries, so a same-transaction `GetTtl` observes it (see module doc).
    /// A zero `ttl_ms` means "no TTL requested" and is a no-op here, matching
    /// the base `atomic_put` / `execute_kv_batch_put` semantics.
    pub(super) fn stage_kv_ttl_side_effect(&mut self, ctx: &StageCtx<'_>, ttl_ms: u64) {
        if ttl_ms == 0 {
            return;
        }
        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        self.txn_overlay_mut(ctx.txn_id).set_ttl(
            ctx.coll_key.clone(),
            ctx.surrogate.0,
            &ctx.document_id,
            StagedTtl::ExpireAt(now_ms.saturating_add(ttl_ms)),
        );
    }

    // ── Shared response helpers ──────────────────────────────────────────

    fn kv_atomic_json_response(&self, task: &ExecutionTask, value: &serde_json::Value) -> Response {
        match response_codec::encode_json(value) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    fn kv_atomic_error(&self, task: &ExecutionTask, collection: &str, e: AtomicError) -> Response {
        match e {
            AtomicError::TypeMismatch { detail } => self.response_error(
                task,
                ErrorCode::TypeMismatch {
                    collection: collection.to_string(),
                    detail,
                },
            ),
            AtomicError::Overflow => self.response_error(
                task,
                ErrorCode::OverflowError {
                    collection: collection.to_string(),
                },
            ),
            AtomicError::Encode { detail } => {
                self.response_error(task, ErrorCode::Internal { detail })
            }
        }
    }
}
