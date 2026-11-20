// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the KV read-modify-write atomics: `Incr`,
//! `IncrFloat`, `Cas`, `GetSet`, plus `BatchPut`.
//!
//! Split out of `stage_kv.rs` to keep that file under the file-size limit.
//! Every handler here follows the same shape as `stage_kv.rs`'s
//! `InsertOnConflictUpdate` handler: resolve the current value under
//! BASE ∪ OVERLAY via [`CoreLoop::resolve_kv_current`], compute the new value
//! with the SAME pure function the autocommit engine methods call
//! (`nodedb_physical::kv_atomic::compute`) so
//! a staged value and its COMMIT-time durable replay never diverge, then
//! stage the new bytes via [`CoreLoop::stage_put`].
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

use nodedb_physical::kv_atomic::compute as atomic_compute;
use nodedb_physical::physical_plan::{DeclaredColumn, KvCounterShape, KvOp};
use nodedb_types::Surrogate;

use super::context::StageCtx;
use super::stage_kv::kv_row_identity;
use super::stage_kv_batch::StageKvBatch;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::atomic::incr_float_reply;
use crate::data::executor::handlers::kv::declared_body::fit_kv_image;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::fitted_counter_f64;
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
    // `Surrogate::ZERO` names no row; remap the vanishingly unlikely zero
    // hash away from it.
    if hash == 0 { 1 } else { hash }
}

/// The rules a staged atomic image meets: the write policy, and the declared
/// columns the plan carries.
pub(super) type ImageRules<'a> = (&'a nodedb_types::RlsWriteCheck, &'a [DeclaredColumn]);

/// The image a staged KV write stores for the image it computed: fitted to
/// the declared columns the plan carries, the rule the durable apply runs. A
/// value past a declared column is refused here, at statement time.
fn stage_fit_kv_image(image: Vec<u8>, declared: &[DeclaredColumn]) -> crate::Result<Vec<u8>> {
    Ok(fit_kv_image(&image, declared)?.unwrap_or(image))
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
                declared_columns,
                key,
                delta,
                ttl_ms,
                // The plan's cross-engine surrogate is applied by the durable
                // COMMIT-time replay through `execute_kv_incr`; the staging
                // overlay keys its own slots (see module doc) and ignores it.
                surrogate: _,
                rls_write_check,
                shape,
            } => {
                let declared = match self.kv_op_columns(task, declared_columns, collection.as_str())
                {
                    Ok(declared) => declared,
                    Err(refusal) => return refusal,
                };
                let ctx = match self
                    .kv_atomic_stage_ctx(task, tid, txn_id, collection.as_str(), key)
                    .and_then(|ctx| self.stage_kv_ttl_side_effect(&ctx, *ttl_ms).map(|()| ctx))
                {
                    Ok(ctx) => ctx,
                    Err(e) => return self.response_error(task, e),
                };
                self.stage_kv_incr(&ctx, key, *delta, shape, (rls_write_check, declared))
            }
            KvOp::IncrFloat {
                collection,
                declared_columns,
                key,
                delta,
                surrogate: _,
                rls_write_check,
                shape,
            } => {
                let declared = match self.kv_op_columns(task, declared_columns, collection.as_str())
                {
                    Ok(declared) => declared,
                    Err(refusal) => return refusal,
                };
                match self.kv_atomic_stage_ctx(task, tid, txn_id, collection.as_str(), key) {
                    Ok(ctx) => self.stage_kv_incr_float(
                        &ctx,
                        key,
                        delta,
                        shape,
                        (rls_write_check, declared),
                    ),
                    Err(e) => self.response_error(task, e),
                }
            }
            KvOp::Cas {
                collection,
                declared_columns,
                key,
                expected,
                new_value,
                surrogate: _,
                rls_write_check,
            } => {
                let declared = match self.kv_op_columns(task, declared_columns, collection.as_str())
                {
                    Ok(declared) => declared,
                    Err(refusal) => return refusal,
                };
                match self.kv_atomic_stage_ctx(task, tid, txn_id, collection.as_str(), key) {
                    Ok(ctx) => self.stage_kv_cas(
                        &ctx,
                        key,
                        expected,
                        new_value,
                        (rls_write_check, declared),
                    ),
                    Err(e) => self.response_error(task, e),
                }
            }
            KvOp::GetSet {
                collection,
                declared_columns,
                key,
                new_value,
                surrogate: _,
                rls_filters,
                rls_write_check,
            } => {
                let declared = match self.kv_op_columns(task, declared_columns, collection.as_str())
                {
                    Ok(declared) => declared,
                    Err(refusal) => return refusal,
                };
                match self.kv_atomic_stage_ctx(task, tid, txn_id, collection.as_str(), key) {
                    Ok(ctx) => self.stage_kv_getset(
                        &ctx,
                        key,
                        new_value,
                        rls_filters,
                        (rls_write_check, declared),
                    ),
                    Err(e) => self.response_error(task, e),
                }
            }
            // Three slots are deliberately elided. The plan's per-entry
            // cross-engine surrogates are applied by the durable COMMIT-time
            // replay through `execute_kv_batch_put`; the staging overlay keys
            // its own slots (see module doc) and ignores them here, same as
            // `Incr`/`Cas`/`GetSet` above. `returning` and its `rls_filters`
            // read gate are ignored because a staged write answers no client:
            // it reports a count now and the rows, if any, would only exist at
            // COMMIT — which is why a row-returning write inside a transaction
            // is refused by the dispatch loop rather than reaching here with a
            // projection it could honour.
            KvOp::BatchPut {
                collection,
                declared_columns,
                entries,
                ttl_ms,
                intent,
                ..
            } => {
                let declared = match self.kv_op_columns(task, declared_columns, collection.as_str())
                {
                    Ok(declared) => declared,
                    Err(refusal) => return refusal,
                };
                self.stage_kv_batch_put(
                    task,
                    StageKvBatch {
                        tid,
                        txn_id,
                        collection: collection.as_str(),
                        entries,
                        ttl_ms: *ttl_ms,
                        intent,
                        declared,
                    },
                )
            }
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
    ) -> crate::Result<StageCtx<'a>> {
        let doc_id = kv_row_identity(key);
        let coll_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let bound = match self.txn_overlays.get(&txn_id) {
            Some(o) => o.surrogate_for_doc_id(&coll_key, &doc_id)?,
            None => None,
        };
        let surrogate = bound.unwrap_or_else(|| synthetic_kv_surrogate(key));
        Ok(StageCtx::new(
            task,
            tid,
            txn_id,
            collection,
            doc_id,
            Surrogate::new(surrogate),
        ))
    }

    // ── Incr / IncrFloat: read-modify-write, computed via the shared engine
    //    value-computation module so staged == commit-replay ─────────────

    fn stage_kv_incr(
        &mut self,
        ctx: &StageCtx<'_>,
        key: &[u8],
        delta: i64,
        shape: &KvCounterShape,
        (rls_write_check, declared): ImageRules<'_>,
    ) -> Response {
        let current = match self.resolve_kv_current(ctx, key) {
            Ok(current) => current,
            Err(e) => return self.response_error(ctx.task, e),
        };
        match atomic_compute::incr(current.as_deref(), delta, shape) {
            Ok((new_i64, computed)) => {
                let new_bytes = match stage_fit_kv_image(computed, declared) {
                    Ok(bytes) => bytes,
                    Err(e) => return self.response_error(ctx.task, e),
                };
                if let Err(e) = self.stage_admit_kv_image(ctx, &new_bytes, rls_write_check) {
                    return self.response_error(ctx.task, e);
                }
                if let Err(e) =
                    crate::engine::kv::KvRowTooLarge::check(ctx.collection, key, &new_bytes)
                {
                    return self.response_error(ctx.task, e);
                }
                if let Err(e) = self.stage_put(ctx, new_bytes) {
                    return self.response_error(ctx.task, e);
                }
                self.kv_atomic_json_response(ctx.task, &serde_json::json!({ "value": new_i64 }))
            }
            Err(e) => self.response_atomic_error(ctx.task, ctx.collection, e.into()),
        }
    }

    fn stage_kv_incr_float(
        &mut self,
        ctx: &StageCtx<'_>,
        key: &[u8],
        delta: &str,
        shape: &KvCounterShape,
        (rls_write_check, declared): ImageRules<'_>,
    ) -> Response {
        let current = match self.resolve_kv_current(ctx, key) {
            Ok(current) => current,
            Err(e) => return self.response_error(ctx.task, e),
        };
        match atomic_compute::incr_float(current.as_deref(), delta, shape) {
            Ok((computed_f64, computed)) => {
                let (new_f64, new_bytes) = match fit_kv_image(&computed, declared) {
                    Ok(None) => (computed_f64, computed),
                    Ok(Some(fitted)) => {
                        (fitted_counter_f64(computed_f64, &computed, &fitted), fitted)
                    }
                    Err(e) => return self.response_error(ctx.task, e),
                };
                if let Err(e) = self.stage_admit_kv_image(ctx, &new_bytes, rls_write_check) {
                    return self.response_error(ctx.task, e);
                }
                if let Err(e) =
                    crate::engine::kv::KvRowTooLarge::check(ctx.collection, key, &new_bytes)
                {
                    return self.response_error(ctx.task, e);
                }
                let reply = incr_float_reply(new_f64, &new_bytes);
                if let Err(e) = self.stage_put(ctx, new_bytes) {
                    return self.response_error(ctx.task, e);
                }
                self.kv_atomic_json_response(ctx.task, &reply)
            }
            Err(e) => self.response_atomic_error(ctx.task, ctx.collection, e.into()),
        }
    }

    /// Decide one staged KV image against the compiled write policy, naming
    /// the row by the overlay's own doc-id so a rejection reports the same
    /// identity the overlay filed it under.
    ///
    /// `pub(super)` so every KV staging handler in this directory decides its
    /// image the same way rather than re-deriving the call.
    pub(super) fn stage_admit_kv_image(
        &self,
        ctx: &StageCtx<'_>,
        image: &[u8],
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> crate::Result<()> {
        self.stage_admit_write(
            rls_write_check,
            image,
            &ctx.document_id,
            ctx.database_id,
            ctx.tid,
            ctx.collection,
        )
    }

    // ── Cas: compare BASE ∪ OVERLAY current, stage on match ─────────────

    fn stage_kv_cas(
        &mut self,
        ctx: &StageCtx<'_>,
        key: &[u8],
        expected: &[u8],
        new_value: &[u8],
        (rls_write_check, declared): ImageRules<'_>,
    ) -> Response {
        let current = match self.resolve_kv_current(ctx, key) {
            Ok(current) => current,
            Err(e) => return self.response_error(ctx.task, e),
        };
        let (matches, write_bytes) =
            match atomic_compute::cas(current.as_deref(), expected, new_value) {
                Ok(outcome) => outcome,
                Err(e) => return self.response_atomic_error(ctx.task, ctx.collection, e.into()),
            };

        if matches {
            let write_bytes = match stage_fit_kv_image(write_bytes, declared) {
                Ok(bytes) => bytes,
                Err(e) => return self.response_error(ctx.task, e),
            };
            if let Err(e) = self.stage_admit_kv_image(ctx, &write_bytes, rls_write_check) {
                return self.response_error(ctx.task, e);
            }
            if let Err(e) =
                crate::engine::kv::KvRowTooLarge::check(ctx.collection, key, &write_bytes)
            {
                return self.response_error(ctx.task, e);
            }
            if let Err(e) = self.stage_put(ctx, write_bytes) {
                return self.response_error(ctx.task, e);
            }
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

    fn stage_kv_getset(
        &mut self,
        ctx: &StageCtx<'_>,
        key: &[u8],
        new_value: &[u8],
        rls_filters: &[u8],
        (rls_write_check, declared): ImageRules<'_>,
    ) -> Response {
        let current = match self.resolve_kv_current(ctx, key) {
            Ok(current) => current,
            Err(e) => return self.response_error(ctx.task, e),
        };
        let write_bytes = match atomic_compute::getset(current.as_deref(), new_value) {
            Ok(bytes) => bytes,
            Err(e) => return self.response_atomic_error(ctx.task, ctx.collection, e.into()),
        };
        let write_bytes = match stage_fit_kv_image(write_bytes, declared) {
            Ok(bytes) => bytes,
            Err(e) => return self.response_error(ctx.task, e),
        };
        if let Err(e) = self.stage_admit_kv_image(ctx, &write_bytes, rls_write_check) {
            return self.response_error(ctx.task, e);
        }
        if let Err(e) = crate::engine::kv::KvRowTooLarge::check(ctx.collection, key, &write_bytes) {
            return self.response_error(ctx.task, e);
        }
        if let Err(e) = self.stage_put(ctx, write_bytes) {
            return self.response_error(ctx.task, e);
        }

        // The old value is a row body, so the read policy decides it here the
        // same way the autocommit handler does: an excluded row comes back
        // absent rather than being disclosed by the write that replaced it.
        let disclosable_old = match &current {
            Some(bytes) => match self.row_passes_rls(bytes, rls_filters) {
                Ok(true) => current.as_deref(),
                Ok(false) => None,
                Err(e) => return self.response_error(ctx.task, e),
            },
            None => None,
        };
        let old_b64 = disclosable_old
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        self.kv_atomic_json_response(ctx.task, &serde_json::json!({ "old_value": old_b64 }))
    }

    // ── Shared response helpers ──────────────────────────────────────────

    fn kv_atomic_json_response(&self, task: &ExecutionTask, value: &serde_json::Value) -> Response {
        match response_codec::encode_json_as_msgpack(value) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }
}
