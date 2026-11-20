// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for `KvOp::BatchPut`, and the TTL side effect a
//! non-zero `ttl_ms` on `Incr` or `BatchPut` stages.

use nodedb_physical::physical_plan::{DeclaredColumn, KvBatchIntent};

use super::context::StageCtx;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::batch_plan::{merge_batch_verb, plan_kv_batch};
use crate::data::executor::handlers::transaction::overlay::StagedTtl;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

/// A `KvOp::BatchPut` a transaction stages.
pub(super) struct StageKvBatch<'a> {
    pub(super) tid: u64,
    pub(super) txn_id: TxnId,
    pub(super) collection: &'a str,
    pub(super) entries: &'a [(Vec<u8>, Vec<u8>)],
    pub(super) ttl_ms: u64,
    pub(super) intent: &'a KvBatchIntent,
    /// The declared columns the plan carries.
    pub(super) declared: &'a [DeclaredColumn],
}

impl CoreLoop {
    /// Stage a batch put under BASE ∪ OVERLAY, judging each key by the
    /// batch's intent the way `execute_kv_batch_put` judges it. An `Insert`
    /// batch with a stored or repeated key stages nothing.
    pub(super) fn stage_kv_batch_put(
        &mut self,
        task: &ExecutionTask,
        batch: StageKvBatch<'_>,
    ) -> Response {
        let StageKvBatch {
            tid,
            txn_id,
            collection,
            entries,
            ttl_ms,
            intent,
            declared,
        } = batch;
        let did = task.request.database_id.as_u64();
        // Every key is read under BASE ∪ OVERLAY, so a row this transaction
        // staged earlier counts as stored.
        let rows = match plan_kv_batch(
            collection,
            entries,
            intent,
            declared,
            &self.crs_context(),
            |key| {
                let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key)?;
                self.resolve_kv_current(&ctx, key)
            },
        ) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };
        if let KvBatchIntent::Merge {
            rls_write_check, ..
        } = intent
        {
            // Staging produces the row images COMMIT installs, so the write
            // policy decides them here.
            for row in &rows {
                let ctx = match self.kv_atomic_stage_ctx(task, tid, txn_id, collection, row.key) {
                    Ok(ctx) => ctx,
                    Err(e) => return self.response_error(task, e),
                };
                if let Err(e) = self.stage_admit_write(
                    rls_write_check,
                    &row.stored,
                    &ctx.document_id,
                    did,
                    tid,
                    collection,
                ) {
                    return self.response_error(task, e);
                }
            }
        }
        // Every row is held to the row limit before the first one stages.
        for row in &rows {
            if let Err(e) =
                crate::engine::kv::KvRowTooLarge::check(collection, row.key, &row.stored)
            {
                return self.response_error(task, e);
            }
        }
        for row in &rows {
            let staged = self
                .kv_atomic_stage_ctx(task, tid, txn_id, collection, row.key)
                .and_then(|ctx| {
                    self.stage_kv_ttl_side_effect(&ctx, ttl_ms)?;
                    self.stage_put(&ctx, row.stored.to_vec())
                });
            if let Err(e) = staged {
                return self.response_error(task, e);
            }
        }
        match intent {
            KvBatchIntent::Merge { .. } => {
                self.response_affected_with_op(task, rows.len() as u64, merge_batch_verb(&rows))
            }
            KvBatchIntent::Put
            | KvBatchIntent::Upsert
            | KvBatchIntent::Insert
            | KvBatchIntent::InsertIfAbsent => {
                match response_codec::encode_count("inserted", rows.len()) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, e),
                }
            }
        }
    }

    /// Stage the TTL side-effect a non-zero `ttl_ms` on `Incr` / `BatchPut`
    /// carries, so a same-transaction `GetTtl` observes it (see module doc).
    /// A zero `ttl_ms` means "no TTL requested" and is a no-op here, matching
    /// the base `atomic_put` / `execute_kv_batch_put` semantics.
    pub(super) fn stage_kv_ttl_side_effect(
        &mut self,
        ctx: &StageCtx<'_>,
        ttl_ms: u64,
    ) -> crate::Result<()> {
        if ttl_ms == 0 {
            return Ok(());
        }
        let now_ms = self.kv_read_now_ms();
        self.txn_overlay_mut(ctx.txn_id).set_ttl(
            ctx.coll_key.clone(),
            ctx.surrogate.0,
            &ctx.document_id,
            StagedTtl::ExpireAt(now_ms.saturating_add(ttl_ms)),
        )
    }
}
