// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the KV TTL writes: `Expire` (SQL `EXPIRE`) and
//! `Persist` (SQL `PERSIST`).
//!
//! TTL is KV-specific: only KV entries carry `expire_at_ms`
//! (`engine/kv/entry.rs::KvEntry.expire_at_ms`), stored OUTSIDE the value
//! body, so it is staged into the overlay's KV TTL delta map (`StagedTtl`,
//! sibling to `Staged`, declared in `overlay::staged`) rather than the
//! shared `Staged::Put`/`Tombstone` every engine's read-merge uses. A
//! same-transaction `GetTtl` (`kv/ttl.rs::execute_kv_get_ttl`) consults this
//! same map keyed by the same [`super::stage_kv::kv_row_identity`].
//!
//! Both handlers reuse [`CoreLoop::kv_atomic_stage_ctx`] (the same
//! surrogate-resolution `Incr` / `Cas` / `GetSet` use) to bind a stable
//! collection-local overlay slot for a key that carries no planner-assigned
//! surrogate, and [`CoreLoop::stage_kv_pk_present`] to decide found vs.
//! not-found under BASE ∪ OVERLAY, matching the base `execute_kv_expire` /
//! `execute_kv_persist` handlers' `NotFound` response for a missing key.
//!
//! `now_ms` for the staged `Expire`'s `expire_at_ms` computation is read the
//! SAME way the base `execute_kv_expire` handler does (`epoch_system_ms`
//! fallback to `current_ms()`), so a staged remaining-TTL matches what
//! COMMIT's durable replay through the real `KvEngine::expire` would
//! produce.

use nodedb_types::Surrogate;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::StagedTtl;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::types::TxnId;

/// The row a staged TTL mutation targets, plus the policy that decides it.
///
/// The staging twin of `handlers::kv::ttl::KvTtlTarget`, and deliberately not
/// the same type: staging addresses a row by the transaction whose overlay
/// holds it, so it carries `txn_id` and no `database_id` — the latter is
/// resolved from `task` by [`CoreLoop::kv_atomic_stage_ctx`]. Sharing one
/// struct would mean carrying a field that is meaningless on one of the two
/// sides.
///
/// `Copy` for the same reason its sibling is: `EXPIRE` and `PERSIST` hand the
/// same address to the shared admission check rather than rebuilding it.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct StageKvTtlTarget<'a> {
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub key: &'a [u8],
    /// Compiled row-level-security WRITE predicate.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

impl CoreLoop {
    /// Stage `KvOp::Expire`: record an absolute expiry instant in the
    /// overlay's KV TTL delta map.
    pub(in crate::data::executor) fn execute_stage_kv_expire(
        &mut self,
        task: &ExecutionTask,
        target: StageKvTtlTarget<'_>,
        ttl_ms: u64,
    ) -> Response {
        let StageKvTtlTarget {
            tid,
            txn_id,
            collection,
            key,
            rls_write_check,
        } = target;
        let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key);
        if !self.stage_kv_pk_present(&ctx, key) {
            return self.response_error(task, ErrorCode::NotFound);
        }
        if let Err(e) = self.stage_admit_kv_ttl_target(&ctx, key, rls_write_check) {
            return self.response_error(task, e);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        let coll_key = ctx.coll_key.clone();
        let document_id = ctx.document_id.clone();
        let surrogate: Surrogate = ctx.surrogate;
        self.txn_overlay_mut(txn_id).set_ttl(
            coll_key,
            surrogate.0,
            &document_id,
            StagedTtl::ExpireAt(now_ms.saturating_add(ttl_ms)),
        );
        self.response_ok(task)
    }

    /// Stage `KvOp::Persist`: record "clear any expiry" in the overlay's KV
    /// TTL delta map.
    pub(in crate::data::executor) fn execute_stage_kv_persist(
        &mut self,
        task: &ExecutionTask,
        target: StageKvTtlTarget<'_>,
    ) -> Response {
        let StageKvTtlTarget {
            tid,
            txn_id,
            collection,
            key,
            rls_write_check,
        } = target;
        let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key);
        if !self.stage_kv_pk_present(&ctx, key) {
            return self.response_error(task, ErrorCode::NotFound);
        }
        if let Err(e) = self.stage_admit_kv_ttl_target(&ctx, key, rls_write_check) {
            return self.response_error(task, e);
        }

        let coll_key = ctx.coll_key.clone();
        let document_id = ctx.document_id.clone();
        let surrogate: Surrogate = ctx.surrogate;
        self.txn_overlay_mut(txn_id).set_ttl(
            coll_key,
            surrogate.0,
            &document_id,
            StagedTtl::Persist,
        );
        self.response_ok(task)
    }

    /// Decide the row a staged TTL mutation targets against the write policy.
    ///
    /// A TTL change leaves the body untouched, so the current row under
    /// BASE ∪ OVERLAY is both the pre- and the post-image. Presence was already
    /// established by the caller, so an image that has since resolved to absent
    /// leaves nothing to decide.
    fn stage_admit_kv_ttl_target(
        &self,
        ctx: &super::context::StageCtx<'_>,
        key: &[u8],
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> crate::Result<()> {
        if matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            return Ok(());
        }
        let Some(body) = self.resolve_kv_current(ctx, key) else {
            return Ok(());
        };
        self.stage_admit_kv_image(ctx, &body, rls_write_check)
    }
}
