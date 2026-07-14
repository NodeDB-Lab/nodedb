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
//! same map keyed by the same [`super::stage_kv::hex_key`] identity.
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

impl CoreLoop {
    /// Stage `KvOp::Expire`: record an absolute expiry instant in the
    /// overlay's KV TTL delta map.
    pub(in crate::data::executor) fn execute_stage_kv_expire(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        key: &[u8],
        ttl_ms: u64,
    ) -> Response {
        let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key);
        if !self.stage_kv_pk_present(&ctx, key) {
            return self.response_error(task, ErrorCode::NotFound);
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
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        key: &[u8],
    ) -> Response {
        let ctx = self.kv_atomic_stage_ctx(task, tid, txn_id, collection, key);
        if !self.stage_kv_pk_present(&ctx, key) {
            return self.response_error(task, ErrorCode::NotFound);
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
}
