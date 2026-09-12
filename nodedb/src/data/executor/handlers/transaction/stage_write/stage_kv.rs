// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for KV point puts: `Put`, `Insert`,
//! `InsertIfAbsent`. Sibling files stage the rest of the fourteen
//! stageable `KvOp`s. A KV row's overlay identity is
//! [`kv_row_identity`] of its raw key, applied symmetrically here and in
//! the read-merge paths.

use nodedb_physical::physical_plan::KvOp;
use nodedb_types::{RowIdentity, Surrogate};

use super::context::StageCtx;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::types::TxnId;

/// Lowercase-hex encode a raw KV key. [`unhex_key`] is the inverse.
fn hex_key(key: &[u8]) -> String {
    let mut s = String::with_capacity(key.len() * 2);
    for b in key {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// The overlay identity of a KV row: its raw key, hex encoded, taken
/// verbatim. Every KV staging writer and read-merge reader builds the
/// identity through this one function.
pub(in crate::data::executor) fn kv_row_identity(raw_key: &[u8]) -> RowIdentity {
    RowIdentity::from_user_key(hex_key(raw_key))
}

/// Decode a lowercase-hex KV overlay doc-id back to raw key bytes, the
/// inverse of [`hex_key`]. Returns `None` for malformed hex.
pub(in crate::data::executor) fn unhex_key(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for &[hi_byte, lo_byte] in bytes.as_chunks::<2>().0 {
        let hi = (hi_byte as char).to_digit(16)?;
        let lo = (lo_byte as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

impl CoreLoop {
    /// Route a stageable `KvOp` to its staging handler. `op` must be one of
    /// the ops `is_stageable_write` accepts — every other `KvOp` is
    /// unreachable here.
    pub(in crate::data::executor) fn execute_stage_kv(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &KvOp,
    ) -> Response {
        match op {
            KvOp::Put {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                ..
            } => {
                let ctx = self.kv_stage_ctx(task, tid, txn_id, collection.as_str(), key, *surrogate);
                self.stage_kv_put(&ctx, value, *ttl_ms)
            }
            KvOp::Insert {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                ..
            } => {
                let ctx = self.kv_stage_ctx(task, tid, txn_id, collection.as_str(), key, *surrogate);
                self.stage_kv_insert(&ctx, key, value, *ttl_ms)
            }
            KvOp::InsertIfAbsent {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                ..
            } => {
                let ctx = self.kv_stage_ctx(task, tid, txn_id, collection.as_str(), key, *surrogate);
                self.stage_kv_insert_if_absent(&ctx, key, value, *ttl_ms)
            }
            KvOp::InsertOnConflictUpdate {
                collection,
                key,
                value,
                updates,
                ttl_ms,
                surrogate,
                rls_write_check,
                ..
            } => {
                let ctx = self.kv_stage_ctx(task, tid, txn_id, collection.as_str(), key, *surrogate);
                self.stage_kv_insert_on_conflict_update(
                    &ctx,
                    key,
                    value,
                    updates,
                    *ttl_ms,
                    rls_write_check,
                )
            }
            KvOp::Delete {
                collection,
                keys,
                rls_write_check,
            } => self.stage_kv_delete(task, tid, txn_id, collection.as_str(), keys, rls_write_check),
            KvOp::BatchPut { .. }
            | KvOp::Incr { .. }
            | KvOp::IncrFloat { .. }
            | KvOp::Cas { .. }
            | KvOp::GetSet { .. } => self.execute_stage_kv_atomic(task, tid, txn_id, op),
            KvOp::FieldSet { .. } | KvOp::Transfer { .. } | KvOp::TransferItem { .. } => {
                self.execute_stage_kv_transfer(task, tid, txn_id, op)
            }
            KvOp::Expire {
                collection,
                key,
                ttl_ms,
                rls_write_check,
            } => self.execute_stage_kv_expire(
                task,
                super::stage_kv_ttl::StageKvTtlTarget {
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    key,
                    rls_write_check,
                },
                *ttl_ms,
            ),
            KvOp::Persist {
                collection,
                key,
                rls_write_check,
            } => self.execute_stage_kv_persist(
                task,
                super::stage_kv_ttl::StageKvTtlTarget {
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    key,
                    rls_write_check,
                },
            ),
            KvOp::Get { .. }
            | KvOp::Scan { .. }
            | KvOp::BatchGet { .. }
            | KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::FieldGet { .. }
            | KvOp::GetTtl { .. }
            | KvOp::Truncate { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. }
            | KvOp::MaterializeScan { .. }
            // Resolve-before-propose is autocommit-only: it decides against
            // committed state and proposes directly, never through staging.
            | KvOp::ResolveWrite(_)
            | KvOp::ResolvedWrite { .. }
            // Predicate DML resolves its row set from committed state at
            // apply time, so there is no point write to stage.
            | KvOp::PredicateUpdate { .. }
            | KvOp::PredicateDelete { .. } => self.stage_not_point_write(task),
        }
    }

    /// Build the shared [`StageCtx`] routing bundle for a KV write, keyed by
    /// [`kv_row_identity`] rather than a document primary key.
    fn kv_stage_ctx<'a>(
        &self,
        task: &'a ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &'a str,
        key: &[u8],
        surrogate: Surrogate,
    ) -> StageCtx<'a> {
        StageCtx::new(
            task,
            tid,
            txn_id,
            collection,
            kv_row_identity(key),
            surrogate,
        )
    }

    // ── Put: upsert, no existence check ─────────────────────────────────────

    fn stage_kv_put(&mut self, ctx: &StageCtx<'_>, value: &[u8], ttl_ms: u64) -> Response {
        self.stage_kv_ttl_side_effect(ctx, ttl_ms);
        if let Err(e) = self.stage_put_capped(ctx, value.to_vec()) {
            return self.response_error(ctx.task, e);
        }
        self.stage_count_response(ctx.task, 1)
    }

    // ── Insert: BASE ∪ OVERLAY uniqueness, statement-time constraint error ──

    fn stage_kv_insert(
        &mut self,
        ctx: &StageCtx<'_>,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
    ) -> Response {
        if self.stage_kv_pk_present(ctx, key) {
            let key_str = String::from_utf8_lossy(key);
            return self.response_error(
                ctx.task,
                crate::Error::RejectedConstraint {
                    collection: ctx.collection.to_string(),
                    constraint: "unique".to_string(),
                    detail: format!(
                        "duplicate key value '{key_str}' violates primary-key \
                         uniqueness on '{}'",
                        ctx.collection
                    ),
                },
            );
        }
        self.stage_kv_ttl_side_effect(ctx, ttl_ms);
        if let Err(e) = self.stage_put_capped(ctx, value.to_vec()) {
            return self.response_error(ctx.task, e);
        }
        self.stage_count_response(ctx.task, 1)
    }

    // ── InsertIfAbsent: silent no-op on conflict ─────────────────────────────

    fn stage_kv_insert_if_absent(
        &mut self,
        ctx: &StageCtx<'_>,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
    ) -> Response {
        if self.stage_kv_pk_present(ctx, key) {
            return self.stage_count_response(ctx.task, 0);
        }
        self.stage_kv_ttl_side_effect(ctx, ttl_ms);
        if let Err(e) = self.stage_put_capped(ctx, value.to_vec()) {
            return self.response_error(ctx.task, e);
        }
        self.stage_count_response(ctx.task, 1)
    }

    // ── Shared KV constraint / resolution helpers ───────────────────────────

    /// True when `key` is present under base ∪ overlay (mirrors
    /// `stage_pk_present`, but against the KV engine). `pub(super)` so
    /// `stage_kv_ttl.rs` reuses this for `Expire`/`Persist` found checks.
    pub(super) fn stage_kv_pk_present(&self, ctx: &StageCtx<'_>, key: &[u8]) -> bool {
        match self.stage_overlay_pk(ctx) {
            super::constraint::OverlayPk::Present => true,
            super::constraint::OverlayPk::Absent => false,
            super::constraint::OverlayPk::Unstaged => {
                let now_ms = current_ms();
                self.kv_engine
                    .get(ctx.database_id, ctx.tid, ctx.collection, key, now_ms)
                    .is_some()
            }
        }
    }

    /// Resolve the current value for `key` under base ∪ overlay, preferring
    /// a staged put/tombstone over the base KV engine. `pub(super)` so
    /// `stage_kv_atomic.rs` reuses this instead of re-deriving it.
    pub(super) fn resolve_kv_current(&self, ctx: &StageCtx<'_>, key: &[u8]) -> Option<Vec<u8>> {
        match self
            .txn_overlays
            .get(&ctx.txn_id)
            .and_then(|o| o.get(&ctx.coll_key, ctx.surrogate.0))
        {
            Some(Staged::Put(body)) => Some(body.clone()),
            Some(Staged::Tombstone) => None,
            None => {
                let now_ms = current_ms();
                self.kv_engine
                    .get(ctx.database_id, ctx.tid, ctx.collection, key, now_ms)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_physical::physical_plan::DocumentOp;

    use super::*;
    use crate::bridge::envelope::{
        Admission, ExemptReason, PhysicalPlan, Priority, Request, Status,
    };
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::kv::crud::KvGetParams;
    use crate::data::executor::handlers::transaction::overlay::StagedTtl;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::*;

    /// A minimal read-only `ExecutionTask`; KV staging routes entirely on
    /// the explicit `tid`/`txn_id`/`collection`/`key` args, not `task.plan`.
    fn make_task(txn_id: Option<TxnId>) -> ExecutionTask {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
            document_id: "y".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        let request = Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        };
        ExecutionTask::new(request)
    }

    fn cache_coll_key(tid: u64) -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(tid), "cache".to_string())
    }

    #[test]
    fn stage_put_with_ttl_stages_absolute_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        // Fixed deterministic clock -- the same one `stage_kv_ttl_side_effect`
        // reads (`epoch_system_ms`), never a fresh wall-clock read.
        core.epoch_system_ms = Some(1_000_000);

        let task = make_task(None);
        let txn_id = TxnId::new(1);
        let ctx = core.kv_stage_ctx(&task, 1, txn_id, "cache", b"k1", Surrogate::new(5));

        let resp = core.stage_kv_put(&ctx, b"v1", 30_000);
        assert_eq!(resp.status, Status::Ok);

        assert_eq!(
            core.txn_overlays
                .get(&txn_id)
                .and_then(|o| o.get_ttl(&cache_coll_key(1), 5)),
            Some(StagedTtl::ExpireAt(1_030_000)),
            "a Put with ttl_ms > 0 must stage an absolute expiry instant"
        );
    }

    #[test]
    fn stage_put_without_ttl_stages_no_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.epoch_system_ms = Some(1_000_000);

        let task = make_task(None);
        let txn_id = TxnId::new(1);
        let ctx = core.kv_stage_ctx(&task, 1, txn_id, "cache", b"k1", Surrogate::new(5));

        let resp = core.stage_kv_put(&ctx, b"v1", 0);
        assert_eq!(resp.status, Status::Ok);

        assert_eq!(
            core.txn_overlays
                .get(&txn_id)
                .and_then(|o| o.get_ttl(&cache_coll_key(1), 5)),
            None,
            "ttl_ms == 0 means persistent -- no StagedTtl entry at all"
        );
    }

    #[test]
    fn staged_put_ttl_is_visible_to_a_same_transaction_read() {
        // Read-your-own-writes: a `Put ... WITH ttl` staged this transaction
        // must be visible to a `Get` later in the same transaction, via
        // `execute_kv_get`, not a hand-rolled predicate.
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        // Set expiry far in the past so it's already behind wall-clock,
        // proving the TTL was recorded without sleeping past a real TTL.
        core.epoch_system_ms = Some(1_000);

        let txn_id = TxnId::new(1);
        let stage_task = make_task(None);
        let ctx = core.kv_stage_ctx(&stage_task, 1, txn_id, "cache", b"k1", Surrogate::new(5));
        let put_resp = core.stage_kv_put(&ctx, b"v1", 5_000);
        assert_eq!(put_resp.status, Status::Ok);

        // `txn_id` set so `execute_kv_get` consults the overlay first.
        let read_task = make_task(Some(txn_id));
        let resp = core.execute_kv_get(
            &read_task,
            KvGetParams {
                did: DatabaseId::DEFAULT.as_u64(),
                tid: 1,
                collection: "cache",
                key: b"k1",
                rls_filters: &[],
                surrogate_ceiling: None,
            },
        );
        assert_eq!(resp.status, Status::Ok);
        assert!(
            resp.payload.is_empty(),
            "the row's staged TTL (expiry far in the past relative to \
             wall-clock `current_ms()`) must make it read as absent in the \
             SAME transaction that staged the Put -- before the fix, no \
             StagedTtl was ever recorded, so this read would incorrectly \
             return the staged value forever"
        );
    }

    #[test]
    fn staged_put_with_no_ttl_remains_readable_same_transaction() {
        // ttl_ms == 0 must still round-trip the staged value (no StagedTtl
        // entry means "persistent", not "invisible").
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.epoch_system_ms = Some(1_000_000);

        let txn_id = TxnId::new(1);
        let stage_task = make_task(None);
        let ctx = core.kv_stage_ctx(&stage_task, 1, txn_id, "cache", b"k1", Surrogate::new(5));
        let put_resp = core.stage_kv_put(&ctx, b"v1", 0);
        assert_eq!(put_resp.status, Status::Ok);

        let read_task = make_task(Some(txn_id));
        let resp = core.execute_kv_get(
            &read_task,
            KvGetParams {
                did: DatabaseId::DEFAULT.as_u64(),
                tid: 1,
                collection: "cache",
                key: b"k1",
                rls_filters: &[],
                surrogate_ceiling: None,
            },
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.payload.as_bytes(), b"v1");
    }
}
