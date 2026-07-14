// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the five stageable KV point writes: `Put`,
//! `Insert`, `InsertIfAbsent`, `InsertOnConflictUpdate`, `Delete`.
//!
//! KV is the first non-Document engine to stage into the transaction
//! overlay -- it reuses the exact same overlay ([`TxnOverlay`],
//! [`Staged`]) and the same [`StageCtx`] routing bundle the Document point
//! writes use. The only new piece is identity: a KV row's real key is
//! arbitrary bytes, but the overlay's doc-id index is `String`-keyed, so a
//! KV row's overlay doc-id is the lowercase-hex encoding of its key
//! ([`hex_key`]), applied symmetrically here (stage) and in the read-merge
//! paths (`overlay_point_lookup`, `merge_overlay_into_scan`). The
//! surrogate is the plan's own KV identity for every op except `Delete`,
//! which carries no surrogate on the plan -- resolved from the overlay's
//! `doc_id_to_surrogate` map first, falling back to the base KV engine's
//! key→surrogate binding (`get_with_surrogate`).
//!
//! `ttl_ms` on `Put` / `Insert` / `InsertIfAbsent` / `InsertOnConflictUpdate`
//! lives outside the value body (`KvEntry.expire_at_ms`), so a non-zero
//! `ttl_ms` is also recorded in the overlay's KV TTL delta map via
//! [`CoreLoop::stage_kv_ttl_side_effect`] (`stage_kv_atomic.rs`), the same
//! helper `Incr` / `BatchPut` reuse -- so a same-transaction read observes
//! the staged expiry instead of treating the row as persistent until COMMIT.
//!
//! `Incr` / `IncrFloat` / `Cas` / `GetSet` / `BatchPut` are also stageable,
//! but their handlers live in the sibling `stage_kv_atomic.rs` (kept
//! separate to stay under the file-size limit) -- see that module's doc for
//! their surrogate-resolution and value-computation reuse. `FieldSet` /
//! `Transfer` / `TransferItem` are stageable too, in the sibling
//! `stage_kv_transfer.rs`. `Expire` / `Persist` are stageable too, in the
//! sibling `stage_kv_ttl.rs`. Every other `KvOp` (the sorted-index family,
//! etc.) is out of scope: it never reaches this file because
//! `is_stageable_write` only routes the fourteen ops above here.

use nodedb_physical::physical_plan::{KvOp, UpdateValue};
use nodedb_types::Surrogate;

use super::context::StageCtx;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::types::TxnId;

/// Lowercase-hex encode a raw KV key for use as the overlay's doc-id.
///
/// Applied symmetrically: every staging path in this file calls it to build
/// the `StageCtx.document_id` passed to the shared overlay helpers, and the
/// read-merge paths (`overlay_point_lookup`, `merge_overlay_into_scan`) call
/// it the same way to resolve a KV key back to its overlay entry.
pub(in crate::data::executor) fn hex_key(key: &[u8]) -> String {
    let mut s = String::with_capacity(key.len() * 2);
    for b in key {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Decode a lowercase-hex KV overlay doc-id back to the raw key bytes --
/// the inverse of [`hex_key`]. Returns `None` for malformed hex (never
/// produced by `hex_key` itself, but the overlay's doc-id map is a plain
/// `String`, so the scan-merge caller stays defensive rather than panicking).
pub(in crate::data::executor) fn unhex_key(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        let hi = (chunk[0] as char).to_digit(16)?;
        let lo = (chunk[1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

impl CoreLoop {
    /// Route a stageable `KvOp` to its staging handler.
    ///
    /// Caller invariant: `op` must be one of the five ops `is_stageable_write`
    /// accepts. Every other `KvOp` is unreachable here -- the Control Plane
    /// never builds a `StageWrite` for them.
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
                let ctx = self.kv_stage_ctx(task, tid, txn_id, collection, key, *surrogate);
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
                let ctx = self.kv_stage_ctx(task, tid, txn_id, collection, key, *surrogate);
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
                let ctx = self.kv_stage_ctx(task, tid, txn_id, collection, key, *surrogate);
                self.stage_kv_insert_if_absent(&ctx, key, value, *ttl_ms)
            }
            KvOp::InsertOnConflictUpdate {
                collection,
                key,
                value,
                updates,
                ttl_ms,
                surrogate,
                ..
            } => {
                let ctx = self.kv_stage_ctx(task, tid, txn_id, collection, key, *surrogate);
                self.stage_kv_insert_on_conflict_update(&ctx, key, value, updates, *ttl_ms)
            }
            KvOp::Delete { collection, keys } => {
                self.stage_kv_delete(task, tid, txn_id, collection, keys)
            }
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
            } => self.execute_stage_kv_expire(task, tid, txn_id, collection, key, *ttl_ms),
            KvOp::Persist { collection, key } => {
                self.execute_stage_kv_persist(task, tid, txn_id, collection, key)
            }
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
            | KvOp::MaterializeScan { .. } => self.stage_not_point_write(task),
        }
    }

    /// Build the shared [`StageCtx`] routing bundle for a KV write, keying
    /// the overlay's doc-id by [`hex_key`] rather than a document primary key.
    fn kv_stage_ctx<'a>(
        &self,
        task: &'a ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &'a str,
        key: &[u8],
        surrogate: Surrogate,
    ) -> StageCtx<'a> {
        // `StageCtx.document_id` is `Cow<str>` precisely so a KV row's
        // overlay doc-id can be an owned hex string here, with no borrow
        // from `task` and no leak.
        StageCtx::new(task, tid, txn_id, collection, hex_key(key), surrogate)
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

    // ── InsertOnConflictUpdate: resolve current, merge, tag by outcome ──────

    fn stage_kv_insert_on_conflict_update(
        &mut self,
        ctx: &StageCtx<'_>,
        key: &[u8],
        value: &[u8],
        updates: &[(String, UpdateValue)],
        ttl_ms: u64,
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
                let merged = crate::data::executor::handlers::upsert::apply_on_conflict_updates(
                    existing_val,
                    &excluded_val,
                    updates,
                );
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

        // `ttl_ms` applies unconditionally, on both the insert and the
        // update branch above -- mirrors `execute_kv_insert_on_conflict_update`,
        // which passes `ttl_ms` straight into `kv_engine.put(..)` regardless
        // of whether `existing_bytes` was `None` or `Some`.
        self.stage_kv_ttl_side_effect(ctx, ttl_ms);
        if let Err(e) = self.stage_put_capped(ctx, stored_bytes) {
            return self.response_error(ctx.task, e);
        }

        let payload = match response_codec::encode_json(&serde_json::json!({
            "affected": 1,
            "op": op,
        })) {
            Ok(p) => p,
            Err(e) => return self.response_error(ctx.task, e),
        };
        self.response_with_payload(ctx.task, payload)
    }

    // ── Delete: resolve surrogate (overlay, then base), tombstone ───────────

    fn stage_kv_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        keys: &[Vec<u8>],
    ) -> Response {
        let did = task.request.database_id;
        let mut deleted = 0usize;
        for key in keys {
            let doc_id = hex_key(key);
            let coll_key = (
                did,
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );

            let overlay_staged = self
                .txn_overlays
                .get(&txn_id)
                .and_then(|o| o.get_by_doc_id(&coll_key, &doc_id))
                .cloned();

            let (surrogate, present) = match overlay_staged {
                // A staged put exists: resolve its bound surrogate through
                // the overlay's own doc_id -> surrogate map so the
                // tombstone lands on the same row.
                Some(Staged::Put(_)) => {
                    let s = self
                        .txn_overlays
                        .get(&txn_id)
                        .and_then(|o| o.surrogate_for_doc_id(&coll_key, &doc_id))
                        .unwrap_or(0);
                    (Surrogate::new(s), true)
                }
                // Already staged-deleted in this transaction: absent,
                // matching PostgreSQL/Document DELETE semantics for a
                // missing key (DELETE 0, not an error).
                Some(Staged::Tombstone) => (Surrogate::ZERO, false),
                // Nothing staged: resolve via the base KV engine's own
                // key -> surrogate binding.
                None => {
                    let now_ms = current_ms();
                    match self.kv_engine.get_with_surrogate(
                        did.as_u64(),
                        tid,
                        collection,
                        key,
                        now_ms,
                    ) {
                        Some((_, s)) => (s, true),
                        None => (Surrogate::ZERO, false),
                    }
                }
            };

            if !present {
                continue;
            }

            self.txn_overlay_mut(txn_id)
                .insert_tombstone(coll_key, surrogate.0, &doc_id);
            deleted += 1;
        }
        self.stage_count_response(task, deleted)
    }

    // ── Shared KV constraint / resolution helpers ───────────────────────────

    /// True when `key` is present under BASE ∪ OVERLAY (mirrors
    /// `stage_pk_present`, but against the KV engine rather than the
    /// document sparse store).
    ///
    /// `pub(super)` so the sibling `stage_kv_ttl.rs` reuses the exact same
    /// BASE ∪ OVERLAY presence check `Expire` / `Persist` need to decide
    /// found-vs-not-found, matching the base handlers' `NotFound` semantics.
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

    /// Resolve the current value for `key` under BASE ∪ OVERLAY, preferring
    /// a staged put/tombstone over the base KV engine.
    ///
    /// `pub(super)` (rather than private) so the atomic-op staging handlers
    /// in `stage_kv_atomic.rs` reuse this exact resolution instead of
    /// re-deriving it -- a staged `Incr`/`Cas`/`GetSet` reads the same
    /// BASE ∪ OVERLAY current value a staged `InsertOnConflictUpdate` does.
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

    /// A minimal read-only `ExecutionTask`, `txn_id` set to whatever the
    /// caller passes in -- everything else about the plan is irrelevant to
    /// KV staging / overlay lookups, which route entirely on the explicit
    /// `tid` / `txn_id` / `collection` / `key` arguments, not on `task.plan`.
    fn make_task(txn_id: Option<TxnId>) -> ExecutionTask {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "x".into(),
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
        // Read-your-own-writes regression: a `Put ... WITH ttl` staged this
        // transaction must be visible to a `Get` later in the SAME
        // transaction, via the exact production overlay-read path
        // (`execute_kv_get`), not a hand-rolled predicate.
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        // `stage_kv_put` derives its expiry from `epoch_system_ms`; set it far
        // in the past so the staged expiry is already behind the real
        // wall-clock `execute_kv_get` compares against -- deterministically
        // proving the staged TTL was recorded and is consulted, without
        // sleeping past a real TTL.
        core.epoch_system_ms = Some(1_000);

        let txn_id = TxnId::new(1);
        let stage_task = make_task(None);
        let ctx = core.kv_stage_ctx(&stage_task, 1, txn_id, "cache", b"k1", Surrogate::new(5));
        let put_resp = core.stage_kv_put(&ctx, b"v1", 5_000);
        assert_eq!(put_resp.status, Status::Ok);

        // Same-transaction read: `txn_id` set on the request so
        // `execute_kv_get` consults the staging overlay before the base
        // engine.
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
        // Sibling to the regression test above: ttl_ms == 0 must still round
        // -trip the staged VALUE through the same-transaction read (no
        // StagedTtl entry means "persistent", not "invisible").
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
