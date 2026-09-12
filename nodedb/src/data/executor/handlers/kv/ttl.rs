// SPDX-License-Identifier: BUSL-1.1

//! KV TTL handlers: Expire, Persist, GetTtl.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{Staged, StagedTtl};
use crate::data::executor::handlers::transaction::stage_write::kv_row_identity;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::types::TenantId;

/// The row a TTL mutation targets, plus the policy that decides it.
///
/// `EXPIRE` and `PERSIST` address a row identically and differ only in the
/// instant they install, so they share one bundle rather than repeating the
/// five-field address list twice. The transaction wrappers in
/// `sub_plan_kv_ttl_sorted.rs` pass this straight through to these handlers,
/// so a COMMIT-time replay addresses and decides the row exactly as an
/// autocommit statement does.
///
/// `Copy` because it is a plain address: a wrapper hands the same one to the
/// handler it delegates to rather than rebuilding it field by field, which is
/// what keeps the two from drifting.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct KvTtlTarget<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    /// Compiled row-level-security WRITE predicate. `AdmitAll` means no write
    /// policy restricts this identity here, and the stored row is not read at
    /// all — an ungoverned collection still touches only the TTL metadata.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_expire(
        &mut self,
        task: &ExecutionTask,
        target: KvTtlTarget<'_>,
        ttl_ms: u64,
    ) -> Response {
        let KvTtlTarget {
            did,
            tid,
            collection,
            key,
            ..
        } = target;
        debug!(core = self.core_id, %collection, ttl_ms, "kv expire");
        // `kv_ttl_now_ms` prefers the Control-Plane-resolved instant carried
        // on `task` so live apply installs the exact `expire_at_ms` the
        // durable WAL record encodes (see `wal_append_kv_op`'s `KvOp::Expire`
        // arm); recomputing the wall clock here independently would drift
        // the two apart by the dispatch latency.
        let now_ms = self.kv_ttl_now_ms(task);

        // A TTL mutation leaves the body untouched, so the stored row is both
        // the pre- and the post-image: decide it before the expiry metadata
        // moves.
        if let Err(e) = self.admit_kv_ttl_target(&target, now_ms) {
            return self.response_error(task, e);
        }

        if self
            .kv_engine
            .expire(did, tid, collection, key, ttl_ms, now_ms)
        {
            self.note_kv_write_lsn(task, did, tid, collection, key);
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    pub(in crate::data::executor) fn execute_kv_persist(
        &mut self,
        task: &ExecutionTask,
        target: KvTtlTarget<'_>,
    ) -> Response {
        let KvTtlTarget {
            did,
            tid,
            collection,
            key,
            ..
        } = target;
        debug!(core = self.core_id, %collection, "kv persist");
        // Unlike `execute_kv_expire`, PERSIST clears a key's TTL outright and
        // resolves no instant — `KvEngine::persist` takes no `now_ms` at all,
        // so there is no clock to source from `task` here. The policy check
        // still needs one to skip already-expired rows, and reads the same
        // wall clock the engine's own expiry evaluation would.
        if let Err(e) = self.admit_kv_ttl_target(&target, current_ms()) {
            return self.response_error(task, e);
        }

        if self.kv_engine.persist(did, tid, collection, key) {
            self.note_kv_write_lsn(task, did, tid, collection, key);
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    /// Decide the stored row a TTL mutation targets against the write policy.
    ///
    /// An absent key mutates nothing, so there is no image to decide and the
    /// handler goes on to report `NotFound` on its own.
    fn admit_kv_ttl_target(&self, target: &KvTtlTarget<'_>, now_ms: u64) -> crate::Result<()> {
        let KvTtlTarget {
            did,
            tid,
            collection,
            key,
            rls_write_check,
        } = *target;
        if matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            return Ok(());
        }
        let Some(body) = self.kv_engine.get(did, tid, collection, key, now_ms) else {
            return Ok(());
        };
        super::rls::admit_kv_row(rls_write_check, &body, key, tid, collection)
    }

    pub(in crate::data::executor) fn execute_kv_get_ttl(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv get_ttl");
        let now_ms = current_ms();

        // Read-your-own-writes: an in-transaction GET_TTL consults this
        // transaction's staging overlay -- both the staged VALUE (for
        // tombstone / fresh-put visibility) and the staged KV TTL delta
        // (`StagedTtl`, populated by staged `Expire` / `Persist` / a
        // TTL-carrying `Incr` / `BatchPut`) -- before falling back to the
        // base KV engine.
        if let Some(txn_id) = task.request.txn_id {
            // Read-your-own-writes refreshes the lease (see the reaper).
            self.touch_overlay(txn_id);
            let coll_key = (
                task.request.database_id,
                TenantId::new(tid),
                collection.to_string(),
            );
            let doc_id = kv_row_identity(key);
            if let Some(overlay) = self.txn_overlays.get(&txn_id) {
                let staged_value = overlay.get_by_doc_id(&coll_key, &doc_id);
                if matches!(staged_value, Some(Staged::Tombstone)) {
                    return self.kv_get_ttl_response(task, -2);
                }
                let staged_ttl = overlay.get_ttl_by_doc_id(&coll_key, &doc_id);
                match staged_ttl {
                    Some(StagedTtl::ExpireAt(expire_at_ms)) => {
                        let ttl_ms = if expire_at_ms <= now_ms {
                            -2 // Already expired: staged-absent.
                        } else {
                            (expire_at_ms - now_ms) as i64
                        };
                        return self.kv_get_ttl_response(task, ttl_ms);
                    }
                    Some(StagedTtl::Persist) => return self.kv_get_ttl_response(task, -1),
                    None => {
                        if matches!(staged_value, Some(Staged::Put(_))) {
                            // A fresh staged put with no TTL delta is
                            // persistent, matching a base PUT with
                            // `ttl_ms == 0`.
                            return self.kv_get_ttl_response(task, -1);
                        }
                        // Nothing staged for this key: fall through to base.
                    }
                }
            }
        }

        let ttl_ms = self
            .kv_engine
            .get_ttl_ms(did, tid, collection, key, now_ms)
            .unwrap_or(-2); // -2 = key does not exist.
        self.kv_get_ttl_response(task, ttl_ms)
    }

    fn kv_get_ttl_response(&self, task: &ExecutionTask, ttl_ms: i64) -> Response {
        match response_codec::encode_json_as_msgpack(&serde_json::json!({ "ttl_ms": ttl_ms })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
