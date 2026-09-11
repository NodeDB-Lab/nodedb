// SPDX-License-Identifier: BUSL-1.1

//! KV point-write handlers: UPSERT-style `PUT`, SQL `INSERT` (unique
//! violation on duplicate), and `INSERT ... ON CONFLICT DO NOTHING`.

use tracing::debug;

use super::types::KvWriteParams;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_put(
        &mut self,
        task: &ExecutionTask,
        params: KvWriteParams<'_>,
    ) -> Response {
        let KvWriteParams {
            did,
            tid,
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            returning,
            rls_filters,
        } = params;
        debug!(core = self.core_id, %collection, "kv put");

        // Memory budget check: reject new PUTs when over budget.
        if self.kv_engine.is_over_budget() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "KV memory budget exceeded, retry later".into(),
                },
            );
        }

        // See `CoreLoop::kv_ttl_now_ms` for the precedence this resolves.
        let now_ms: u64 = self.kv_ttl_now_ms(task);
        let old = self.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: did,
            tenant_id: tid,
            collection,
            key,
            value,
            ttl_ms,
            now_ms,
            surrogate,
        });
        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }

        // RESP SET / UPSERT semantics: if `old` is present the write
        // replaced an existing row and the Event Plane must see an Update
        // event with both sides populated. Otherwise it was a fresh
        // Insert.
        let key_str = String::from_utf8_lossy(key);
        let (op, old_slice): (_, Option<&[u8]>) = match old.as_deref() {
            Some(o) => (crate::event::WriteOp::Update, Some(o)),
            None => (crate::event::WriteOp::Insert, None),
        };
        self.emit_write_event(
            task,
            collection,
            op,
            crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
            Some(value),
            old_slice,
        );

        self.note_kv_write_lsn(task, did, tid, collection, key);
        if let Some(spec) = returning {
            // `value` IS the stored body on this path: the put writes the
            // caller's bytes verbatim, so projecting them is projecting the
            // stored post-image, not an echo of the request.
            return self.kv_stored_returning_response(task, spec, rls_filters, &[(key, value)]);
        }
        self.response_ok(task)
    }

    /// SQL `INSERT` semantics: write only if key doesn't already exist.
    /// Duplicate raises `unique_violation` (SQLSTATE 23505). Distinct from
    /// `execute_kv_put` which is RESP-SET / UPSERT upsert semantics.
    ///
    /// Existence probe is linearizable with the subsequent put: KV shards
    /// are pinned to one Data Plane core (vshard routing), and the core
    /// loop runs ops serially — no other writer can slip between the
    /// probe and the put on the same key.
    pub(in crate::data::executor) fn execute_kv_insert(
        &mut self,
        task: &ExecutionTask,
        params: KvWriteParams<'_>,
    ) -> Response {
        let KvWriteParams {
            did,
            tid,
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            returning,
            rls_filters,
        } = params;
        debug!(core = self.core_id, %collection, "kv insert");

        if self.kv_engine.is_over_budget() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "KV memory budget exceeded, retry later".into(),
                },
            );
        }

        // See `CoreLoop::kv_ttl_now_ms` for the precedence this resolves.
        let now_ms = self.kv_ttl_now_ms(task);
        if self
            .kv_engine
            .get(did, tid, collection, key, now_ms)
            .is_some()
        {
            let key_str = String::from_utf8_lossy(key);
            return self.response_error(
                task,
                crate::Error::RejectedConstraint {
                    collection: collection.to_string(),
                    constraint: "unique".to_string(),
                    detail: format!(
                        "duplicate key value '{key_str}' violates primary-key \
                         uniqueness on '{collection}'"
                    ),
                },
            );
        }

        self.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: did,
            tenant_id: tid,
            collection,
            key,
            value,
            ttl_ms,
            now_ms,
            surrogate,
        });
        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }

        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Insert,
            crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
            Some(value),
            None,
        );

        self.note_kv_write_lsn(task, did, tid, collection, key);
        if let Some(spec) = returning {
            return self.kv_stored_returning_response(task, spec, rls_filters, &[(key, value)]);
        }
        self.response_ok(task)
    }

    /// SQL `INSERT ... ON CONFLICT DO NOTHING` semantics: write if absent,
    /// silent no-op on duplicate. No error on conflict.
    pub(in crate::data::executor) fn execute_kv_insert_if_absent(
        &mut self,
        task: &ExecutionTask,
        params: KvWriteParams<'_>,
    ) -> Response {
        let KvWriteParams {
            did,
            tid,
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            returning,
            rls_filters,
        } = params;
        debug!(core = self.core_id, %collection, "kv insert-if-absent");

        if self.kv_engine.is_over_budget() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "KV memory budget exceeded, retry later".into(),
                },
            );
        }

        // See `CoreLoop::kv_ttl_now_ms` for the precedence this resolves.
        let now_ms = self.kv_ttl_now_ms(task);
        if self
            .kv_engine
            .get(did, tid, collection, key, now_ms)
            .is_some()
        {
            // Key already present: `ON CONFLICT DO NOTHING` inserts nothing, so
            // it reports 0 rows — matches the strict/schemaless `if_absent`
            // path, which reports 0 for the same reason.
            //
            // A `RETURNING` on the same statement ships an EMPTY row set rather
            // than the count shape: nothing was written, so there is no
            // post-image to project, and a count payload would be decoded as a
            // row set of the wrong shape by the RETURNING renderer.
            if let Some(spec) = returning {
                return self.kv_stored_returning_response(task, spec, rls_filters, &[]);
            }
            return self.response_affected(task, 0);
        }

        self.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: did,
            tenant_id: tid,
            collection,
            key,
            value,
            ttl_ms,
            now_ms,
            surrogate,
        });
        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }

        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Insert,
            crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
            Some(value),
            None,
        );

        self.note_kv_write_lsn(task, did, tid, collection, key);
        if let Some(spec) = returning {
            return self.kv_stored_returning_response(task, spec, rls_filters, &[(key, value)]);
        }
        self.response_affected(task, 1)
    }

    /// Record a committed KV point write's version, keyed by the raw KV key
    /// bytes, if a WAL LSN was threaded onto the task. Shared by every KV write
    /// chokepoint (basic put/delete, atomic ops, batch, field, TTL).
    pub(in crate::data::executor) fn note_kv_write_lsn(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
    ) {
        let Some(lsn) = task.wal_lsn() else {
            return;
        };
        self.note_write_lsn(
            crate::types::DatabaseId::new(did),
            crate::types::TenantId::new(tid),
            collection,
            Some(crate::data::executor::core_loop::write_index::KeyRepr::KvKey(Box::from(key))),
            lsn,
        );
    }
}
