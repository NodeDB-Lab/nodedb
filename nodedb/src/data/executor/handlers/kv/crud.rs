//! KV Get, Put, Delete, Truncate handlers.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

/// Parameters for `INSERT ... ON CONFLICT (key) DO UPDATE SET ...` on KV.
pub(in crate::data::executor) struct KvInsertOnConflictUpdateParams<'a> {
    pub tid: u32,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub updates: &'a [(String, crate::bridge::physical_plan::UpdateValue)],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_get(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        rls_filters: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv get");
        let now_ms = current_ms();
        match self.kv_engine.get(tid, collection, key, now_ms) {
            Some(value) => {
                // RLS post-fetch: evaluate filters against KV value.
                if !crate::data::executor::handlers::rls_eval::rls_check_msgpack_bytes(
                    rls_filters,
                    &value,
                ) {
                    return self.response_with_payload(task, Vec::new());
                }
                if let Some(ref m) = self.metrics {
                    m.record_kv_get();
                }
                // Value is already standard msgpack — pass through directly.
                self.response_with_payload(task, value)
            }
            None => self.response_with_payload(task, Vec::new()),
        }
    }

    pub(in crate::data::executor) fn execute_kv_put(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
    ) -> Response {
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

        let now_ms = current_ms();
        let _old = self
            .kv_engine
            .put(tid, collection, key, value, ttl_ms, now_ms);
        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }

        // Emit write event to Event Plane.
        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Insert,
            &key_str,
            Some(value),
            None,
        );

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
        tid: u32,
        collection: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv insert");

        if self.kv_engine.is_over_budget() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "KV memory budget exceeded, retry later".into(),
                },
            );
        }

        let now_ms = current_ms();
        if self.kv_engine.get(tid, collection, key, now_ms).is_some() {
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

        let _old = self
            .kv_engine
            .put(tid, collection, key, value, ttl_ms, now_ms);
        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }

        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Insert,
            &key_str,
            Some(value),
            None,
        );

        self.response_ok(task)
    }

    /// SQL `INSERT ... ON CONFLICT DO NOTHING` semantics: write if absent,
    /// silent no-op on duplicate. No error on conflict.
    pub(in crate::data::executor) fn execute_kv_insert_if_absent(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv insert-if-absent");

        if self.kv_engine.is_over_budget() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "KV memory budget exceeded, retry later".into(),
                },
            );
        }

        let now_ms = current_ms();
        if self.kv_engine.get(tid, collection, key, now_ms).is_some() {
            // Silent skip — matches the strict/schemaless `if_absent` path.
            return self.response_ok(task);
        }

        let _old = self
            .kv_engine
            .put(tid, collection, key, value, ttl_ms, now_ms);
        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }

        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Insert,
            &key_str,
            Some(value),
            None,
        );

        self.response_ok(task)
    }

    /// SQL `INSERT ... ON CONFLICT (key) DO UPDATE SET ...` semantics.
    /// Read-modify-write: if the key is absent, plain put; if present,
    /// decode the stored value, apply the updates (with `EXCLUDED`
    /// resolving to the would-be-inserted row), and write the merged
    /// result back.
    pub(in crate::data::executor) fn execute_kv_insert_on_conflict_update(
        &mut self,
        task: &ExecutionTask,
        params: KvInsertOnConflictUpdateParams<'_>,
    ) -> Response {
        let KvInsertOnConflictUpdateParams {
            tid,
            collection,
            key,
            value,
            ttl_ms,
            updates,
        } = params;
        debug!(core = self.core_id, %collection, "kv insert-on-conflict-update");

        if self.kv_engine.is_over_budget() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "KV memory budget exceeded, retry later".into(),
                },
            );
        }

        let now_ms = current_ms();
        let existing_bytes = self.kv_engine.get(tid, collection, key, now_ms);

        let stored_bytes: Vec<u8> = match existing_bytes {
            None => value.to_vec(),
            Some(existing_raw) => {
                let existing_val = match nodedb_types::value_from_msgpack(&existing_raw) {
                    Ok(v) => v,
                    Err(_) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to decode existing KV value for ON CONFLICT \
                                         DO UPDATE"
                                    .into(),
                            },
                        );
                    }
                };
                let excluded_val = match nodedb_types::value_from_msgpack(value) {
                    Ok(v) => v,
                    Err(_) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to decode incoming KV value for ON CONFLICT \
                                         DO UPDATE"
                                    .into(),
                            },
                        );
                    }
                };
                let merged = super::super::upsert::apply_on_conflict_updates(
                    existing_val,
                    &excluded_val,
                    updates,
                );
                match nodedb_types::value_to_msgpack(&merged) {
                    Ok(b) => b,
                    Err(_) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to encode merged KV value".into(),
                            },
                        );
                    }
                }
            }
        };

        let _old = self
            .kv_engine
            .put(tid, collection, key, &stored_bytes, ttl_ms, now_ms);
        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }

        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Insert,
            &key_str,
            Some(&stored_bytes),
            None,
        );

        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_kv_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        keys: &[Vec<u8>],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = keys.len(), "kv delete");
        let now_ms = current_ms();
        let count = self.kv_engine.delete(tid, collection, keys, now_ms);
        if let Some(ref m) = self.metrics {
            m.record_kv_delete();
        }

        // Emit delete events to Event Plane (one per deleted key).
        if count > 0 {
            for key in keys {
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Delete,
                    &key_str,
                    None,
                    None,
                );
            }
        }

        match response_codec::encode_count("deleted", count) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_truncate(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv truncate");
        let count = self.kv_engine.truncate(tid, collection);
        match response_codec::encode_count("deleted", count) {
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
