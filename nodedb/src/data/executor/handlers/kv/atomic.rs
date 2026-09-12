// SPDX-License-Identifier: BUSL-1.1

//! KV atomic operation handlers: Incr, IncrFloat, Cas, GetSet.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::AtomicError;
use crate::engine::kv::current_ms;

/// Shared identity context for a single-key KV atomic operation
/// (INCR_FLOAT / GETSET) dispatched to this core.
pub(in crate::data::executor) struct KvAtomicCtx<'a> {
    pub(in crate::data::executor) task: &'a ExecutionTask,
    pub(in crate::data::executor) did: u64,
    pub(in crate::data::executor) tid: u64,
    pub(in crate::data::executor) collection: &'a str,
    pub(in crate::data::executor) key: &'a [u8],
    pub(in crate::data::executor) surrogate: nodedb_types::Surrogate,
    /// Compiled row-level-security WRITE predicate from the plan. Every
    /// handler reading this field decides the image it is about to persist
    /// against it.
    pub(in crate::data::executor) rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_incr(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        delta: i64,
        ttl_ms: u64,
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        debug!(core = self.core_id, %collection, delta, "kv incr");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        // `Incr` carries a TTL that installs a new absolute `expire_at_ms`
        // when `ttl_ms > 0` (see `atomic_put`), so the live-installed instant
        // must be the same one `wal_append_kv_op` resolved and recorded —
        // see `CoreLoop::kv_ttl_now_ms` for the precedence this resolves.
        let now_ms: u64 = self.kv_ttl_now_ms(task);
        // The engine computes the post-image and installs it in one pass, so
        // the write policy is handed in and decided on the computed bytes
        // rather than on a duplicate of the increment arithmetic out here.
        let admit =
            |image: &[u8]| super::rls::admit_kv_row(rls_write_check, image, key, tid, collection);
        match self.kv_engine.incr(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            delta,
            ttl_ms,
            &admit,
        ) {
            Ok(new_value) => {
                if let Some(ref m) = self.metrics {
                    m.record_kv_put();
                }
                let new_bytes = zerompk::to_msgpack_vec(&new_value).unwrap_or_default();
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Update,
                    crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
                    Some(&new_bytes),
                    None,
                );
                self.note_kv_write_lsn(task, did, tid, collection, key);
                match response_codec::encode_json_as_msgpack(
                    &serde_json::json!({ "value": new_value }),
                ) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(AtomicError::TypeMismatch { detail }) => self.response_error(
                task,
                ErrorCode::TypeMismatch {
                    collection: collection.to_string(),
                    detail,
                },
            ),
            Err(AtomicError::Overflow) => self.response_error(
                task,
                ErrorCode::OverflowError {
                    collection: collection.to_string(),
                },
            ),
            Err(AtomicError::Encode { detail }) => {
                self.response_error(task, ErrorCode::Internal { detail })
            }
            // Nothing was written: the engine consults the gate before it
            // installs the computed value.
            Err(AtomicError::Rejected(error)) => self.response_error(task, *error),
        }
    }

    pub(in crate::data::executor) fn execute_kv_incr_float(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        delta: f64,
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        debug!(core = self.core_id, %collection, delta, "kv incr_float");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        // Same engine-internal compute-and-persist as `Incr` — see there.
        let admit =
            |image: &[u8]| super::rls::admit_kv_row(rls_write_check, image, key, tid, collection);
        match self.kv_engine.incr_float(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            delta,
            &admit,
        ) {
            Ok(new_value) => {
                if let Some(ref m) = self.metrics {
                    m.record_kv_put();
                }
                let new_bytes = zerompk::to_msgpack_vec(&new_value).unwrap_or_default();
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Update,
                    crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
                    Some(&new_bytes),
                    None,
                );
                self.note_kv_write_lsn(task, did, tid, collection, key);
                match response_codec::encode_json_as_msgpack(
                    &serde_json::json!({ "value": new_value }),
                ) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(AtomicError::TypeMismatch { detail }) => self.response_error(
                task,
                ErrorCode::TypeMismatch {
                    collection: collection.to_string(),
                    detail,
                },
            ),
            Err(AtomicError::Overflow) => self.response_error(
                task,
                ErrorCode::OverflowError {
                    collection: collection.to_string(),
                },
            ),
            Err(AtomicError::Encode { detail }) => {
                self.response_error(task, ErrorCode::Internal { detail })
            }
            // Nothing was written: the engine consults the gate before it
            // installs the computed value.
            Err(AtomicError::Rejected(error)) => self.response_error(task, *error),
        }
    }

    pub(in crate::data::executor) fn execute_kv_cas(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        expected: &[u8],
        new_value: &[u8],
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        debug!(core = self.core_id, %collection, "kv cas");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        // `new_value` is caller-supplied, so the row that would exist after a
        // successful swap is known before the engine is entered — decided here
        // rather than after the fact.
        if let Err(e) = super::rls::admit_kv_row(rls_write_check, new_value, key, tid, collection) {
            return self.response_error(task, e);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        let result = self.kv_engine.cas(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            expected,
            new_value,
        );

        if result.success {
            if let Some(ref m) = self.metrics {
                m.record_kv_put();
            }
            let key_str = String::from_utf8_lossy(key);
            self.emit_write_event(
                task,
                collection,
                crate::event::WriteOp::Update,
                crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
                Some(new_value),
                None,
            );
            self.note_kv_write_lsn(task, did, tid, collection, key);
        }

        let current_b64 = result
            .current_value
            .as_ref()
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        match response_codec::encode_json_as_msgpack(&serde_json::json!({
            "success": result.success,
            "current_value": current_b64,
        })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// `rls_filters` decides the OLD value handed back: `GETSET` is a read as
    /// much as a write, so a row the read policy hides must come back absent
    /// rather than being disclosed by the write that replaced it. The write
    /// half is a separate decision on `new_value`.
    pub(in crate::data::executor) fn execute_kv_getset(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        new_value: &[u8],
        rls_filters: &[u8],
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        debug!(core = self.core_id, %collection, "kv getset");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        // The stored row is replaced wholesale, so the post-image is known
        // before the engine call.
        if let Err(e) = super::rls::admit_kv_row(rls_write_check, new_value, key, tid, collection) {
            return self.response_error(task, e);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        let old = self.kv_engine.getset(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            new_value,
        );

        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }
        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Update,
            crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
            Some(new_value),
            old.as_deref(),
        );
        self.note_kv_write_lsn(task, did, tid, collection, key);

        // A row the read policy excludes is reported exactly as an absent row,
        // the same convention `execute_kv_get` uses — the caller cannot tell it
        // apart from a key that never existed, so the reply discloses nothing.
        // A filter that fails to evaluate withholds the value too: an old value
        // the policy could not be decided against is not one it cleared.
        let disclosable_old = match &old {
            Some(bytes) => match self.row_passes_rls(bytes, rls_filters) {
                Ok(true) => old.as_deref(),
                Ok(false) => None,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            },
            None => None,
        };

        let old_b64 = disclosable_old
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        match response_codec::encode_json_as_msgpack(&serde_json::json!({ "old_value": old_b64 })) {
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
