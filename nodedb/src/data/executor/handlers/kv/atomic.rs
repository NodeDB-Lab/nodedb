//! KV atomic operation handlers: Incr, IncrFloat, Cas, GetSet.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::AtomicError;
use crate::engine::kv::current_ms;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_incr(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        delta: i64,
        ttl_ms: u64,
    ) -> Response {
        debug!(core = self.core_id, %collection, delta, "kv incr");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();
        match self
            .kv_engine
            .incr(tid, collection, key, delta, ttl_ms, now_ms)
        {
            Ok(new_value) => {
                if let Some(ref m) = self.metrics {
                    m.record_kv_put();
                }
                let new_bytes = rmp_serde::to_vec(&new_value).unwrap_or_default();
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Update,
                    &key_str,
                    Some(&new_bytes),
                    None,
                );
                let payload = serde_json::json!({ "value": new_value })
                    .to_string()
                    .into_bytes();
                self.response_with_payload(task, payload)
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
        }
    }

    pub(in crate::data::executor) fn execute_kv_incr_float(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        delta: f64,
    ) -> Response {
        debug!(core = self.core_id, %collection, delta, "kv incr_float");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();
        match self
            .kv_engine
            .incr_float(tid, collection, key, delta, now_ms)
        {
            Ok(new_value) => {
                if let Some(ref m) = self.metrics {
                    m.record_kv_put();
                }
                let new_bytes = rmp_serde::to_vec(&new_value).unwrap_or_default();
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Update,
                    &key_str,
                    Some(&new_bytes),
                    None,
                );
                let payload = serde_json::json!({ "value": new_value })
                    .to_string()
                    .into_bytes();
                self.response_with_payload(task, payload)
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
        }
    }

    pub(in crate::data::executor) fn execute_kv_cas(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        expected: &[u8],
        new_value: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv cas");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();
        let result = self
            .kv_engine
            .cas(tid, collection, key, expected, new_value, now_ms);

        if result.success {
            if let Some(ref m) = self.metrics {
                m.record_kv_put();
            }
            let key_str = String::from_utf8_lossy(key);
            self.emit_write_event(
                task,
                collection,
                crate::event::WriteOp::Update,
                &key_str,
                Some(new_value),
                None,
            );
        }

        let current_b64 = result
            .current_value
            .as_ref()
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        let payload = serde_json::json!({
            "success": result.success,
            "current_value": current_b64,
        })
        .to_string()
        .into_bytes();
        self.response_with_payload(task, payload)
    }

    pub(in crate::data::executor) fn execute_kv_getset(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        new_value: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv getset");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();
        let old = self
            .kv_engine
            .getset(tid, collection, key, new_value, now_ms);

        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }
        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Update,
            &key_str,
            Some(new_value),
            old.as_deref(),
        );

        let old_b64 = old
            .as_ref()
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        let payload = serde_json::json!({ "old_value": old_b64 })
            .to_string()
            .into_bytes();
        self.response_with_payload(task, payload)
    }
}
