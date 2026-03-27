//! KV engine operation handlers for the Data Plane executor.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::KvOp;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    /// Dispatch a KV operation to the appropriate handler.
    pub(in crate::data::executor) fn execute_kv(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        op: &KvOp,
    ) -> Response {
        match op {
            KvOp::Get { collection, key } => self.execute_kv_get(task, tid, collection, key),
            KvOp::Put {
                collection,
                key,
                value,
                ttl_ms,
            } => self.execute_kv_put(task, tid, collection, key, value, *ttl_ms),
            KvOp::Delete { collection, keys } => {
                self.execute_kv_delete(task, tid, collection, keys)
            }
            KvOp::Scan {
                collection,
                cursor,
                count,
                filters: _,
                match_pattern,
            } => self.execute_kv_scan(
                task,
                tid,
                collection,
                cursor,
                *count,
                match_pattern.as_deref(),
            ),
            KvOp::Expire {
                collection,
                key,
                ttl_ms,
            } => self.execute_kv_expire(task, tid, collection, key, *ttl_ms),
            KvOp::Persist { collection, key } => {
                self.execute_kv_persist(task, tid, collection, key)
            }
            KvOp::BatchGet { collection, keys } => {
                self.execute_kv_batch_get(task, tid, collection, keys)
            }
            KvOp::BatchPut {
                collection,
                entries,
                ttl_ms,
            } => self.execute_kv_batch_put(task, tid, collection, entries, *ttl_ms),
        }
    }

    fn execute_kv_get(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv get");
        let now_ms = current_ms();
        match self.kv_engine.get(tid, collection, key, now_ms) {
            Some(value) => self.response_with_payload(task, value),
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    fn execute_kv_put(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv put");
        let now_ms = current_ms();
        let _old = self.kv_engine.put(
            tid,
            collection,
            key.to_vec(),
            value.to_vec(),
            ttl_ms,
            now_ms,
        );
        self.response_ok(task)
    }

    fn execute_kv_scan(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        cursor: &[u8],
        count: usize,
        match_pattern: Option<&str>,
    ) -> Response {
        debug!(core = self.core_id, %collection, count, "kv scan");
        let now_ms = current_ms();
        let (entries, next_cursor) =
            self.kv_engine
                .scan(tid, collection, cursor, count, now_ms, match_pattern);

        // Encode as JSON: { "cursor": "<base64>", "entries": [{"key":"...","value":"..."}] }
        let cursor_b64 = if next_cursor.is_empty() {
            "0".to_string()
        } else {
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &next_cursor)
        };
        let json_entries: Vec<serde_json::Value> = entries
            .iter()
            .map(|(k, v)| {
                serde_json::json!({
                    "key": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, k),
                    "value": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v),
                })
            })
            .collect();
        let payload = serde_json::json!({
            "cursor": cursor_b64,
            "count": json_entries.len(),
            "entries": json_entries,
        })
        .to_string()
        .into_bytes();
        self.response_with_payload(task, payload)
    }

    fn execute_kv_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        keys: &[Vec<u8>],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = keys.len(), "kv delete");
        let now_ms = current_ms();
        let count = self.kv_engine.delete(tid, collection, keys, now_ms);
        let payload = serde_json::json!({ "deleted": count })
            .to_string()
            .into_bytes();
        self.response_with_payload(task, payload)
    }

    fn execute_kv_expire(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        ttl_ms: u64,
    ) -> Response {
        debug!(core = self.core_id, %collection, ttl_ms, "kv expire");
        let now_ms = current_ms();
        if self.kv_engine.expire(tid, collection, key, ttl_ms, now_ms) {
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    fn execute_kv_persist(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv persist");
        if self.kv_engine.persist(tid, collection, key) {
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    fn execute_kv_batch_get(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        keys: &[Vec<u8>],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = keys.len(), "kv batch get");
        let now_ms = current_ms();
        let results = self.kv_engine.batch_get(tid, collection, keys, now_ms);

        // Serialize as JSON array: [value_or_null, value_or_null, ...]
        let json_results: Vec<serde_json::Value> = results
            .into_iter()
            .map(|opt| match opt {
                Some(v) => serde_json::Value::String(base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &v,
                )),
                None => serde_json::Value::Null,
            })
            .collect();
        let payload = serde_json::to_vec(&json_results).unwrap_or_default();
        self.response_with_payload(task, payload)
    }

    fn execute_kv_batch_put(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        entries: &[(Vec<u8>, Vec<u8>)],
        ttl_ms: u64,
    ) -> Response {
        debug!(core = self.core_id, %collection, count = entries.len(), "kv batch put");
        let now_ms = current_ms();
        let new_count = self
            .kv_engine
            .batch_put(tid, collection, entries, ttl_ms, now_ms);
        let payload = serde_json::json!({ "inserted": new_count })
            .to_string()
            .into_bytes();
        self.response_with_payload(task, payload)
    }
}
