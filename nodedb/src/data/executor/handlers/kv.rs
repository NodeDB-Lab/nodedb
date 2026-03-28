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
            KvOp::Get {
                collection,
                key,
                rls_filters,
            } => self.execute_kv_get(task, tid, collection, key, rls_filters),
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
                filters,
                match_pattern,
            } => self.execute_kv_scan(
                task,
                tid,
                collection,
                cursor,
                *count,
                match_pattern.as_deref(),
                filters,
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
            KvOp::RegisterIndex {
                collection,
                field,
                field_position,
                backfill,
            } => self.execute_kv_register_index(
                task,
                tid,
                collection,
                field,
                *field_position,
                *backfill,
            ),
            KvOp::DropIndex { collection, field } => {
                self.execute_kv_drop_index(task, tid, collection, field)
            }
            KvOp::FieldGet {
                collection,
                key,
                fields,
            } => self.execute_kv_field_get(task, tid, collection, key, fields),
            KvOp::FieldSet {
                collection,
                key,
                updates,
            } => self.execute_kv_field_set(task, tid, collection, key, updates),
            KvOp::Truncate { collection } => self.execute_kv_truncate(task, tid, collection),
        }
    }

    fn execute_kv_get(
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
                if !super::rls_eval::rls_check_msgpack_bytes(rls_filters, &value) {
                    return self.response_error(task, ErrorCode::NotFound);
                }
                self.response_with_payload(task, value)
            }
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
        self.response_ok(task)
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_kv_scan(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        cursor: &[u8],
        count: usize,
        match_pattern: Option<&str>,
        filters: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, count, "kv scan");
        let now_ms = current_ms();

        // Try to extract a single equality filter for index pushdown.
        let (filter_field, filter_value) = extract_eq_filter(filters);
        let (entries, next_cursor) = self.kv_engine.scan(
            tid,
            collection,
            cursor,
            count,
            now_ms,
            match_pattern,
            filter_field.as_deref(),
            filter_value.as_deref(),
        );

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

    fn execute_kv_register_index(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
        field_position: usize,
        backfill: bool,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field, "kv register index");
        let now_ms = current_ms();
        let backfilled =
            self.kv_engine
                .register_index(tid, collection, field, field_position, backfill, now_ms);
        let payload = serde_json::json!({
            "index": field,
            "backfilled": backfilled,
            "write_amp_estimate": format!("{:.0}%", 15.0 + 10.0 * self.kv_engine.index_count(tid, collection) as f64),
        })
        .to_string()
        .into_bytes();
        self.response_with_payload(task, payload)
    }

    fn execute_kv_drop_index(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field, "kv drop index");
        let removed = self.kv_engine.drop_index(tid, collection, field);
        let payload = serde_json::json!({
            "index": field,
            "entries_removed": removed,
        })
        .to_string()
        .into_bytes();
        self.response_with_payload(task, payload)
    }

    fn execute_kv_field_get(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        fields: &[String],
    ) -> Response {
        debug!(core = self.core_id, %collection, field_count = fields.len(), "kv field get");
        let now_ms = current_ms();

        // Get the full value.
        let Some(value) = self.kv_engine.get(tid, collection, key, now_ms) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Deserialize as MessagePack → JSON.
        let doc: serde_json::Value = match rmp_serde::from_slice(&value) {
            Ok(v) => v,
            Err(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "value is not MessagePack-encoded; use GET for raw values".into(),
                    },
                );
            }
        };

        // Extract requested fields.
        let result: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .map(|f| {
                let v = doc.get(f).cloned().unwrap_or(serde_json::Value::Null);
                (f.clone(), v)
            })
            .collect();

        let payload = serde_json::to_vec(&result).unwrap_or_default();
        self.response_with_payload(task, payload)
    }

    fn execute_kv_field_set(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, field_count = updates.len(), "kv field set");
        let now_ms = current_ms();

        // Read current value.
        let current = self.kv_engine.get(tid, collection, key, now_ms);

        let mut doc: serde_json::Map<String, serde_json::Value> = current
            .as_ref()
            .and_then(|v| rmp_serde::from_slice(v).ok())
            .and_then(|v: serde_json::Value| v.as_object().cloned())
            .unwrap_or_default();

        // Merge field updates.
        for (field, value_bytes) in updates {
            let new_value: serde_json::Value = serde_json::from_slice(value_bytes).unwrap_or(
                serde_json::Value::String(String::from_utf8_lossy(value_bytes).into_owned()),
            );
            doc.insert(field.clone(), new_value);
        }

        // Serialize back to MessagePack and write.
        let new_value = match rmp_serde::to_vec(&serde_json::Value::Object(doc)) {
            Ok(v) => v,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("field set serialization: {e}"),
                    },
                );
            }
        };

        self.kv_engine
            .put(tid, collection, key, &new_value, 0, now_ms);
        self.response_ok(task)
    }

    fn execute_kv_truncate(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv truncate");
        let count = self.kv_engine.truncate(tid, collection);
        let payload = serde_json::json!({ "deleted": count })
            .to_string()
            .into_bytes();
        self.response_with_payload(task, payload)
    }
}

/// Extract a single equality filter from serialized ScanFilter bytes.
///
/// Looks for the first `{"field": "x", "op": "eq", "value": "y"}` filter.
/// Returns `(Some(field), Some(value_bytes))` if found, `(None, None)` otherwise.
fn extract_eq_filter(filters: &[u8]) -> (Option<String>, Option<Vec<u8>>) {
    if filters.is_empty() {
        return (None, None);
    }

    // Filters are MessagePack-encoded Vec<ScanFilter>.
    let Ok(parsed) = rmp_serde::from_slice::<Vec<serde_json::Value>>(filters) else {
        tracing::trace!(
            len = filters.len(),
            "filter deserialization failed, falling back to full scan"
        );
        return (None, None);
    };

    for filter in &parsed {
        let Some(field) = filter.get("field").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(op) = filter.get("op").and_then(|v| v.as_str()) else {
            continue;
        };
        if op != "eq" {
            continue;
        }
        let Some(value) = filter.get("value") else {
            continue;
        };

        let value_bytes = match value {
            serde_json::Value::String(s) => s.as_bytes().to_vec(),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    let sortable = (i as u64) ^ (1u64 << 63);
                    sortable.to_be_bytes().to_vec()
                } else {
                    n.to_string().into_bytes()
                }
            }
            other => other.to_string().into_bytes(),
        };

        return (Some(field.to_string()), Some(value_bytes));
    }

    (None, None)
}
