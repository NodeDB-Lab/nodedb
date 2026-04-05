//! KV field-level operation handlers: FieldGet, FieldSet.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_field_get(
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
        let doc: serde_json::Value = match nodedb_types::json_from_msgpack(&value) {
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

    pub(in crate::data::executor) fn execute_kv_field_set(
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
            .and_then(|v| nodedb_types::json_from_msgpack(v).ok())
            .and_then(|v: serde_json::Value| v.as_object().cloned())
            .unwrap_or_default();

        // Merge field updates, tracking how many fields are new (not previously existing).
        let mut fields_added = 0u64;
        for (field, value_bytes) in updates {
            let new_value: serde_json::Value = serde_json::from_slice(value_bytes).unwrap_or(
                serde_json::Value::String(String::from_utf8_lossy(value_bytes).into_owned()),
            );
            if !doc.contains_key(field) {
                fields_added += 1;
            }
            doc.insert(field.clone(), new_value);
        }

        // Serialize back to MessagePack and write.
        let new_value = match nodedb_types::json_to_msgpack(&serde_json::Value::Object(doc)) {
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
        let payload = serde_json::json!({ "fields_added": fields_added })
            .to_string()
            .into_bytes();
        self.response_with_payload(task, payload)
    }
}
