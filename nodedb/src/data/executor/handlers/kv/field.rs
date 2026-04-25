//! KV field-level operation handlers: FieldGet, FieldSet.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
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

        // Decode as standard msgpack map.
        let doc = match nodedb_types::json_from_msgpack(&value) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "value is not a msgpack-encoded object".into(),
                    },
                );
            }
        };

        // Extract requested fields.
        let mut result = serde_json::Map::new();
        for f in fields {
            let v = doc.get(f).cloned().unwrap_or(serde_json::Value::Null);
            result.insert(f.clone(), v);
        }

        match response_codec::encode_json(&serde_json::Value::Object(result)) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
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

        // Decode current value as standard msgpack map.
        let mut doc: serde_json::Map<String, serde_json::Value> = current
            .as_ref()
            .and_then(|v| nodedb_types::json_from_msgpack(v).ok())
            .and_then(|v| {
                if let serde_json::Value::Object(m) = v {
                    Some(m)
                } else {
                    None
                }
            })
            .unwrap_or_default();

        // Merge field updates, tracking how many fields are new (not previously existing).
        let mut fields_added = 0u64;
        for (field, value_bytes) in updates {
            let new_value = if value_bytes.is_empty() {
                serde_json::Value::Null
            } else {
                match nodedb_types::json_from_msgpack(value_bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("field set '{field}': msgpack decode: {e}"),
                            },
                        );
                    }
                }
            };
            if !doc.contains_key(field) {
                fields_added += 1;
            }
            doc.insert(field.clone(), new_value);
        }

        // Serialize back as standard msgpack (not zerompk tagged).
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

        self.kv_engine.put(
            tid,
            collection,
            key,
            &new_value,
            0,
            now_ms,
            nodedb_types::Surrogate::ZERO,
        );
        match response_codec::encode_json(&serde_json::json!({ "fields_added": fields_added })) {
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
