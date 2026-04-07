//! KV Scan handler and filter extraction.

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_kv_scan(
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

        // Parse filter predicates for post-scan evaluation.
        // Index pushdown handles eq filters on indexed fields, but general
        // predicates (gt, lt, in, etc.) need post-scan evaluation.
        let filter_predicates: Vec<crate::bridge::scan_filter::ScanFilter> = if !filters.is_empty()
        {
            zerompk::from_msgpack(filters).unwrap_or_default()
        } else {
            Vec::new()
        };

        let mut results = Vec::with_capacity(entries.len());
        for (k, v) in &entries {
            let key_str = String::from_utf8_lossy(k).to_string();
            let json: serde_json::Value = nodedb_types::value_from_msgpack(v)
                .ok()
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null);
            let obj = if let serde_json::Value::Object(mut map) = json {
                map.entry("key".to_string())
                    .or_insert(serde_json::Value::String(key_str));
                serde_json::Value::Object(map)
            } else {
                serde_json::json!({"key": key_str, "value": json})
            };

            // Apply filter predicates post-scan.
            if !filter_predicates.is_empty() {
                let mp = super::super::super::doc_format::encode_to_msgpack(&obj);
                if !filter_predicates.iter().all(|f| f.matches_binary(&mp)) {
                    continue;
                }
            }

            results.push(obj);
        }
        // Wrap results with cursor for pagination support.
        let response_obj = if next_cursor.is_empty() {
            serde_json::json!({ "entries": results })
        } else {
            use base64::Engine;
            let cursor_b64 = base64::engine::general_purpose::STANDARD.encode(&next_cursor);
            serde_json::json!({ "entries": results, "next_cursor": cursor_b64 })
        };
        let payload = match super::super::super::response_codec::encode_json(&response_obj) {
            Ok(p) => p,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        if let Some(ref m) = self.metrics {
            m.record_kv_scan();
        }
        self.response_with_payload(task, payload)
    }
}

/// Extract a single equality filter from serialized ScanFilter bytes.
///
/// Looks for the first `{"field": "x", "op": "eq", "value": "y"}` filter.
/// Returns `(Some(field), Some(value_bytes))` if found, `(None, None)` otherwise.
pub(in crate::data::executor) fn extract_eq_filter(
    filters: &[u8],
) -> (Option<String>, Option<Vec<u8>>) {
    if filters.is_empty() {
        return (None, None);
    }

    // Filters are MessagePack-encoded Vec<ScanFilter>.
    let Ok(parsed) = zerompk::from_msgpack::<Vec<nodedb_types::json_msgpack::JsonValue>>(filters)
        .map(|v| {
            v.into_iter()
                .map(|jv| jv.0)
                .collect::<Vec<serde_json::Value>>()
        })
    else {
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
