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
