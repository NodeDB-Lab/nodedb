// SPDX-License-Identifier: BUSL-1.1

//! Formatting helpers for WebSocket RPC responses and notifications.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::control::change_stream::ChangeEvent;
use crate::control::gateway::GatewayErrorMap;

/// Format a live query notification as JSON.
pub fn format_live_notification(sub_id: u64, event: &ChangeEvent) -> String {
    serde_json::json!({
        "method": "live",
        "params": {
            "subscription_id": sub_id,
            "collection": event.collection,
            "operation": event.operation.as_str(),
            "document_id": event.document_id,
            "timestamp_ms": event.timestamp_ms,
        }
    })
    .to_string()
}

/// Format a consistent JSON-RPC error response (always includes `id`).
pub fn error_response(id: serde_json::Value, message: &str) -> String {
    serde_json::json!({"id": id, "error": message}).to_string()
}

/// Format a WS error frame using the gateway error mapping.
pub fn ws_error_from_gateway(id: &serde_json::Value, err: &crate::Error) -> String {
    let (_status, msg) = GatewayErrorMap::to_http(err);
    error_response(id.clone(), &msg)
}

/// Extract collection name from SQL (first word after FROM, case-insensitive).
pub fn extract_collection_from_sql(sql: &str) -> String {
    find_ascii_case_insensitive(sql, " FROM ")
        .and_then(|pos| sql.get(pos + 6..))
        .and_then(|after| after.split_whitespace().next())
        .map(|s| s.to_lowercase())
        .unwrap_or_default()
}
