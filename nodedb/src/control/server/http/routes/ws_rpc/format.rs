// SPDX-License-Identifier: BUSL-1.1

//! Formatting helpers for WebSocket RPC responses and notifications.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::control::change_stream::{ChangeCursor, SequencedChangeEvent};
use crate::control::gateway::GatewayErrorMap;
use crate::control::server::shared::statement_exec::RequestError;

use super::super::ndjson_error::statement_error_value;

/// Format a cursor-aware LIVE SELECT notification. `cursor` resumes right
/// after the event.
pub fn format_sequenced_live_notification(
    sub_id: u64,
    event: &SequencedChangeEvent,
    cursor: &ChangeCursor,
) -> String {
    serde_json::json!({
        "method": "live",
        "params": {
            "subscription_id": sub_id,
            "cursor": cursor.to_string(),
            "wal_lsn": event.lsn.as_u64(),
            "database_id": event.database_id().as_u64(),
            "collection": event.collection,
            "operation": event.operation.as_str(),
            "document_id": event.document_id.as_str(),
            "timestamp_ms": event.timestamp_ms,
        }
    })
    .to_string()
}

/// Format a cursor-aware connection resume notification. `cursor` resumes
/// right after the event.
pub fn format_resume_notification(event: &SequencedChangeEvent, cursor: &ChangeCursor) -> String {
    serde_json::json!({"method": "change", "params": {
        "cursor": cursor.to_string(), "wal_lsn": event.lsn.as_u64(),
        "database_id": event.database_id().as_u64(), "collection": event.collection, "operation": event.operation.as_str(),
        "document_id": event.document_id.as_str(), "timestamp_ms": event.timestamp_ms,
    }})
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

/// Format the WS error frame of a `query` request. `sql` is the request text.
///
/// An error with no statement answers [`ws_error_from_gateway`]. An error of
/// statement `k` answers `code` (`NDB-<n>`), `sqlstate`, `statement` and
/// `position`.
pub(crate) fn ws_error_from_request(
    id: &serde_json::Value,
    err: &RequestError,
    sql: &str,
) -> String {
    if err.at.is_none() {
        return ws_error_from_gateway(id, &err.error);
    }
    let mut frame = statement_error_value(&err.error, err.at, sql);
    if let Some(object) = frame.as_object_mut() {
        object.insert("id".to_owned(), id.clone());
    }
    frame.to_string()
}

/// Extract collection name from SQL (first word after FROM, case-insensitive).
pub fn extract_collection_from_sql(sql: &str) -> String {
    find_ascii_case_insensitive(sql, " FROM ")
        .and_then(|pos| sql.get(pos + 6..))
        .and_then(|after| after.split_whitespace().next())
        .map(|s| s.to_lowercase())
        .unwrap_or_default()
}
