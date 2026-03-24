//! WebSocket RPC endpoint.
//!
//! Accepts WebSocket connections at `/ws`. Clients send JSON requests
//! and receive JSON responses. Supports SQL query execution, live query
//! subscriptions, and ping/pong.
//!
//! Protocol:
//! ```json
//! // Request
//! {"id": 1, "method": "query", "params": {"sql": "SELECT * FROM users"}}
//! {"id": 2, "method": "ping"}
//! {"id": 3, "method": "live", "params": {"sql": "LIVE SELECT * FROM orders"}}
//!
//! // Response
//! {"id": 1, "result": [...]}
//! {"id": 2, "result": "pong"}
//!
//! // Live notification (server-initiated, no request id)
//! {"method": "live", "params": {"subscription_id": 1, "operation": "INSERT", "document_id": "d1", "collection": "orders"}}
//! ```

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use tracing::debug;

use super::super::auth::AppState;
use crate::control::change_stream::ChangeEvent;
use crate::control::state::SharedState;
use crate::types::TenantId;

/// WebSocket upgrade handler.
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state))
}

/// Handle a single WebSocket connection.
async fn handle_ws_connection(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let shared = Arc::clone(&state.shared);
    let tenant_id = TenantId::new(1); // Default tenant; auth can be added via first message
    let trace_id = crate::control::trace_context::generate_trace_id();

    // Bounded channel for live notifications → WS sender.
    // 256 messages provides ~10s of buffer at 25 events/sec. If the client
    // can't keep up, the send will block briefly then we close the connection.
    let (live_tx, mut live_rx) = tokio::sync::mpsc::channel::<String>(256);

    // Spawn a task that forwards live notifications to the WS sender.
    let send_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(msg) = live_rx.recv() => {
                    if sender.send(Message::Text(msg.into())).await.is_err() {
                        debug!("WebSocket send failed; closing connection");
                        break;
                    }
                }
                else => break,
            }
        }
    });

    // Process incoming messages.
    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(text) => {
                let response = process_message(
                    &shared,
                    &state.query_ctx,
                    tenant_id,
                    trace_id,
                    &text,
                    &live_tx,
                )
                .await;
                if let Err(e) = live_tx.send(response).await {
                    debug!("response channel closed: {e}; dropping connection");
                    break;
                }
            }
            Message::Close(_) => break,
            Message::Ping(_) => {
                if let Err(e) = live_tx
                    .send(serde_json::json!({"pong": true}).to_string())
                    .await
                {
                    debug!("pong send failed: {e}; dropping connection");
                    break;
                }
            }
            _ => {}
        }
    }

    // Cleanup.
    drop(live_tx);
    let _ = send_handle.await;
    debug!("WebSocket RPC connection closed");
}

/// Process a single JSON-RPC message and return the response.
async fn process_message(
    shared: &SharedState,
    query_ctx: &crate::control::planner::context::QueryContext,
    tenant_id: TenantId,
    trace_id: u64,
    text: &str,
    live_tx: &tokio::sync::mpsc::Sender<String>,
) -> String {
    let req: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            return error_response(serde_json::Value::Null, &format!("invalid JSON: {e}"));
        }
    };

    let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let method = req
        .get("method")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    match method {
        "ping" => serde_json::json!({"id": id, "result": "pong"}).to_string(),

        "query" => {
            let sql = req
                .get("params")
                .and_then(|p| p.get("sql"))
                .and_then(|s| s.as_str())
                .unwrap_or("");

            if sql.is_empty() {
                return error_response(id, "missing params.sql");
            }

            match execute_sql(shared, query_ctx, tenant_id, sql, trace_id).await {
                Ok(result) => serde_json::json!({"id": id, "result": result}).to_string(),
                Err(e) => error_response(id, &e),
            }
        }

        "live" => {
            let sql = req
                .get("params")
                .and_then(|p| p.get("sql"))
                .and_then(|s| s.as_str())
                .unwrap_or("");

            let collection = extract_collection_from_sql(sql);

            if collection.is_empty() {
                return error_response(id, "missing collection in LIVE SELECT");
            }

            let mut sub = shared
                .change_stream
                .subscribe(Some(collection.clone()), Some(tenant_id));
            let sub_id = sub.id;
            let live_tx = live_tx.clone();

            // Spawn background task to deliver live changes.
            tokio::spawn(async move {
                while let Ok(event) = sub.recv_filtered().await {
                    let notification = format_live_notification(sub_id, &event);
                    if let Err(e) = live_tx.send(notification).await {
                        debug!(sub_id, "live subscription channel closed: {e}");
                        break;
                    }
                }
            });

            serde_json::json!({
                "id": id,
                "result": {
                    "subscription_id": sub_id,
                    "collection": collection,
                    "status": "active"
                }
            })
            .to_string()
        }

        _ => error_response(id, &format!("unknown method: {method}")),
    }
}

/// Execute SQL and return result as JSON.
async fn execute_sql(
    shared: &SharedState,
    query_ctx: &crate::control::planner::context::QueryContext,
    tenant_id: TenantId,
    sql: &str,
    trace_id: u64,
) -> Result<serde_json::Value, String> {
    let tasks = query_ctx
        .plan_sql(sql, tenant_id)
        .await
        .map_err(|e| e.to_string())?;

    let mut results = Vec::new();
    for task in tasks {
        let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            shared,
            task.tenant_id,
            task.vshard_id,
            task.plan,
            trace_id,
        )
        .await
        .map_err(|e| e.to_string())?;

        if !resp.payload.is_empty() {
            let json = crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            match serde_json::from_str::<serde_json::Value>(&json) {
                Ok(v) => results.push(v),
                Err(_) => results.push(serde_json::Value::String(json)),
            }
        }
    }

    match results.len() {
        0 => Ok(serde_json::Value::Null),
        1 => Ok(results
            .into_iter()
            .next()
            .unwrap_or(serde_json::Value::Null)),
        _ => Ok(serde_json::Value::Array(results)),
    }
}

/// Extract collection name from a SQL string using the DataFusion-compatible
/// convention: the first word after FROM (case-insensitive).
///
/// Operates on the uppercase copy to find the keyword, then slices the
/// original string to preserve the original casing before lowercasing.
fn extract_collection_from_sql(sql: &str) -> String {
    let upper = sql.to_uppercase();
    upper
        .find(" FROM ")
        .and_then(|pos| {
            // Slice from the original string at the same byte offset.
            // Safe because " FROM " is ASCII-only, so byte positions match.
            sql.get(pos + 6..)
        })
        .and_then(|after| after.split_whitespace().next())
        .map(|s| s.to_lowercase())
        .unwrap_or_default()
}

/// Format a consistent JSON-RPC error response (always includes `id`).
fn error_response(id: serde_json::Value, message: &str) -> String {
    serde_json::json!({"id": id, "error": message}).to_string()
}

/// Format a live query notification as JSON.
fn format_live_notification(sub_id: u64, event: &ChangeEvent) -> String {
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
