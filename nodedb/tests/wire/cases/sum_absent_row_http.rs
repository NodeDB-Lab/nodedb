// SPDX-License-Identifier: BUSL-1.1

//! The absent-row races of [`super::sum_absent_row`] over HTTP
//! `/v1/query`, NDJSON `/v1/query/stream` and WebSocket RPC `/v1/ws`.
//!
//! A serialization conflict answers 409 on HTTP. NDJSON answers 409 for a
//! conflict at planning, and an error line for one at the route. WebSocket
//! RPC answers an error frame. The helpers here read each of these as a
//! conflict, and any other error as a failure.

#![cfg(feature = "failpoints")]

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use super::sum_absent_row::{
    ABSENT_DELETE, ABSENT_UPDATE, End, JOINLESS_UPDATE, PARKED_BOUND, ROUTE_GATE, Route,
    UNBOUND_DELETE, UNBOUND_UPDATE, probe_calvin_route, race_parked_at, uncontended,
};

/// The HTTP status of a serialization conflict: class `40` maps to 409.
const HTTP_CONFLICT: u16 = 409;
/// The text every serialization conflict message carries.
const CONFLICT_TEXT: &str = "serialization conflict";
/// The id of the one RPC a WebSocket write sends.
const RPC_ID: u64 = 1;

pub(super) type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// How an error `message` ends a write: a conflict, or a failure.
fn error_end(message: &str) -> End {
    if message.to_ascii_lowercase().contains(CONFLICT_TEXT) {
        End::Conflict
    } else {
        End::Failed(message.to_string())
    }
}

/// POST `sql` to `path` and return the status and the body.
async fn post(port: u16, path: &str, sql: &str) -> Result<(u16, String), String> {
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}{path}"))
        .json(&serde_json::json!({ "sql": sql }))
        .timeout(PARKED_BOUND)
        .send()
        .await
        .map_err(|e| format!("POST {path} {sql}: {e}"))?;
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .map_err(|e| format!("read the {path} body for {sql}: {e}"))?;
    Ok((status, body))
}

/// POST `sql` to `/v1/query` and report how it ended.
pub(super) async fn http_end(port: u16, sql: &str) -> End {
    match post(port, "/v1/query", sql).await {
        Ok((status, _)) if (200..300).contains(&status) => End::Answered,
        Ok((HTTP_CONFLICT, _)) => End::Conflict,
        Ok((status, body)) => End::Failed(format!("{status}: {body}")),
        Err(e) => End::Failed(e),
    }
}

/// POST `sql` to `/v1/query/stream` and report how it ended.
pub(super) async fn ndjson_end(port: u16, sql: &str) -> End {
    let (status, body) = match post(port, "/v1/query/stream", sql).await {
        Ok(answer) => answer,
        Err(e) => return End::Failed(e),
    };
    if status == HTTP_CONFLICT {
        return End::Conflict;
    }
    if !(200..300).contains(&status) {
        return End::Failed(format!("{status}: {body}"));
    }
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(frame) = serde_json::from_str::<serde_json::Value>(line) else {
            return End::Failed(format!("not an NDJSON line: {line}"));
        };
        if let Some(error) = frame.get("error") {
            return error_end(&error.to_string());
        }
    }
    End::Answered
}

/// Open a WebSocket RPC connection.
pub(super) async fn ws_connect(port: u16) -> WsStream {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/v1/ws"))
        .await
        .expect("connect the WebSocket RPC endpoint");
    ws
}

/// Send `sql` as one RPC on `ws` and report how it ended.
pub(super) async fn ws_end(ws: &mut WsStream, sql: &str) -> End {
    let request = serde_json::json!({ "id": RPC_ID, "method": "query", "params": { "sql": sql } });
    if let Err(e) = ws.send(Message::Text(request.to_string().into())).await {
        return End::Failed(format!("send {sql}: {e}"));
    }
    loop {
        let message = match tokio::time::timeout(PARKED_BOUND, ws.next()).await {
            Ok(Some(Ok(message))) => message,
            Ok(Some(Err(e))) => return End::Failed(format!("read the answer to {sql}: {e}")),
            Ok(None) => return End::Failed(format!("the socket closed before answering {sql}")),
            Err(_) => return End::Failed(format!("no answer to {sql}")),
        };
        let Message::Text(text) = message else {
            continue;
        };
        let Ok(frame) = serde_json::from_str::<serde_json::Value>(&text) else {
            return End::Failed(format!("not a JSON frame: {text}"));
        };
        if frame["id"] != RPC_ID {
            continue;
        }
        return match frame.get("error") {
            Some(error) => error_end(&error.to_string()),
            None => End::Answered,
        };
    }
}

/// An unbound update that commits after the insert of its key ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_update_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::Http, UNBOUND_UPDATE).await;
}

/// An unbound delete that commits after the insert of its key ends at balance 18, sum 15.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_delete_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::Http, UNBOUND_DELETE).await;
}

/// An absent-row update that commits after the row returns ends at balance 15, sum 17.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_update_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Http, ABSENT_UPDATE).await;
}

/// An absent-row delete that commits after the row returns ends at balance 15, sum 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_delete_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Http, ABSENT_DELETE).await;
}

/// A join-less update that commits after its join value lands ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_update_of_a_joinless_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Http, JOINLESS_UPDATE).await;
}

/// An unbound update that commits after the insert of its key ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_update_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::Ndjson, UNBOUND_UPDATE).await;
}

/// An unbound delete that commits after the insert of its key ends at balance 18, sum 15.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_delete_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::Ndjson, UNBOUND_DELETE).await;
}

/// An absent-row update that commits after the row returns ends at balance 15, sum 17.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_update_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Ndjson, ABSENT_UPDATE).await;
}

/// An absent-row delete that commits after the row returns ends at balance 15, sum 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_delete_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Ndjson, ABSENT_DELETE).await;
}

/// An unbound update that commits after the insert of its key ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_update_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::Ws, UNBOUND_UPDATE).await;
}

/// An unbound delete that commits after the insert of its key ends at balance 18, sum 15.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_delete_of_an_unbound_key_settles() {
    race_parked_at(ROUTE_GATE, Route::Ws, UNBOUND_DELETE).await;
}

/// An absent-row update that commits after the row returns ends at balance 15, sum 17.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_update_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Ws, ABSENT_UPDATE).await;
}

/// An absent-row delete that commits after the row returns ends at balance 15, sum 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_delete_of_an_absent_row_settles() {
    race_parked_at(ROUTE_GATE, Route::Ws, ABSENT_DELETE).await;
}

/// An unbound or absent-row update that answers 409 with no mover aborts a write nothing raced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_uncontended_absent_row_updates_commit() {
    uncontended(Route::Http).await;
}

/// An unbound or absent-row update that answers a conflict with no mover aborts an unraced write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_uncontended_absent_row_updates_commit() {
    uncontended(Route::Ndjson).await;
}

/// An unbound or absent-row update that answers a conflict with no mover aborts an unraced write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_uncontended_absent_row_updates_commit() {
    uncontended(Route::Ws).await;
}

/// An unbound update that skips Calvin carries no read, so no insert of its key aborts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_unbound_update_takes_calvin() {
    probe_calvin_route(Route::Http).await;
}
