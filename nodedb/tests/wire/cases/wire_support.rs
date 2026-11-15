// SPDX-License-Identifier: BUSL-1.1

//! Helpers that the wire tests of a request share: statements, rows, HTTP
//! and WebSocket RPC.

use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use crate::harness::TestServer;

/// Run `sql` and panic with the server error text on an error.
pub(super) async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// The rows of `collection`, ordered by key. Each row holds `id` and `v`.
pub(super) async fn rows(server: &TestServer, collection: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!("SELECT id, v FROM {collection} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
}

/// POST `sql` to `path`. Returns the status and the body.
pub(super) async fn post(server: &TestServer, path: &str, sql: &str) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}{path}", server.http_port))
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {path} {sql}: {e}"));
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

/// Send `sql` as WebSocket RPC `query` on a new socket. Returns the frame
/// that answers it.
pub(super) async fn ws_frame(server: &TestServer, sql: &str) -> Value {
    let url = format!("ws://127.0.0.1:{}/v1/ws", server.http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("connect the WebSocket RPC endpoint");
    let request = serde_json::json!({ "id": 1, "method": "query", "params": { "sql": sql } });
    ws.send(Message::Text(request.to_string().into()))
        .await
        .unwrap_or_else(|e| panic!("send {sql}: {e}"));
    loop {
        let message = tokio::time::timeout(std::time::Duration::from_secs(30), ws.next())
            .await
            .unwrap_or_else(|_| panic!("no answer to {sql}"))
            .unwrap_or_else(|| panic!("the socket closed before answering {sql}"))
            .unwrap_or_else(|e| panic!("read the answer to {sql}: {e}"));
        let Message::Text(text) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("a JSON frame for {sql}: {e}: {text}"));
        if frame["id"] == 1 {
            return frame;
        }
    }
}
