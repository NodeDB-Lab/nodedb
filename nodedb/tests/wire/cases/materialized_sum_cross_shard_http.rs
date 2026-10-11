// SPDX-License-Identifier: BUSL-1.1

//! A plain `INSERT` and an `INSERT ... SELECT` into the source of a
//! materialized sum whose target lives on another vShard, over HTTP
//! `/v1/query`, NDJSON `/v1/query/stream` and WebSocket RPC `/v1/ws`.
//!
//! The plain `INSERT` plans the source write and an `ApplyBalanceDelta` task
//! homed on the target's vShard. The two commit together through Calvin.
//! The `INSERT ... SELECT` runs in an implicit transaction that expands it
//! into point writes and ships each balance to its target. Each route
//! answers one `{"affected": n}` row: the balance task adds no count, as on
//! pgwire and native.
//!
//! The fixture is the one the pgwire cases use: two Data Plane cores, and a
//! target and source that land on different cores.

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use super::materialized_sum_cross_shard_expanders::{INSERT_SELECT, balance, feed, sum_fixture};
use crate::harness::TestServer;

/// A plain write into the source. It credits 7 to `acc2`.
const PLAIN_INSERT: &str =
    "INSERT INTO ptv_post (id, account_id, amount) VALUES ('h1', 'acc2', '7')";

/// The answer a write that touched `n` rows gives on these routes.
fn affected(n: u64) -> serde_json::Value {
    serde_json::json!({ "affected": n })
}

/// Seed the feed `INSERT_SELECT` copies: 4 for `acc`, 6 for `acc2`.
async fn seed_feed(server: &TestServer) {
    feed(server, "f1", "acc", "4").await;
    feed(server, "f2", "acc2", "6").await;
}

/// The balances after `PLAIN_INSERT` and `INSERT_SELECT` both ran.
async fn assert_balances(server: &TestServer) {
    assert_eq!(balance(server, "acc").await, "19", "10 + 5 + 4");
    assert_eq!(balance(server, "acc2").await, "13", "7 + 6");
}

/// POST `sql` to `/v1/query` and return its `rows`.
async fn http_rows(http_port: u16, sql: &str) -> Vec<serde_json::Value> {
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{http_port}/v1/query"))
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST /v1/query {sql}: {e}"));
    let status = response.status();
    let body: serde_json::Value = response
        .json()
        .await
        .unwrap_or_else(|e| panic!("decode the /v1/query body for {sql}: {e}"));
    assert!(status.is_success(), "{sql} answered {status}: {body}");
    body["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("a `rows` array for {sql}: {body}"))
        .clone()
}

/// POST `sql` to `/v1/query/stream` and return its lines, parsed.
async fn ndjson_lines(http_port: u16, sql: &str) -> Vec<serde_json::Value> {
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{http_port}/v1/query/stream"))
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST /v1/query/stream {sql}: {e}"));
    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|e| panic!("read the /v1/query/stream body for {sql}: {e}"));
    assert!(status.is_success(), "{sql} answered {status}: {body}");
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("an NDJSON line for {sql}: {e}: {line}"))
        })
        .collect()
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Send `sql` as RPC `id` and return its `result`.
async fn ws_result(ws: &mut WsStream, id: u64, sql: &str) -> serde_json::Value {
    let request = serde_json::json!({ "id": id, "method": "query", "params": { "sql": sql } });
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
        let frame: serde_json::Value = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("a JSON frame for {sql}: {e}: {text}"));
        if frame["id"] != id {
            continue;
        }
        assert!(frame.get("error").is_none(), "{sql} failed: {frame}");
        return frame["result"].clone();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_query_moves_a_cross_shard_sum_atomically() {
    let server = sum_fixture().await;
    seed_feed(&server).await;

    assert_eq!(
        http_rows(server.http_port, PLAIN_INSERT).await,
        vec![affected(1)],
        "one row: the balance task adds no count"
    );
    assert_eq!(balance(&server, "acc2").await, "7", "h1 credited");

    assert_eq!(
        http_rows(server.http_port, INSERT_SELECT).await,
        vec![affected(2)],
        "two copied rows: a balance task adds no count"
    );
    assert_balances(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_query_moves_a_cross_shard_sum_atomically() {
    let server = sum_fixture().await;
    seed_feed(&server).await;

    assert_eq!(
        ndjson_lines(server.http_port, PLAIN_INSERT).await,
        vec![affected(1)],
        "one line: the balance task adds no count"
    );
    assert_eq!(balance(&server, "acc2").await, "7", "h1 credited");

    assert_eq!(
        ndjson_lines(server.http_port, INSERT_SELECT).await,
        vec![affected(2)],
        "two copied rows: a balance task adds no count"
    );
    assert_balances(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_rpc_query_moves_a_cross_shard_sum_atomically() {
    let server = sum_fixture().await;
    seed_feed(&server).await;
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}/v1/ws", server.http_port))
            .await
            .expect("connect the WebSocket RPC endpoint");

    assert_eq!(
        ws_result(&mut ws, 1, PLAIN_INSERT).await,
        affected(1),
        "one row: the balance task adds no count"
    );
    assert_eq!(balance(&server, "acc2").await, "7", "h1 credited");

    assert_eq!(
        ws_result(&mut ws, 2, INSERT_SELECT).await,
        affected(2),
        "two copied rows: a balance task adds no count"
    );
    assert_balances(&server).await;
}
