// SPDX-License-Identifier: BUSL-1.1

//! A lone `GRAPH` or `MATCH` statement runs on HTTP `/v1/query`, NDJSON
//! `/v1/query/stream` and WebSocket RPC `/v1/ws`.
//!
//! The planner cannot parse either statement. The graph router owns them, so a
//! route that skips the router answers a parse error. HTTP is the control:
//! it answers both statements. NDJSON and WebSocket answer the same.

use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use crate::harness::TestServer;

/// The collection that holds the edges.
const GRAPH: &str = "h2_g";
/// The pattern that reads every `l` edge of [`GRAPH`].
const MATCH_ALL: &str = "MATCH (x)-[:l]->(y) IN 'h2_g' RETURN x, y";

/// A route that carries one statement per request.
#[derive(Clone, Copy, Debug)]
enum Route {
    Http,
    Ndjson,
    Ws,
}

/// The answer to one statement: whether it succeeded, and its text.
struct Answer {
    ok: bool,
    text: String,
}

impl Route {
    async fn send(self, server: &TestServer, sql: &str) -> Answer {
        match self {
            Self::Http => {
                let (status, text) = post(server, "/v1/query", sql).await;
                Answer {
                    ok: status == 200,
                    text: format!("{status}: {text}"),
                }
            }
            Self::Ndjson => {
                let (status, text) = post(server, "/v1/query/stream", sql).await;
                Answer {
                    ok: status == 200 && !text.contains("\"error\""),
                    text: format!("{status}: {text}"),
                }
            }
            Self::Ws => {
                let frame = ws_frame(server, sql).await;
                Answer {
                    ok: frame.get("error").is_none(),
                    text: frame.to_string(),
                }
            }
        }
    }
}

/// A server with [`GRAPH`] and the edge `seed_a -> seed_b`.
async fn fixture() -> TestServer {
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE COLLECTION {GRAPH}"))
        .await
        .expect("create the graph collection");
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{GRAPH}' FROM 'seed_a' TO 'seed_b' TYPE 'l'"
        ))
        .await
        .expect("seed an edge");
    server
}

/// POST `sql` to `path`. Returns the status and the body.
async fn post(server: &TestServer, path: &str, sql: &str) -> (u16, String) {
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
async fn ws_frame(server: &TestServer, sql: &str) -> Value {
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

/// Check that `route` runs `GRAPH INSERT EDGE` and `MATCH`: the match names
/// the seeded edge, and an edge the route inserted shows on HTTP.
async fn assert_runs_lone_graph_statements(route: Route) {
    let server = fixture().await;

    let matched = route.send(&server, MATCH_ALL).await;
    assert!(
        matched.ok,
        "{route:?} must run a lone MATCH, got {}",
        matched.text
    );
    for node in ["seed_a", "seed_b"] {
        assert!(
            matched.text.contains(node),
            "{route:?} MATCH must name {node}, got {}",
            matched.text
        );
    }

    let insert = format!("GRAPH INSERT EDGE IN '{GRAPH}' FROM 'new_a' TO 'new_b' TYPE 'l'");
    let inserted = route.send(&server, &insert).await;
    assert!(
        inserted.ok,
        "{route:?} must run a lone GRAPH INSERT EDGE, got {}",
        inserted.text
    );
    let read_back = Route::Http.send(&server, MATCH_ALL).await;
    for node in ["new_a", "new_b"] {
        assert!(
            read_back.text.contains(node),
            "the edge {route:?} inserted must show on HTTP, missing {node}: {}",
            read_back.text
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_lone_graph_statement_http() {
    assert_runs_lone_graph_statements(Route::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_lone_graph_statement_ndjson() {
    assert_runs_lone_graph_statements(Route::Ndjson).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_lone_graph_statement_ws() {
    assert_runs_lone_graph_statements(Route::Ws).await;
}
