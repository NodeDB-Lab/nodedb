// SPDX-License-Identifier: BUSL-1.1

//! Transaction control inside one request over HTTP `/v1/query`, NDJSON
//! `/v1/query/stream` and WebSocket RPC.
//!
//! `BEGIN; ..; COMMIT` runs the inner statements as the request. The answer
//! is the answer of the inner statements sent alone. `BEGIN; ..; ROLLBACK`
//! applies nothing and answers no row. Every other placement answers 0A000
//! (HTTP 501) and applies nothing. With `[server.sql]
//! txn_wrapper_stateless = false`, the wrapper answers 0A000 too.

use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use crate::harness::TestServer;

/// The text the 0A000 refusal of a partial wrapper holds.
const REFUSAL: &str = "separate requests";

#[derive(Clone, Copy, Debug)]
enum Transport {
    Http,
    Ndjson,
    Ws,
}

const TRANSPORTS: [Transport; 3] = [Transport::Http, Transport::Ndjson, Transport::Ws];

/// One answer, read the same way on every transport.
#[derive(Debug)]
struct Answer {
    /// The HTTP status. WebSocket answers 200 for a result, 0 for an error.
    status: u16,
    /// The error text, empty when the answer holds none.
    error: String,
    /// The answer with no error: the HTTP body, the NDJSON lines as an
    /// array, or the WebSocket `result`.
    value: Value,
}

fn json(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("a JSON value: {e}: {text}"))
}

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
        let frame = json(&text);
        if frame["id"] == 1 {
            return frame;
        }
    }
}

fn error_text(value: Option<&Value>) -> String {
    value.map(Value::to_string).unwrap_or_default()
}

async fn answer(server: &TestServer, transport: Transport, sql: &str) -> Answer {
    match transport {
        Transport::Http => {
            let (status, raw) = post(server, "/v1/query", sql).await;
            let body = json(&raw);
            Answer {
                status,
                error: error_text(body.get("error")),
                value: body,
            }
        }
        Transport::Ndjson => {
            let (status, raw) = post(server, "/v1/query/stream", sql).await;
            if status != 200 {
                return Answer {
                    status,
                    error: raw,
                    value: Value::Null,
                };
            }
            let lines: Vec<Value> = raw
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(json)
                .collect();
            let error = error_text(lines.iter().find_map(|line| line.get("error")));
            Answer {
                status,
                error,
                value: Value::from(lines),
            }
        }
        Transport::Ws => {
            let frame = ws_frame(server, sql).await;
            let error = error_text(frame.get("error"));
            Answer {
                status: if frame.get("error").is_some() { 0 } else { 200 },
                error,
                value: frame["result"].clone(),
            }
        }
    }
}

/// A one-core server with `tw (id, n)`.
async fn fixture(server: TestServer) -> TestServer {
    server
        .exec("CREATE COLLECTION tw (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')")
        .await
        .unwrap_or_else(|e| panic!("create tw: {e}"));
    server
}

fn insert(id: &str) -> String {
    format!("INSERT INTO tw (id, n) VALUES ('{id}', 1)")
}

/// The ids of `tw` among `ids` that a row holds.
async fn stored(server: &TestServer, ids: &[&str]) -> Vec<String> {
    let list = ids.iter().map(|id| format!("'{id}'")).collect::<Vec<_>>();
    let sql = format!(
        "SELECT id FROM tw WHERE id IN ({}) ORDER BY id",
        list.join(", ")
    );
    server
        .query_text(&sql)
        .await
        .unwrap_or_else(|e| panic!("read {sql}: {e}"))
}

/// Whether `answer` holds no row on its transport.
fn holds_no_row(transport: Transport, answer: &Answer) -> bool {
    match transport {
        Transport::Http => answer.value["rows"] == serde_json::json!([]),
        Transport::Ndjson => answer.value == serde_json::json!([]),
        Transport::Ws => answer.value.is_null(),
    }
}

/// Both inner rows apply. The answer equals the answer of the same two
/// statements sent with no wrapper.
/// Lever: `split_with_wrapper` returns `Self::split(sql)` alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_wrapper_answers_as_its_inner_statements() {
    let server = fixture(TestServer::start().await).await;
    for transport in TRANSPORTS {
        let ids = ["c1a", "c1b", "c1c", "c1d"].map(|id| format!("{transport:?}_{id}"));
        let wrapped = format!("BEGIN; {}; {}; COMMIT", insert(&ids[0]), insert(&ids[1]));
        let got = answer(&server, transport, &wrapped).await;
        assert_eq!(got.status, 200, "{transport:?} {wrapped}: {got:?}");
        assert!(got.error.is_empty(), "{transport:?} {wrapped}: {got:?}");

        let plain = format!("{}; {}", insert(&ids[2]), insert(&ids[3]));
        let alone = answer(&server, transport, &plain).await;
        assert_eq!(alone.status, 200, "{transport:?} {plain}: {alone:?}");
        assert_eq!(
            got.value, alone.value,
            "{transport:?}: the wrapper must answer as its inner statements"
        );
        let all: Vec<&str> = ids.iter().map(String::as_str).collect();
        assert_eq!(stored(&server, &all).await, ids.to_vec(), "{transport:?}");
    }
}

/// Nothing applies, and the answer holds no row.
/// Lever: `plan_authorize_and_admit_once` ignores `rolls_back`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rollback_wrapper_applies_nothing_and_answers_no_row() {
    let server = fixture(TestServer::start().await).await;
    for transport in TRANSPORTS {
        let a = format!("{transport:?}_c2a");
        let sql = format!("BEGIN; {}; ROLLBACK", insert(&a));
        let got = answer(&server, transport, &sql).await;
        assert_eq!(got.status, 200, "{transport:?} {sql}: {got:?}");
        assert!(got.error.is_empty(), "{transport:?} {sql}: {got:?}");
        assert!(holds_no_row(transport, &got), "{transport:?}: {got:?}");
        assert!(stored(&server, &[&a]).await.is_empty(), "{got:?}");
    }
}

/// A planning error inside a rollback wrapper answers that error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rollback_wrapper_answers_an_inner_plan_error() {
    let server = fixture(TestServer::start().await).await;
    for transport in TRANSPORTS {
        let sql = "BEGIN; INSERT INTO tw_missing (id) VALUES ('x'); ROLLBACK";
        let got = answer(&server, transport, sql).await;
        assert!(!got.error.is_empty(), "{transport:?} {sql}: {got:?}");
        assert!(
            got.error.contains("tw_missing"),
            "{transport:?} {sql}: {got:?}"
        );
    }
}

/// Check that the request `shape` builds answers the 0A000 refusal on every
/// transport and applies neither of its rows.
async fn assert_refused(server: &TestServer, needle: &str, shape: impl Fn(&str, &str) -> String) {
    for transport in TRANSPORTS {
        let (a, b) = (format!("{transport:?}_ra"), format!("{transport:?}_rb"));
        let sql = shape(&insert(&a), &insert(&b));
        let got = answer(server, transport, &sql).await;
        if !matches!(transport, Transport::Ws) {
            assert_eq!(got.status, 501, "{transport:?} {sql}: {got:?}");
        }
        assert!(got.error.contains(needle), "{transport:?} {sql}: {got:?}");
        assert!(
            stored(server, &[&a, &b]).await.is_empty(),
            "{transport:?} {sql}"
        );
    }
}

/// A COMMIT between two statements.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_in_the_middle_is_refused() {
    let server = fixture(TestServer::start().await).await;
    assert_refused(&server, REFUSAL, |a, b| format!("{a}; COMMIT; {b}")).await;
}

/// A BEGIN with no end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_begin_with_no_end_is_refused() {
    let server = fixture(TestServer::start().await).await;
    assert_refused(&server, REFUSAL, |a, b| format!("BEGIN; {a}; {b}")).await;
}

/// A SAVEPOINT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_savepoint_is_refused() {
    let server = fixture(TestServer::start().await).await;
    assert_refused(&server, REFUSAL, |a, _| format!("SAVEPOINT s; {a}")).await;
}

/// A BEGIN with an isolation level.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_begin_with_a_mode_is_refused() {
    let server = fixture(TestServer::start().await).await;
    assert_refused(&server, REFUSAL, |a, _| {
        format!("BEGIN ISOLATION LEVEL SERIALIZABLE; {a}; COMMIT")
    })
    .await;
}

/// A session command inside the wrapper takes the solo refusal, numbered in
/// the inner statements.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_command_inside_the_wrapper_is_refused() {
    let server = fixture(TestServer::start().await).await;
    assert_refused(&server, "statement 2 of 2", |a, _| {
        format!("BEGIN; {a}; SET x = 1; COMMIT")
    })
    .await;
}

/// With the switch off, a commit wrapper takes the planner's solo refusal.
/// Lever: the edges pass `txn_wrapper: true` whatever the switch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_switch_off_refuses_a_commit_wrapper() {
    let server = fixture(TestServer::start_with_txn_wrapper_off().await).await;
    assert_refused(&server, "its own request", |a, b| {
        format!("BEGIN; {a}; {b}; COMMIT")
    })
    .await;
}
