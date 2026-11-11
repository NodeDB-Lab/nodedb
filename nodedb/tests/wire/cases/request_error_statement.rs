// SPDX-License-Identifier: BUSL-1.1

//! The error of statement 2 of a request names it on every transport.
//!
//! HTTP `/v1/query` answers the status and `error` text the statement answers
//! alone, plus `statement` (1-based) and `position`, the 1-based character
//! offset where the statement starts. An NDJSON line and a WebSocket frame
//! also answer `code`, the public code `NDB-<n>`, and `sqlstate`. A request
//! of one statement answers no `sqlstate`, `statement` or `position` key.
//! Native prefixes the message with `statement 2 of 2: `.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use serde_json::Value;

use super::wire_support::{exec, ws_frame};
use crate::harness::TestServer;

/// The first statement of every request. It applies alone.
const FIRST: &str = "INSERT INTO rq_plain (id, n) VALUES ('ok1', 1)";
/// A statement on a collection that does not exist.
const MISSING: &str = "INSERT INTO rq_missing (id, n) VALUES ('m1', 1)";
/// A statement the planner cannot parse.
const UNPARSED: &str = "SELECT * FROM rq_plain WHERE";
/// A statement that violates the CHECK of `rq_checked`.
const VIOLATION: &str = "INSERT INTO rq_checked (id, n) VALUES ('c1', -1)";
/// A statement that writes the key the fixture holds. The Data Plane refuses it.
const DUPLICATE: &str = "INSERT INTO rq_plain (id, n) VALUES ('dup', 2)";

/// A one-core server with `rq_plain`, holding row `dup`, and `rq_checked`.
async fn fixture() -> TestServer {
    let server = TestServer::start().await;
    exec(
        &server,
        "CREATE COLLECTION rq_plain (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')",
    )
    .await;
    exec(&server, "INSERT INTO rq_plain (id, n) VALUES ('dup', 1)").await;
    exec(&server, "CREATE COLLECTION rq_checked").await;
    exec(
        &server,
        "ALTER COLLECTION rq_checked ADD CONSTRAINT rq_positive CHECK (NEW.n > 0)",
    )
    .await;
    server
}

/// `FIRST`, then `second`.
fn pair(second: &str) -> String {
    format!("{FIRST}; {second}")
}

/// The 1-based character offset of `second` in `pair(second)`.
fn position_of_second(second: &str) -> u64 {
    let sql = pair(second);
    let start = sql
        .rfind(second)
        .expect("the pair holds its second statement");
    (sql[..start].chars().count() + 1) as u64
}

async fn http(server: &TestServer, sql: &str) -> (u16, Value) {
    let (status, body) = server.http_query(sql).await;
    let value = serde_json::from_str(&body)
        .unwrap_or_else(|e| panic!("a JSON body for {sql}: {e}: {body}"));
    (status, value)
}

fn keys(value: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = value
        .as_object()
        .map(|object| object.keys().map(String::as_str).collect())
        .unwrap_or_default();
    keys.sort_unstable();
    keys
}

/// Check that HTTP answers `second` in statement 2 with an error that holds
/// `needle`, plus `statement` and `position`. `second` alone names no
/// statement. A lone INSERT takes the router, so its text can differ.
async fn assert_http_names_statement_two(server: &TestServer, second: &str, needle: &str) {
    let (alone_status, alone) = http(server, second).await;
    assert!(!(200..300).contains(&alone_status), "{second}: {alone}");
    assert!(
        alone.get("statement").is_none() && alone.get("position").is_none(),
        "a request of one statement names no statement: {alone}"
    );
    let sql = pair(second);
    let (status, body) = http(server, &sql).await;
    assert!(!(200..300).contains(&status), "{sql}: {status} {body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(error.contains(needle), "{sql} must hold {needle:?}: {body}");
    assert!(
        !error.starts_with("statement 2"),
        "the HTTP text carries no prefix: {body}"
    );
    assert_eq!(body["statement"], 2, "{sql}: {body}");
    assert_eq!(
        body["position"],
        position_of_second(second),
        "{sql}: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_missing_collection_in_statement_two_names_it() {
    let server = fixture().await;
    assert_http_names_statement_two(&server, MISSING, "rq_missing").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_plan_error_in_statement_two_names_it() {
    let server = fixture().await;
    assert_http_names_statement_two(&server, UNPARSED, "").await;
}

/// A CHECK violation in statement 2 answers 409.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_check_violation_in_statement_two_names_it() {
    let server = fixture().await;
    assert_http_names_statement_two(&server, VIOLATION, "rq_positive").await;
    assert_eq!(server.http_query(&pair(VIOLATION)).await.0, 409);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_data_plane_error_in_statement_two_names_it() {
    let server = fixture().await;
    assert_http_names_statement_two(&server, DUPLICATE, "dup").await;
}

/// Every non-empty NDJSON line of `sql`, parsed.
async fn ndjson_lines(server: &TestServer, sql: &str) -> Vec<Value> {
    let body = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/v1/query/stream",
            server.http_port
        ))
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST /v1/query/stream {sql}: {e}"))
        .text()
        .await
        .unwrap_or_default();
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|e| panic!("a JSON line: {e}: {line}"))
        })
        .collect()
}

fn error_line(lines: &[Value], sql: &str) -> Value {
    lines
        .iter()
        .find(|line| line.get("error").is_some())
        .cloned()
        .unwrap_or_else(|| panic!("{sql} must answer an error line, got {lines:?}"))
}

/// Check that `error` names statement 2 of `pair(DUPLICATE)`, its code and
/// its SQLSTATE.
fn assert_names_duplicate(error: &Value) {
    assert_eq!(error["code"], "NDB-1000", "{error}");
    assert_eq!(error["sqlstate"], "23505", "{error}");
    assert_eq!(error["statement"], 2, "{error}");
    assert_eq!(error["position"], position_of_second(DUPLICATE), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_error_in_statement_two_names_it_and_its_code() {
    let server = fixture().await;
    let alone = error_line(&ndjson_lines(&server, DUPLICATE).await, DUPLICATE);
    assert_eq!(keys(&alone), ["code", "error"], "{alone}");

    let sql = pair(DUPLICATE);
    let line = error_line(&ndjson_lines(&server, &sql).await, &sql);
    assert_eq!(line["error"], alone["error"], "{line}");
    assert_names_duplicate(&line);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_error_in_statement_two_names_it_and_its_code() {
    let server = fixture().await;
    let alone = ws_frame(&server, DUPLICATE).await;
    assert_eq!(keys(&alone), ["error", "id"], "{alone}");

    let frame = ws_frame(&server, &pair(DUPLICATE)).await;
    assert_eq!(frame["error"], alone["error"], "{frame}");
    assert_names_duplicate(&frame);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_plan_error_in_statement_two_carries_the_prefix() {
    let server = fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let alone = send_sql(&mut stream, 2, UNPARSED).await;
    let alone = alone.error.expect("the statement alone must not plan");
    let paired = send_sql(&mut stream, 3, &pair(UNPARSED)).await;
    let paired = paired.error.expect("statement 2 must not plan");
    assert_eq!(paired.code, alone.code);
    assert_eq!(
        paired.message,
        format!("statement 2 of 2: {}", alone.message)
    );
}

/// A Data-Plane error of statement 2 of a request the per-task loop runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_data_plane_error_in_statement_two_carries_the_prefix() {
    let server = fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let paired = send_sql(&mut stream, 2, &pair(DUPLICATE)).await;
    let paired = paired.error.expect("statement 2 must not apply");
    assert!(
        paired.message.starts_with("statement 2 of 2: "),
        "{}",
        paired.message
    );
}
