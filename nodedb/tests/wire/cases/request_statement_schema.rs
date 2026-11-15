// SPDX-License-Identifier: BUSL-1.1

//! Each statement of a request shapes its rows against its own output
//! columns.
//!
//! Two reads of one collection name different aliases. Statement 1's row
//! answers its own alias, and statement 2's row answers its own. The body
//! keeps main's summed shape: one `rows` list, in statement order.

use serde_json::Value;

use crate::harness::TestServer;

const REQUEST: &str = "SELECT n AS first_n FROM ss_rows WHERE id = 'a'; \
                       SELECT id AS second_id FROM ss_rows WHERE id = 'a'";

/// A one-core server with `ss_rows`, holding row `a` with `n` = 7.
async fn fixture() -> TestServer {
    let server = TestServer::start().await;
    for sql in [
        "CREATE COLLECTION ss_rows (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')",
        "INSERT INTO ss_rows (id, n) VALUES ('a', 7)",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("run {sql}: {e}"));
    }
    server
}

/// Check that `rows` holds statement 1's row, then statement 2's row, each
/// under its own alias.
fn assert_rows_of_each_statement(rows: &[Value], context: &str) {
    assert_eq!(rows.len(), 2, "{context}: {rows:?}");
    assert_eq!(rows[0]["first_n"], 7, "{context}: {rows:?}");
    assert_eq!(rows[1]["second_id"], "a", "{context}: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_rows_take_their_own_statement_columns() {
    let server = fixture().await;
    let (status, body) = server.http_query(REQUEST).await;
    assert_eq!(status, 200, "{body}");
    let body: Value =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("a JSON body: {e}: {body}"));
    let rows = body["rows"].as_array().cloned().unwrap_or_default();
    assert_rows_of_each_statement(&rows, "/v1/query");
    assert!(
        body.get("statements").is_none(),
        "the body keeps the summed shape: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_rows_take_their_own_statement_columns() {
    let server = fixture().await;
    let body = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/v1/query/stream",
            server.http_port
        ))
        .json(&serde_json::json!({ "sql": REQUEST }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST /v1/query/stream: {e}"))
        .text()
        .await
        .unwrap_or_default();
    let rows: Vec<Value> = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|e| panic!("a JSON line: {e}: {line}"))
        })
        .collect();
    assert_rows_of_each_statement(&rows, "/v1/query/stream");
}
