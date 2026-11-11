// SPDX-License-Identifier: BUSL-1.1

//! CHECK constraints and enum labels hold for every statement of a request on
//! NDJSON `/v1/query/stream`, WebSocket RPC `/v1/ws`, HTTP `/v1/query` and
//! native SQL.
//!
//! A CHECK violation answers SQLSTATE 23514 (HTTP 409). An invalid enum label
//! answers 22P02 (HTTP 400). A later statement that writes a checked
//! collection an earlier statement wrote reads that statement's row: a valid
//! UPDATE applies, and one that breaks the CHECK answers 23514. A refused
//! request applies no row.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::protocol::NativeResponse;

use super::wire_support::{exec, ws_frame};
use crate::harness::TestServer;

/// The collection with no constraint.
const PLAIN: &str = "ce_plain";
/// The collection whose rows need `n` above 0.
const CHECKED: &str = "ce_checked";
/// The CHECK constraint of [`CHECKED`].
const CHECK_NAME: &str = "ce_positive_n";
/// The collection with an enum-typed column `state`.
const ORDERS: &str = "ce_orders";
/// The text an enum-label refusal holds.
const ENUM_REFUSAL: &str = "invalid input value for enum";

/// A one-core server with [`PLAIN`], [`CHECKED`] and [`ORDERS`].
async fn fixture() -> TestServer {
    let server = TestServer::start().await;
    for name in [PLAIN, CHECKED] {
        exec(&server, &format!("CREATE COLLECTION {name}")).await;
    }
    exec(
        &server,
        &format!("ALTER COLLECTION {CHECKED} ADD CONSTRAINT {CHECK_NAME} CHECK (NEW.n > 0)"),
    )
    .await;
    exec(&server, "CREATE TYPE ce_state AS ENUM ('new', 'done')").await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {ORDERS} (id TEXT PRIMARY KEY, state ce_state) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    server
}

/// Check that `collection` holds no row `id`.
async fn assert_absent(server: &TestServer, collection: &str, id: &str) {
    let found = server
        .query_text(&format!("SELECT id FROM {collection} WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"));
    assert!(
        found.is_empty(),
        "a refused request must not apply {collection}.{id}, found {found:?}"
    );
}

/// A valid row of [`PLAIN`], then a row of [`CHECKED`] that violates the CHECK.
fn later_violation(id: &str) -> String {
    format!(
        "INSERT INTO {PLAIN} (id, n) VALUES ('{id}', 1); \
         INSERT INTO {CHECKED} (id, n) VALUES ('{id}', -1)"
    )
}

/// POST `sql` to `/v1/query/stream`. Returns the status and the body.
async fn ndjson(server: &TestServer, sql: &str) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/v1/query/stream",
            server.http_port
        ))
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST /v1/query/stream {sql}: {e}"));
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

/// Check that the WebSocket frame for `sql` is an error that holds `needle`.
async fn assert_ws_refused(server: &TestServer, sql: &str, needle: &str) {
    let frame = ws_frame(server, sql).await;
    let error = frame
        .get("error")
        .unwrap_or_else(|| panic!("{sql} must answer an error frame, got {frame}"));
    assert!(
        error.to_string().contains(needle),
        "the error for {sql} must hold {needle:?}, got {frame}"
    );
}

/// Check that an HTTP-family answer refuses with `status` and holds `needle`.
fn assert_http_refused(sql: &str, (status, body): (u16, String), expected: u16, needle: &str) {
    assert_eq!(
        status, expected,
        "{sql} must answer {expected}, got {status}: {body}"
    );
    assert!(
        body.contains(needle),
        "the body for {sql} must hold {needle:?}, got {body}"
    );
}

/// Send `sql` on a new native session. Seq 1 is the handshake.
async fn native(server: &TestServer, sql: &str) -> NativeResponse {
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    send_sql(&mut stream, 2, sql).await
}

/// Check that the native answer refuses with `sqlstate` and holds `needle`.
fn assert_native_refused(sql: &str, response: &NativeResponse, sqlstate: &str, needle: &str) {
    let error = response
        .error
        .as_ref()
        .unwrap_or_else(|| panic!("{sql} must be refused, got {:?}", response.status));
    assert_eq!(error.code, sqlstate, "{sql}: {}", error.message);
    assert!(
        error.message.contains(needle),
        "the error for {sql} must hold {needle:?}, got {}",
        error.message
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_insert_that_violates_a_check_is_refused() {
    let server = fixture().await;
    let sql = format!("INSERT INTO {CHECKED} (id, n) VALUES ('nd1', -1)");
    assert_http_refused(&sql, ndjson(&server, &sql).await, 409, CHECK_NAME);
    assert_absent(&server, CHECKED, "nd1").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_check_violation_in_a_later_statement_refuses_the_request() {
    let server = fixture().await;
    let sql = later_violation("nd2");
    assert_http_refused(&sql, ndjson(&server, &sql).await, 409, CHECK_NAME);
    assert_absent(&server, PLAIN, "nd2").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_insert_that_violates_a_check_is_refused() {
    let server = fixture().await;
    let sql = format!("INSERT INTO {CHECKED} (id, n) VALUES ('ws1', -1)");
    assert_ws_refused(&server, &sql, CHECK_NAME).await;
    assert_absent(&server, CHECKED, "ws1").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_check_violation_in_a_later_statement_refuses_the_request() {
    let server = fixture().await;
    let sql = later_violation("ws2");
    assert_ws_refused(&server, &sql, CHECK_NAME).await;
    assert_absent(&server, PLAIN, "ws2").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_rows_that_pass_the_check_apply() {
    let server = fixture().await;
    server
        .http_exec(&format!(
            "INSERT INTO {PLAIN} (id, n) VALUES ('ok1', -5); \
             INSERT INTO {CHECKED} (id, n) VALUES ('ok1', 2)"
        ))
        .await;
    let found = server
        .query_text(&format!("SELECT n FROM {CHECKED} WHERE id = 'ok1'"))
        .await
        .unwrap_or_else(|e| panic!("read {CHECKED}: {e}"));
    assert_eq!(found, ["2"], "a row that passes the CHECK applies");
}

/// The UPDATE's check merges its SET values over the stored row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_update_that_violates_a_check_is_refused() {
    let server = fixture().await;
    exec(
        &server,
        &format!("INSERT INTO {CHECKED} (id, n) VALUES ('up1', 4)"),
    )
    .await;
    let sql = format!("UPDATE {CHECKED} SET n = -1 WHERE id = 'up1'");
    assert_http_refused(&sql, server.http_query(&sql).await, 409, CHECK_NAME);
    let found = server
        .query_text(&format!("SELECT n FROM {CHECKED} WHERE id = 'up1'"))
        .await
        .unwrap_or_else(|e| panic!("read {CHECKED}: {e}"));
    assert_eq!(found, ["4"], "a refused UPDATE leaves the row unchanged");
}

/// An invalid label in `ORDERS`, as the request's only statement.
fn invalid_label(id: &str) -> String {
    format!("INSERT INTO {ORDERS} (id, state) VALUES ('{id}', 'bogus')")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_invalid_enum_label_is_refused() {
    let server = fixture().await;
    let sql = invalid_label("h1");
    assert_http_refused(&sql, server.http_query(&sql).await, 400, ENUM_REFUSAL);
    assert_absent(&server, ORDERS, "h1").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_invalid_enum_label_is_refused() {
    let server = fixture().await;
    let sql = invalid_label("n1");
    assert_http_refused(&sql, ndjson(&server, &sql).await, 400, ENUM_REFUSAL);
    assert_absent(&server, ORDERS, "n1").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_invalid_enum_label_is_refused() {
    let server = fixture().await;
    let sql = invalid_label("w1");
    assert_ws_refused(&server, &sql, ENUM_REFUSAL).await;
    assert_absent(&server, ORDERS, "w1").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_invalid_enum_label_in_a_later_statement_refuses_the_request() {
    let server = fixture().await;
    let sql = format!(
        "INSERT INTO {PLAIN} (id, n) VALUES ('na1', 1); {}",
        invalid_label("na1")
    );
    assert_native_refused(&sql, &native(&server, &sql).await, "22P02", ENUM_REFUSAL);
    assert_absent(&server, PLAIN, "na1").await;
}

/// An INSERT of a checked row, then an UPDATE of that row to `n`. The UPDATE's
/// CHECK reads the row the INSERT wrote.
fn write_then_update(id: &str, n: i64) -> String {
    format!(
        "INSERT INTO {CHECKED} (id, n) VALUES ('{id}', 1); \
         UPDATE {CHECKED} SET n = {n} WHERE id = '{id}'"
    )
}

async fn stored_n(server: &TestServer, id: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT n FROM {CHECKED} WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("read {CHECKED}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_valid_update_after_a_write_of_a_checked_collection_applies() {
    let server = fixture().await;
    server.http_exec(&write_then_update("u1", 2)).await;
    assert_eq!(stored_n(&server, "u1").await, ["2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_update_that_breaks_the_check_after_a_write_is_refused() {
    let server = fixture().await;
    let sql = write_then_update("u2", -1);
    assert_http_refused(&sql, server.http_query(&sql).await, 409, CHECK_NAME);
    assert_absent(&server, CHECKED, "u2").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_valid_update_after_a_write_of_a_checked_collection_applies() {
    let server = fixture().await;
    let sql = write_then_update("u3", 2);
    let response = native(&server, &sql).await;
    assert!(response.error.is_none(), "{sql}: {:?}", response.error);
    assert_eq!(stored_n(&server, "u3").await, ["2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_update_that_breaks_the_check_after_a_write_is_refused() {
    let server = fixture().await;
    let sql = write_then_update("u4", -1);
    assert_native_refused(&sql, &native(&server, &sql).await, "23514", CHECK_NAME);
    assert_absent(&server, CHECKED, "u4").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_check_violation_in_a_later_statement_refuses_the_request() {
    let server = fixture().await;
    let sql = later_violation("h2");
    assert_http_refused(&sql, server.http_query(&sql).await, 409, CHECK_NAME);
    assert_absent(&server, PLAIN, "h2").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_upsert_that_violates_a_check_in_a_later_statement_is_refused() {
    let server = fixture().await;
    let sql = format!(
        "INSERT INTO {PLAIN} (id, n) VALUES ('h3', 1); \
         UPSERT INTO {CHECKED} (id, n) VALUES ('h3', -1)"
    );
    assert_http_refused(&sql, server.http_query(&sql).await, 409, CHECK_NAME);
    assert_absent(&server, PLAIN, "h3").await;
    assert_absent(&server, CHECKED, "h3").await;
}
