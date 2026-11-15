// SPDX-License-Identifier: BUSL-1.1

//! A request of several statements with a write, whose writes all stage,
//! runs whole in one implicit transaction on HTTP, NDJSON, WebSocket RPC and
//! native. An error in a later statement undoes the writes before it, as on
//! pgwire.
//!
//! Levers:
//! - `request_runs_whole_in_txn` (`implicit_request.rs`) returns false. The
//!   rollback cells keep the earlier insert, the derived-task cell and the
//!   batch-insert key cell answer the per-task route.
//! - `stages_whole` drops its TRUNCATE check. The TRUNCATE cell loses its
//!   `truncated` count.
//! - `run_implicit_statement` skips `group_by_statement`. The derived-task
//!   cell reads the balance before the move.
//! - `route_whole_in_txn` ignores `statement_needs_implicit_txn`. The flagged
//!   cell answers `inserted`.
//! - `request_reads` ignores `derives_node_delete_reads`. The node delete
//!   cell answers 500.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use serde_json::Value;

use super::materialized_sum_cross_shard_expanders::{feed, sum_fixture};
use super::wire_support::{exec, post, ws_frame};
use crate::harness::TestServer;

#[derive(Clone, Copy, Debug)]
enum Transport {
    Http,
    Ndjson,
    Ws,
    Native,
}

/// Run `sql` on `transport`. Returns the error text, or `None`.
async fn error_of(server: &TestServer, transport: Transport, sql: &str) -> Option<String> {
    match transport {
        Transport::Http => {
            let (status, raw) = post(server, "/v1/query", sql).await;
            (status != 200).then(|| format!("{status}: {raw}"))
        }
        Transport::Ndjson => {
            let (status, raw) = post(server, "/v1/query/stream", sql).await;
            (status != 200 || raw.contains("\"error\"")).then(|| format!("{status}: {raw}"))
        }
        Transport::Ws => {
            let frame = ws_frame(server, sql).await;
            frame.get("error").map(Value::to_string)
        }
        Transport::Native => {
            let mut stream = open_trust_session(server.native_port, "nodedb").await;
            let response = send_sql(&mut stream, 2, sql).await;
            response
                .error
                .map(|error| format!("{}: {}", error.code, error.message))
        }
    }
}

/// The `rows` array of an HTTP `/v1/query` answer that must succeed.
async fn http_rows(server: &TestServer, sql: &str) -> Vec<Value> {
    let (status, raw) = post(server, "/v1/query", sql).await;
    assert_eq!(status, 200, "{sql}: {raw}");
    let body: Value =
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("a JSON answer for {sql}: {e}"));
    body["rows"]
        .as_array()
        .cloned()
        .unwrap_or_else(|| panic!("no `rows` array: {raw}"))
}

/// Create `table` with row `a`.
async fn seeded(server: &TestServer, table: &str) {
    exec(
        server,
        &format!(
            "CREATE COLLECTION {table} (id TEXT PRIMARY KEY, v INT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        server,
        &format!("INSERT INTO {table} (id, v) VALUES ('a', 1)"),
    )
    .await;
}

/// The ids of `table`, in key order.
async fn ids(server: &TestServer, table: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT id FROM {table} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {table}: {e}"))
}

/// The second statement inserts a key that exists. The request answers the
/// error, and the first insert rolls back.
async fn assert_later_error_undoes_the_earlier_write(transport: Transport) {
    let server = TestServer::start().await;
    let table = "whole_t";
    seeded(&server, table).await;
    let sql = format!(
        "INSERT INTO {table} (id, v) VALUES ('b', 2); \
         INSERT INTO {table} (id, v) VALUES ('a', 3)"
    );
    let error = error_of(&server, transport, &sql).await;
    assert!(
        error.is_some(),
        "{transport:?}: the duplicate key must fail"
    );
    assert_eq!(
        ids(&server, table).await,
        vec!["a".to_string()],
        "{transport:?}: the insert of 'b' must roll back, error {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_later_error_undoes_the_earlier_write() {
    assert_later_error_undoes_the_earlier_write(Transport::Http).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ndjson_later_error_undoes_the_earlier_write() {
    assert_later_error_undoes_the_earlier_write(Transport::Ndjson).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_later_error_undoes_the_earlier_write() {
    assert_later_error_undoes_the_earlier_write(Transport::Ws).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_later_error_undoes_the_earlier_write() {
    assert_later_error_undoes_the_earlier_write(Transport::Native).await;
}

/// Two batch inserts answer one count under `inserted`, as a batch insert
/// answers alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_batch_inserts_answer_one_inserted_count_over_http() {
    let server = TestServer::start().await;
    let table = "whole_batch";
    seeded(&server, table).await;
    let sql = format!(
        "INSERT INTO {table} (id, v) VALUES ('b', 2), ('c', 3); \
         INSERT INTO {table} (id, v) VALUES ('d', 4), ('e', 5)"
    );
    let rows = http_rows(&server, &sql).await;
    assert_eq!(rows, vec![serde_json::json!({ "inserted": 4 })], "{sql}");
}

/// A request a statement flagged for the transaction keeps its `affected`
/// answer: each `INSERT .. SELECT` moves a cross-shard sum.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flagged_request_keeps_its_affected_answer_over_http() {
    let server = sum_fixture().await;
    feed(&server, "e5", "acc", "3").await;
    feed(&server, "e6", "acc", "4").await;
    let copy = |id: &str| {
        format!(
            "INSERT INTO ptv_post (id, account_id, amount) \
             SELECT id, account_id, amount FROM ptv_feed WHERE id = '{id}'"
        )
    };
    let sql = format!("{}; {}", copy("e5"), copy("e6"));
    let rows = http_rows(&server, &sql).await;
    assert_eq!(rows, vec![serde_json::json!({ "affected": 2 })], "{sql}");
}

/// A request with a TRUNCATE keeps the per-task route, which answers the
/// TRUNCATE's `truncated` count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_with_a_truncate_answers_its_truncated_count_over_http() {
    let server = TestServer::start().await;
    let table = "whole_trunc";
    seeded(&server, table).await;
    let sql = format!("INSERT INTO {table} (id, v) VALUES ('b', 2); TRUNCATE {table}");
    let rows = http_rows(&server, &sql).await;
    assert!(
        rows.iter().any(|row| row.get("truncated").is_some()),
        "the TRUNCATE must answer its count: {rows:?}"
    );
    assert!(ids(&server, table).await.is_empty(), "TRUNCATE empties it");
}

/// A read after a write to a sum source reads the balance the write moved:
/// the balance move runs right after its statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_after_a_sum_source_write_reads_the_moved_balance_over_http() {
    let server = sum_fixture().await;
    let sql = "UPDATE ptv_post SET amount = '1' WHERE id = 'e0'; \
               SELECT balance FROM ptv_acct WHERE id = 'acc'";
    let rows = http_rows(&server, sql).await;
    assert_eq!(rows, vec![serde_json::json!({ "balance": "6" })], "{sql}");
}

/// A delete of a node with edges beside another write commits. Its stage
/// reads the node's edges, and COMMIT checks them at the snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_of_a_node_with_edges_beside_a_write_commits_over_http() {
    let server = TestServer::start().await;
    let table = "whole_nodes";
    seeded(&server, table).await;
    exec(
        &server,
        &format!("GRAPH INSERT EDGE IN '{table}' FROM 'a' TO 'b' TYPE 'knows'"),
    )
    .await;
    let sql = format!(
        "DELETE FROM {table} WHERE id = 'a'; \
         INSERT INTO {table} (id, v) VALUES ('c', 3)"
    );
    let (status, raw) = post(&server, "/v1/query", &sql).await;
    assert_eq!(status, 200, "{sql}: {raw}");
    assert_eq!(ids(&server, table).await, vec!["c".to_string()]);
}
