// SPDX-License-Identifier: BUSL-1.1

//! An autocommit write on one vShard whose `RETURNING` list can fail commits
//! through Calvin over native and HTTP, as in PostgreSQL: an error in the
//! list fails the statement and stores nothing.
//!
//! Each divisor reads a row column, so only the evaluation of the written
//! row can raise the error.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::Value;
use nodedb_types::protocol::NativeResponse;
use tokio::net::TcpStream;

use crate::harness::TestServer;

/// A native session on `server`.
pub(super) async fn native(server: &TestServer) -> TcpStream {
    open_trust_session(server.native_port, "nodedb").await
}

/// The SQLSTATE and message `sql` fails with over native.
pub(super) async fn native_error(stream: &mut TcpStream, seq: u64, sql: &str) -> (String, String) {
    let response = send_sql(stream, seq, sql).await;
    let error = response
        .error
        .unwrap_or_else(|| panic!("the statement fails: {sql}"));
    (error.code, error.message)
}

/// Assert `sql` fails over HTTP with a message that contains `expected`.
pub(super) async fn http_error(server: &TestServer, sql: &str, expected: &str) {
    let (status, body) = server.http_query(sql).await;
    assert!(
        !(200..300).contains(&status),
        "the statement fails: {sql}: {status} {body}"
    );
    assert!(
        body.to_lowercase().contains(&expected.to_lowercase()),
        "{sql}: {body}"
    );
}

/// The one cell `response` answers.
pub(super) fn only_value(response: NativeResponse) -> Value {
    assert!(response.error.is_none(), "{:?}", response.error);
    match response.rows.as_deref() {
        Some([row]) if row.len() == 1 => row[0].clone(),
        rows => panic!("one cell, got {rows:?}"),
    }
}

/// The rows `sql` reads over pgwire, each as its columns joined by tab,
/// sorted.
pub(super) async fn rows_of(server: &TestServer, sql: &str) -> Vec<String> {
    let mut rows = server
        .query_text_joined(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.sort();
    rows
}

async fn create(server: &TestServer, ddl: &str) {
    server
        .exec(ddl)
        .await
        .unwrap_or_else(|e| panic!("{ddl}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_list_on_an_insert_stores_nothing() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION rv_ins (id TEXT PRIMARY KEY, n INT)",
    )
    .await;

    let mut stream = native(&server).await;
    let (code, message) = native_error(
        &mut stream,
        2,
        "INSERT INTO rv_ins (id, n) VALUES ('a', 5) RETURNING 1 / (n - n)",
    )
    .await;
    assert_eq!(code, "22012", "{message}");
    http_error(
        &server,
        "INSERT INTO rv_ins (id, n) VALUES ('b', 5) RETURNING 1 / (n - n)",
        "division by zero",
    )
    .await;

    assert!(
        rows_of(&server, "SELECT id FROM rv_ins").await.is_empty(),
        "the refusals stored nothing"
    );
}

/// The list draws `nextval('s')`, then the division fails. The drawn value
/// stands: the session's `currval` is 1 and the next `nextval` is 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_list_keeps_the_values_it_drew() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION rv_seq (id TEXT PRIMARY KEY, n INT)",
    )
    .await;
    create(&server, "CREATE SEQUENCE s").await;
    create(&server, "CREATE SEQUENCE s_http").await;

    let mut stream = native(&server).await;
    let (code, message) = native_error(
        &mut stream,
        2,
        "INSERT INTO rv_seq (id, n) VALUES ('a', 5) RETURNING nextval('s'), 1 / (n - n)",
    )
    .await;
    assert_eq!(code, "22012", "{message}");
    let currval = only_value(send_sql(&mut stream, 3, "SELECT currval('s')").await);
    assert_eq!(currval, Value::Integer(1), "the refused list drew 1");
    let next = only_value(send_sql(&mut stream, 4, "SELECT nextval('s')").await);
    assert_eq!(next, Value::Integer(2), "the drawn value stands");

    // An HTTP request holds no session, so only the sequence shows the draw.
    http_error(
        &server,
        "INSERT INTO rv_seq (id, n) VALUES ('b', 5) RETURNING nextval('s_http'), 1 / (n - n)",
        "division by zero",
    )
    .await;
    let next = only_value(send_sql(&mut stream, 5, "SELECT nextval('s_http')").await);
    assert_eq!(next, Value::Integer(2), "the HTTP refusal drew 1");

    assert!(rows_of(&server, "SELECT id FROM rv_seq").await.is_empty());
}

/// Three rows on one vShard pass `max_returning_rows = 2`. The update fails
/// with `54000` and changes no row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_past_the_row_limit_changes_no_row() {
    let server = TestServer::start_multicores_with_max_returning_rows(2, 2).await;
    create(
        &server,
        "CREATE COLLECTION rv_lim (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')",
    )
    .await;
    server
        .exec("INSERT INTO rv_lim (id, n) VALUES ('r1', 0), ('r2', 0), ('r3', 0)")
        .await
        .expect("seed rv_lim");

    let mut stream = native(&server).await;
    let (code, message) = native_error(
        &mut stream,
        2,
        "UPDATE rv_lim SET n = 5 WHERE n = 0 RETURNING id",
    )
    .await;
    assert_eq!(code, "54000", "{message}");
    assert!(message.contains("exceeds limit 2"), "{message}");
    http_error(
        &server,
        "UPDATE rv_lim SET n = 5 WHERE n = 0 RETURNING id",
        "exceeds limit 2",
    )
    .await;

    assert_eq!(
        rows_of(&server, "SELECT id, n FROM rv_lim").await,
        ["r1\t0", "r2\t0", "r3\t0"],
        "no row changed"
    );
}

/// Each collection with rows `r1` and `r2`, both `n = 0`. One document
/// collection and one KV collection, each on one vShard.
const PREDICATE_COLLECTIONS: [(&str, &str); 2] = [
    (
        "rv_pred_doc",
        "CREATE COLLECTION rv_pred_doc (id TEXT PRIMARY KEY, n INT) \
         WITH (engine='document_strict')",
    ),
    (
        "rv_pred_kv",
        "CREATE COLLECTION rv_pred_kv (id TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
    ),
];

/// An update and a delete by predicate fail in their list over native and
/// HTTP. Neither changes a row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_write_with_a_failing_list_changes_nothing() {
    let server = TestServer::start().await;
    let mut stream = native(&server).await;
    let mut seq = 2;
    for (collection, ddl) in PREDICATE_COLLECTIONS {
        create(&server, ddl).await;
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, n) VALUES ('r1', 0), ('r2', 0)"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection}: {e}"));

        for sql in [
            format!("UPDATE {collection} SET n = 5 WHERE n = 0 RETURNING 1 / (n - n)"),
            format!("DELETE FROM {collection} WHERE n = 0 RETURNING 1 / (n - n)"),
        ] {
            let (code, message) = native_error(&mut stream, seq, &sql).await;
            seq += 1;
            assert_eq!(code, "22012", "{sql}: {message}");
            http_error(&server, &sql, "division by zero").await;
        }

        assert_eq!(
            rows_of(&server, &format!("SELECT id, n FROM {collection}")).await,
            ["r1\t0", "r2\t0"],
            "{collection}: no row changed"
        );
    }
}
