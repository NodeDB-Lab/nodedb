// SPDX-License-Identifier: BUSL-1.1

//! A simple query of several statements stops at the first error, as in PostgreSQL.
//! The client reads the responses of the earlier statements, then the ErrorResponse.
//! Statements after the failed one do not run. The implicit transaction rolls back earlier writes.
//!
//! `Client::simple_query_raw` yields each backend message in wire order.
//! A `CommandComplete` arrives as `Ok`, and the ErrorResponse arrives as `Err`.

use futures::StreamExt;
use tokio_postgres::SimpleQueryMessage;

use crate::harness::TestServer;

/// One backend reply of a simple query, in wire order.
#[derive(Debug, PartialEq)]
enum Reply {
    /// `CommandComplete` with the row count tokio-postgres reads from the tag.
    Tag(u64),
    /// `DataRow`.
    Row,
    /// `ErrorResponse` with its SQLSTATE.
    Error(String),
}

/// Every reply of `sql` through the ErrorResponse, in wire order.
async fn replies(server: &TestServer, sql: &str) -> Vec<Reply> {
    let stream = server
        .client
        .simple_query_raw(sql)
        .await
        .unwrap_or_else(|e| panic!("send {sql}: {e}"));
    let mut stream = Box::pin(stream);
    let mut replies = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(SimpleQueryMessage::CommandComplete(n)) => replies.push(Reply::Tag(n)),
            Ok(SimpleQueryMessage::Row(_)) => replies.push(Reply::Row),
            Ok(_) => {}
            Err(e) => {
                let code = e
                    .as_db_error()
                    .map(|db| db.code().code().to_owned())
                    .unwrap_or_else(|| panic!("{sql} answered a client-side error: {e}"));
                replies.push(Reply::Error(code));
                break;
            }
        }
    }
    replies
}

async fn ids(server: &TestServer, collection: &str) -> Vec<String> {
    let mut ids = server
        .query_text(&format!("SELECT id FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"));
    ids.sort();
    ids
}

async fn create(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, v TEXT)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
}

/// The tag of the first statement reaches the client before the error of the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_at_second_statement_follows_first_tag() {
    let server = TestServer::start().await;
    create(&server, "err_tags_second").await;

    let got = replies(
        &server,
        "INSERT INTO err_tags_second (id) VALUES ('a'); \
         INSERT INTO err_tags_missing (id) VALUES ('b')",
    )
    .await;
    assert_eq!(
        got,
        vec![Reply::Tag(1), Reply::Error("42P01".to_owned())],
        "the client must read `INSERT 0 1`, then the error of the second statement"
    );
    assert!(
        ids(&server, "err_tags_second").await.is_empty(),
        "the error rolls back the implicit transaction and its first write"
    );
}

/// The row set of an earlier SELECT reaches the client before the error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_after_select_follows_its_rows() {
    let server = TestServer::start().await;
    create(&server, "err_tags_rows").await;
    server
        .exec("INSERT INTO err_tags_rows (id) VALUES ('r')")
        .await
        .unwrap_or_else(|e| panic!("seed: {e}"));

    let got = replies(
        &server,
        "SELECT id FROM err_tags_rows; SELECT id FROM err_tags_missing",
    )
    .await;
    assert_eq!(
        got,
        vec![Reply::Row, Reply::Tag(1), Reply::Error("42P01".to_owned())],
        "the client must read the row and `SELECT 1`, then the error"
    );
}

/// A query of one failing statement answers the ErrorResponse alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_failing_statement_answers_error_only() {
    let server = TestServer::start().await;

    let got = replies(&server, "INSERT INTO err_tags_missing (id) VALUES ('b')").await;
    assert_eq!(got, vec![Reply::Error("42P01".to_owned())]);
}

/// A failed first statement answers the error alone, and the second never runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_first_statement_skips_the_rest() {
    let server = TestServer::start().await;
    create(&server, "err_tags_first").await;

    let got = replies(
        &server,
        "INSERT INTO err_tags_missing (id) VALUES ('b'); \
         INSERT INTO err_tags_first (id) VALUES ('c')",
    )
    .await;
    assert_eq!(got, vec![Reply::Error("42P01".to_owned())]);
    assert!(
        ids(&server, "err_tags_first").await.is_empty(),
        "the statement after the failed one must not run"
    );
}

/// An error inside an explicit block after earlier statements aborts the block.
/// The client reads the earlier tags, then the error, then 25P02 for the next query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_inside_explicit_block_aborts_it() {
    let server = TestServer::start().await;
    create(&server, "err_tags_block").await;

    let got = replies(
        &server,
        "BEGIN; INSERT INTO err_tags_block (id) VALUES ('d'); \
         SELECT id FROM err_tags_missing",
    )
    .await;
    assert_eq!(
        got,
        vec![
            Reply::Tag(0),
            Reply::Tag(1),
            Reply::Error("42P01".to_owned())
        ],
        "the client must read `BEGIN` and `INSERT 0 1`, then the error"
    );
    assert_eq!(
        replies(&server, "SELECT 1").await,
        vec![Reply::Error("25P02".to_owned())],
        "the aborted block must refuse the next statement"
    );
    server
        .exec("ROLLBACK")
        .await
        .unwrap_or_else(|e| panic!("rollback: {e}"));
    assert!(
        ids(&server, "err_tags_block").await.is_empty(),
        "ROLLBACK must drop the write of the aborted block"
    );
}
