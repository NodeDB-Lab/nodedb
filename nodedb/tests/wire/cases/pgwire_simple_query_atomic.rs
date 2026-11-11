// SPDX-License-Identifier: BUSL-1.1

//! A simple query of several statements runs as one implicit transaction, as in PostgreSQL.
//!
//! An error rolls back the earlier writes of the implicit block. A `COMMIT` inside the
//! string ends the block, and a later statement opens a new one. The implicit block
//! answers no `BEGIN` or `COMMIT` tag. A `BEGIN` inside the string opens an explicit block,
//! and an error leaves it aborted.
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
    /// `DataRow` with the text of its first column.
    Row(String),
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
            Ok(SimpleQueryMessage::Row(row)) => {
                replies.push(Reply::Row(row.get(0).unwrap_or("").to_owned()));
            }
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

/// An error at the second statement rolls back the first write.
/// Lever: `execute_simple_query` passes `implicit_block: false`, or
/// `rollback_implicit` returns at once. Either leaves `a`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_rolls_back_the_earlier_write() {
    let server = TestServer::start().await;
    create(&server, "atomic_rollback").await;

    let got = replies(
        &server,
        "INSERT INTO atomic_rollback (id) VALUES ('a'); \
         INSERT INTO atomic_missing (id) VALUES ('b')",
    )
    .await;
    assert_eq!(
        got,
        vec![Reply::Tag(1), Reply::Error("42P01".to_owned())],
        "the client must read `INSERT 0 1`, then the error of the second statement"
    );
    assert!(
        ids(&server, "atomic_rollback").await.is_empty(),
        "the error must roll back the write of the first statement"
    );
}

/// A COMMIT inside the string ends the implicit block before the error.
/// Green on base: each statement autocommits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_inside_the_string_keeps_the_earlier_write() {
    let server = TestServer::start().await;
    create(&server, "atomic_commit").await;

    let got = replies(
        &server,
        "INSERT INTO atomic_commit (id) VALUES ('a'); COMMIT; \
         INSERT INTO atomic_missing (id) VALUES ('b')",
    )
    .await;
    assert_eq!(
        got,
        vec![
            Reply::Tag(1),
            Reply::Tag(0),
            Reply::Error("42P01".to_owned())
        ],
        "the client must read `INSERT 0 1` and `COMMIT`, then the error"
    );
    assert_eq!(
        ids(&server, "atomic_commit").await,
        vec!["a".to_owned()],
        "the COMMIT must keep the write before it"
    );
}

/// A BEGIN inside the string opens an explicit block. An error aborts it and no ROLLBACK runs.
/// Lever: `rollback_implicit` runs whatever block is open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_inside_an_explicit_block_leaves_it_aborted() {
    let server = TestServer::start().await;
    create(&server, "atomic_explicit").await;

    let got = replies(
        &server,
        "BEGIN; INSERT INTO atomic_explicit (id) VALUES ('a'); \
         INSERT INTO atomic_missing (id) VALUES ('b')",
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
        "the aborted explicit block must refuse the next statement"
    );
    server
        .exec("ROLLBACK")
        .await
        .unwrap_or_else(|e| panic!("rollback: {e}"));
    assert!(
        ids(&server, "atomic_explicit").await.is_empty(),
        "ROLLBACK must drop the write of the aborted block"
    );
}

/// Two writes commit at the end of the string, with one tag each and no BEGIN or COMMIT tag.
/// A ROLLBACK after the string must find no open block.
/// Lever: skip the `handle_commit` after the loop. The ROLLBACK drops both writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_writes_commit_with_their_own_tags() {
    let server = TestServer::start().await;
    create(&server, "atomic_two").await;

    let got = replies(
        &server,
        "INSERT INTO atomic_two (id) VALUES ('a'); \
         INSERT INTO atomic_two (id) VALUES ('b')",
    )
    .await;
    assert_eq!(
        got,
        vec![Reply::Tag(1), Reply::Tag(1)],
        "the client must read one tag per statement and no tag of the implicit block"
    );
    server
        .exec("ROLLBACK")
        .await
        .unwrap_or_else(|e| panic!("rollback: {e}"));
    assert_eq!(
        ids(&server, "atomic_two").await,
        vec!["a".to_owned(), "b".to_owned()],
        "the end of the string must commit both writes"
    );
}

/// A read before and after a write answers its rows, and the write commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_around_a_write_answer_their_rows() {
    let server = TestServer::start().await;
    create(&server, "atomic_reads").await;

    let got = replies(
        &server,
        "SELECT 1; INSERT INTO atomic_reads (id) VALUES ('a'); SELECT 2",
    )
    .await;
    assert_eq!(
        got,
        vec![
            Reply::Row("1".to_owned()),
            Reply::Tag(1),
            Reply::Tag(1),
            Reply::Row("2".to_owned()),
            Reply::Tag(1),
        ],
        "the client must read each row and tag in statement order"
    );
    assert_eq!(ids(&server, "atomic_reads").await, vec!["a".to_owned()]);
}

/// 128 UPSERTs in one string land in one implicit block.
/// `sql_multi_statement_batch.rs` sends the same shape and counts the rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_upserts_in_one_string_land() {
    let server = TestServer::start().await;
    create(&server, "atomic_many").await;

    let batch = (0..128)
        .map(|i| format!("UPSERT INTO atomic_many (id, v) VALUES ('k{i}', 'v')"))
        .collect::<Vec<_>>()
        .join("; ");
    let got = replies(&server, &batch).await;
    assert_eq!(got.len(), 128, "one tag per UPSERT: {got:?}");
    assert!(
        got.iter().all(|reply| matches!(reply, Reply::Tag(_))),
        "no UPSERT may answer an error: {got:?}"
    );
    assert_eq!(ids(&server, "atomic_many").await.len(), 128);
}

/// A string of one statement answers the tag alone, or the error alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_statement_answers_its_tag_or_its_error() {
    let server = TestServer::start().await;
    create(&server, "atomic_single").await;

    assert_eq!(
        replies(&server, "INSERT INTO atomic_single (id) VALUES ('a')").await,
        vec![Reply::Tag(1)]
    );
    assert_eq!(
        replies(&server, "INSERT INTO atomic_missing (id) VALUES ('b')").await,
        vec![Reply::Error("42P01".to_owned())]
    );
    assert_eq!(ids(&server, "atomic_single").await, vec!["a".to_owned()]);
}
