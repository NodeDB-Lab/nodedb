// SPDX-License-Identifier: BUSL-1.1

//! `SET statement_timeout` bounds the statements that follow it.
//!
//! The budget is pinned once at the statement boundary and rides every
//! Control -> Data request envelope the statement fans out into. A statement
//! that goes over stops and the client sees SQLSTATE `57014` instead of rows.
//!
//! ## What this file proves, and what it does not
//!
//! These are end-to-end wire assertions: the session parameter reaches the read
//! path, `0` disables the session limit, a generous budget is inert, and a
//! statement that times out yields an error and nothing else.
//!
//! They deliberately do NOT try to prove WHERE the statement was stopped. A
//! deadline that has already passed when the task is dequeued is refused at
//! admission, and one that passes mid-scan is caught at a safe point inside the
//! handler; both render as `57014`, so no client-visible signal separates them
//! and a test that claimed to tell them apart would be guessing. The
//! mid-execution proof is a unit test that calls the scan handler directly with
//! an already-passed deadline, bypassing admission entirely, so only an
//! in-handler safe point can produce its result — see
//! `data::executor::handlers::columnar_read::scan`.
//!
//! ## Why the budget, not the query, is what makes these deterministic
//!
//! No sleep and no artificial delay, and no bet that a given row count outruns
//! a given budget — that bet fails in both directions and turns a real
//! assertion into a coin flip. The budget instead is the smallest one
//! `statement_timeout` can express, and it is spent before the statement is
//! parsed, planned, authorized or dispatched. No machine finishes that
//! prologue in a microsecond, so every one of these statements is over its
//! deadline by construction.
//!
//! The seed stays several times `stream_chunk_size` so the bounded query is a
//! streaming one — the shape the partial-result assertion is about — without
//! the thousands of rows the shipped chunk size would need.

use crate::harness::TestServer;

/// Rows seeded. Several times `CHUNK_ROWS`, so the scan streams.
const ROWS: usize = 200;
/// `stream_chunk_size` the server boots with.
const CHUNK_ROWS: usize = 20;

/// SQLSTATE `query_canceled` — what a statement over its deadline returns.
const QUERY_CANCELED: &str = "57014";

/// The smallest budget `statement_timeout` can express: one microsecond.
///
/// Unmeetable by construction — see the module doc — so a statement bounded
/// with it stops on any machine at any load.
const UNMEETABLE_BUDGET: &str = "1us";

/// The bounded statement: an unfiltered full scan and materialize.
const SLOW_QUERY: &str = "SELECT id, payload FROM slow_scan ORDER BY payload";

async fn seeded_server() -> TestServer {
    let srv = TestServer::start_with_stream_chunk_size(CHUNK_ROWS).await;
    srv.exec(
        "CREATE COLLECTION slow_scan \
         COLUMNS (id TEXT PRIMARY KEY, payload TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();

    // One statement for the whole seed. The payload prefix descends as `id`
    // ascends, so ORDER BY payload has to reorder every row rather than
    // confirm an order the rows already have.
    let filler = "x".repeat(96);
    let mut sql = String::from("INSERT INTO slow_scan (id, payload) VALUES ");
    for i in 0..ROWS {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("('{i:06}', '{:06}{filler}')", ROWS - i));
    }
    srv.exec(&sql).await.unwrap();
    srv
}

#[tokio::test]
async fn statement_timeout_bounds_a_long_statement() {
    let srv = seeded_server().await;
    srv.exec(&format!("SET statement_timeout = '{UNMEETABLE_BUDGET}'"))
        .await
        .unwrap();

    let error = srv
        .query_rows(SLOW_QUERY)
        .await
        .map(|rows| rows.len())
        .expect_err("a spent budget must stop the statement");
    assert!(
        error.contains(QUERY_CANCELED),
        "expected SQLSTATE {QUERY_CANCELED}, got {error}"
    );
}

#[tokio::test]
async fn statement_timeout_zero_means_no_limit() {
    let srv = seeded_server().await;
    // Prove the same statement is bounded first, so the success below is the
    // `0` doing the work and not a machine that happened to be fast.
    srv.exec(&format!("SET statement_timeout = '{UNMEETABLE_BUDGET}'"))
        .await
        .unwrap();
    assert!(
        srv.query_rows(SLOW_QUERY)
            .await
            .map(|rows| rows.len())
            .is_err(),
        "the first half must bound the statement, or the second half proves nothing"
    );

    srv.exec("SET statement_timeout = 0").await.unwrap();
    let rows = srv
        .query_rows(SLOW_QUERY)
        .await
        .expect("statement_timeout = 0 removes the session limit");
    assert_eq!(rows.len(), ROWS, "every seeded row must come back");
}

#[tokio::test]
async fn statement_well_within_its_timeout_is_unaffected() {
    let srv = seeded_server().await;
    srv.exec("SET statement_timeout = '30s'").await.unwrap();

    let rows = srv
        .query_rows(SLOW_QUERY)
        .await
        .expect("a statement inside its budget must return its rows");
    assert_eq!(rows.len(), ROWS);
}

/// A statement stopped by its deadline returns the error and NOTHING else — no
/// partial row set a client could not tell apart from a complete one — and
/// leaves the collection and the session usable.
///
/// It does not claim to stop the statement mid-stream: which point stops it is
/// a race no client-visible signal reports, and pinning one would be a bet on
/// machine speed. The in-handler safe point has its own deterministic proof in
/// the columnar scan handler's unit test.
#[tokio::test]
async fn timed_out_streaming_statement_returns_no_rows() {
    let srv = seeded_server().await;
    srv.exec(&format!("SET statement_timeout = '{UNMEETABLE_BUDGET}'"))
        .await
        .unwrap();

    match srv.query_rows(SLOW_QUERY).await {
        Ok(rows) => panic!("expected an error, got {} rows", rows.len()),
        Err(error) => assert!(
            error.contains(QUERY_CANCELED),
            "expected SQLSTATE {QUERY_CANCELED}, got {error}"
        ),
    }

    // The collection is intact and the session is usable: the timeout cut the
    // statement, not the data or the connection.
    srv.exec("SET statement_timeout = 0").await.unwrap();
    let rows = srv.query_rows(SLOW_QUERY).await.unwrap();
    assert_eq!(
        rows.len(),
        ROWS,
        "a timed-out read must leave the collection untouched"
    );
}

#[tokio::test]
async fn statement_timeout_accepts_postgres_value_forms() {
    let srv = TestServer::start().await;
    for value in ["0", "250", "1ms", "2s", "1min", "500us"] {
        srv.exec(&format!("SET statement_timeout = '{value}'"))
            .await
            .unwrap_or_else(|e| panic!("SET statement_timeout = '{value}' must be accepted: {e}"));
        assert_eq!(
            srv.query_text("SHOW statement_timeout").await.unwrap(),
            vec![value.to_string()],
            "SHOW must echo the value that was set"
        );
    }
}

#[tokio::test]
async fn statement_timeout_rejects_an_unparsable_value() {
    let srv = TestServer::start().await;
    srv.expect_error("SET statement_timeout = 'whenever'", "22023")
        .await;
    // The refused SET must not have replaced the session's value.
    assert_eq!(
        srv.query_text("SHOW statement_timeout").await.unwrap(),
        vec!["0".to_string()]
    );
}

/// Each statement of a multi-statement message runs under its own budget,
/// as in PostgreSQL 13 and later. A `SET statement_timeout = 0` that leads a
/// message lifts the spent budget the message arrived under, so the scan
/// after it returns every row. A deadline pinned once per message keeps the
/// spent budget and refuses the scan.
#[tokio::test]
async fn each_statement_of_a_message_takes_its_own_budget() {
    let srv = seeded_server().await;
    srv.exec(&format!("SET statement_timeout = '{UNMEETABLE_BUDGET}'"))
        .await
        .unwrap();

    let rows = srv
        .query_rows(&format!("SET statement_timeout = 0; {SLOW_QUERY}"))
        .await
        .expect("the scan runs under the budget the SET before it gave");
    assert_eq!(rows.len(), ROWS, "every seeded row must come back");
}

/// A `SET statement_timeout` inside a message bounds the statements after
/// it in the same message.
#[tokio::test]
async fn a_set_inside_a_message_bounds_the_statements_after_it() {
    let srv = seeded_server().await;
    match srv
        .query_rows(&format!(
            "SET statement_timeout = '{UNMEETABLE_BUDGET}'; {SLOW_QUERY}"
        ))
        .await
    {
        Ok(rows) => panic!("expected an error, got {} rows", rows.len()),
        Err(error) => assert!(
            error.contains(QUERY_CANCELED),
            "expected SQLSTATE {QUERY_CANCELED}, got {error}"
        ),
    }
}
