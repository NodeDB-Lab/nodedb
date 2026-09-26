// SPDX-License-Identifier: BUSL-1.1

//! DEFINE EVENT and REMOVE EVENT, end to end.
//!
//! A defined event's THEN action runs for each matching write. After REMOVE
//! EVENT, a write runs no action. Removing an undefined event is an error
//! with SQLSTATE 42704.
//!
//! The server runs one core, and the Event Plane handles that core's events
//! in order. A sentinel write, whose own event logs `sentinel`, lands after
//! any action an earlier write started.

use std::time::Duration;

use crate::harness::TestServer;

/// How long to wait for an event action to land.
const ACTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Poll `ev_log` until it holds a row with id `id`. Fails once
/// `ACTION_TIMEOUT` passes.
async fn wait_for_log(server: &TestServer, id: &str) {
    let deadline = tokio::time::Instant::now() + ACTION_TIMEOUT;
    loop {
        if logged(server, id).await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for '{id}' in ev_log"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn logged(server: &TestServer, id: &str) -> bool {
    let rows = server
        .query_text(&format!("SELECT id FROM ev_log WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("read ev_log: {e}"));
    !rows.is_empty()
}

async fn exec_all(server: &TestServer, statements: &[&str]) {
    for sql in statements {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_event_runs_no_action() {
    let server = TestServer::start().await;
    exec_all(
        &server,
        &[
            "CREATE COLLECTION ev_src",
            "CREATE COLLECTION ev_sentinel",
            "CREATE COLLECTION ev_log",
            "DEFINE EVENT log_insert ON ev_src WHEN INSERT \
             THEN INSERT INTO ev_log (id) VALUES ($document_id)",
            "DEFINE EVENT log_sentinel ON ev_sentinel WHEN INSERT \
             THEN INSERT INTO ev_log (id) VALUES ('sentinel')",
            "INSERT INTO ev_src (id, v) VALUES ('a', 1)",
        ],
    )
    .await;
    wait_for_log(&server, "a").await;

    exec_all(
        &server,
        &[
            "REMOVE EVENT log_insert ON ev_src",
            "INSERT INTO ev_src (id, v) VALUES ('b', 2)",
            "INSERT INTO ev_sentinel (id, v) VALUES ('s', 1)",
        ],
    )
    .await;
    wait_for_log(&server, "sentinel").await;

    assert!(
        !logged(&server, "b").await,
        "a write after REMOVE EVENT must run no action"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_an_undefined_event_is_42704() {
    let server = TestServer::start().await;
    exec_all(&server, &["CREATE COLLECTION ev_none"]).await;
    let error = server
        .client
        .simple_query("REMOVE EVENT missing ON ev_none")
        .await
        .expect_err("an undefined event cannot be removed");
    let code = error
        .as_db_error()
        .map(|db| db.code().code().to_owned())
        .unwrap_or_default();
    assert_eq!(code, "42704", "unexpected error: {error}");
}
