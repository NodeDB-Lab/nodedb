// SPDX-License-Identifier: BUSL-1.1

//! Both sides of a LATERAL join read the statement's `AS OF SYSTEM TIME`.
//!
//! A write after the cut stays out of the inner side, on the top-k and the
//! loop shape. The history window check reaches the inner collection.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::harness::TestServer;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

/// One user with one event before the returned cut and one event after it.
async fn seed(server: &TestServer, bitemporal_inner: bool) -> i64 {
    server
        .exec(
            "CREATE COLLECTION asof_users (id TEXT PRIMARY KEY, created_at BIGINT) \
             WITH (engine='document_strict', bitemporal=true)",
        )
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE COLLECTION asof_events (id TEXT PRIMARY KEY, user_id TEXT, score BIGINT) \
             WITH (engine='document_strict', bitemporal={bitemporal_inner})"
        ))
        .await
        .unwrap();
    server
        .exec("INSERT INTO asof_users (id, created_at) VALUES ('u1', 0)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO asof_events (id, user_id, score) VALUES ('before', 'u1', 1)")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cut = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;
    server
        .exec("INSERT INTO asof_events (id, user_id, score) VALUES ('after', 'u1', 2)")
        .await
        .unwrap();
    cut
}

fn top_k(as_of: &str) -> String {
    format!(
        "SELECT u.id FROM asof_users u, LATERAL (\
             SELECT e.id FROM asof_events e WHERE e.user_id = u.id \
             ORDER BY e.score DESC LIMIT 5\
         ) best {as_of}"
    )
}

fn nested_loop(as_of: &str) -> String {
    format!(
        "SELECT u.id FROM asof_users u JOIN LATERAL (\
             SELECT e.id FROM asof_events e WHERE e.score > u.created_at\
         ) recent ON true {as_of}"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_inner_side_reads_the_statement_cut() {
    let server = TestServer::start().await;
    let cut = seed(&server, true).await;

    for (shape, sql) in [
        ("top-k", top_k as fn(&str) -> String),
        ("loop", nested_loop),
    ] {
        let current = server.query_text(&sql("")).await.unwrap();
        assert_eq!(current.len(), 2, "{shape}: the current state holds both");

        let at_cut = server
            .query_text(&sql(&format!("AS OF SYSTEM TIME {cut}")))
            .await
            .unwrap();
        assert_eq!(
            at_cut.len(),
            1,
            "{shape}: the event written after the cut stays out: {at_cut:?}"
        );
    }
}

/// The inner collection keeps no audit history, so a cut older than the
/// snapshot age limit is refused on its account. The outer side alone keeps
/// such a history.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_history_window_covers_the_inner_collection() {
    let server = TestServer::start().await;
    seed(&server, false).await;

    for sql in [
        top_k("AS OF SYSTEM TIME 1000"),
        nested_loop("AS OF SYSTEM TIME 1000"),
    ] {
        server.expect_error(&sql, "asof_events").await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_versions_is_refused() {
    let server = TestServer::start().await;
    seed(&server, true).await;

    server
        .expect_error(&top_k("AS OF SYSTEM TIME NULL"), "AS OF SYSTEM TIME NULL")
        .await;
}
