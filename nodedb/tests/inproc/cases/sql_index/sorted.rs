// SPDX-License-Identifier: BUSL-1.1

//! `WHERE indexed = value ORDER BY ...` reads through the index.
//!
//! The planner used to refuse the index for any ORDER BY, so a lookup of the
//! newest rows of one session scanned the whole collection. The index lookup
//! now carries the sort keys. These tests check the plan shape and that the
//! rows come back ordered, cut by OFFSET and LIMIT after the sort, and
//! filtered by the residual predicate.

use super::helpers::explain_lower;
use nodedb_test_support::pgwire_harness::TestServer;

async fn seed_strict(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION sorted_turns (\
               id STRING PRIMARY KEY, \
               session_id STRING NOT NULL, \
               seq INT NOT NULL, \
               note STRING) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE INDEX ON sorted_turns(session_id)")
        .await
        .unwrap();
    // Session s1 holds seq 1..=8 inserted out of order. Session s2 must
    // never appear in an s1 read.
    for seq in [5, 2, 8, 1, 7, 3, 6, 4] {
        server
            .exec(&format!(
                "INSERT INTO sorted_turns (id, session_id, seq, note) \
                 VALUES ('a{seq}', 's1', {seq}, 'n{seq}')"
            ))
            .await
            .unwrap();
    }
    for seq in [90, 91, 92] {
        server
            .exec(&format!(
                "INSERT INTO sorted_turns (id, session_id, seq, note) \
                 VALUES ('b{seq}', 's2', {seq}, 'n{seq}')"
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_on_an_indexed_equality_plans_the_index() {
    let server = TestServer::start().await;
    seed_strict(&server).await;

    let plan = explain_lower(
        &server,
        "SELECT id FROM sorted_turns WHERE session_id = 's1' ORDER BY seq DESC LIMIT 3",
    )
    .await;
    assert!(
        plan.contains("indexedfetch"),
        "ORDER BY must keep the index, got: {plan}"
    );
    assert!(
        plan.contains("sort_keys"),
        "the indexed fetch must carry the sort keys, got: {plan}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn descending_limit_returns_the_newest_rows_in_order() {
    let server = TestServer::start().await;
    seed_strict(&server).await;

    let rows = server
        .query_text("SELECT id FROM sorted_turns WHERE session_id = 's1' ORDER BY seq DESC LIMIT 3")
        .await
        .unwrap();
    assert_eq!(rows, vec!["a8", "a7", "a6"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ascending_order_returns_every_row_of_the_session() {
    let server = TestServer::start().await;
    seed_strict(&server).await;

    let rows = server
        .query_text("SELECT id FROM sorted_turns WHERE session_id = 's1' ORDER BY seq")
        .await
        .unwrap();
    assert_eq!(rows, vec!["a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offset_and_limit_cut_the_sorted_rows() {
    let server = TestServer::start().await;
    seed_strict(&server).await;

    // The first two rows by seq are skipped after the sort, not before it.
    let rows = server
        .query_text(
            "SELECT id FROM sorted_turns WHERE session_id = 's1' \
             ORDER BY seq LIMIT 3 OFFSET 2",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec!["a3", "a4", "a5"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_residual_filter_applies_before_the_sort_cut() {
    let server = TestServer::start().await;
    seed_strict(&server).await;

    let rows = server
        .query_text(
            "SELECT id FROM sorted_turns WHERE session_id = 's1' AND seq > 3 \
             ORDER BY seq DESC LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec!["a8", "a7"]);

    let rows = server
        .query_text(
            "SELECT id FROM sorted_turns WHERE session_id = 's1' AND seq < 4 \
             ORDER BY seq DESC LIMIT 5",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec!["a3", "a2", "a1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_order_by_on_the_primary_key_orders_by_identity() {
    let server = TestServer::start().await;
    seed_strict(&server).await;

    let rows = server
        .query_text("SELECT id FROM sorted_turns WHERE session_id = 's2' ORDER BY id DESC")
        .await
        .unwrap();
    assert_eq!(rows, vec!["b92", "b91", "b90"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_written_after_the_index_exists_sort_with_the_committed_rows() {
    let server = TestServer::start().await;
    seed_strict(&server).await;

    server
        .exec(
            "INSERT INTO sorted_turns (id, session_id, seq, note) \
             VALUES ('a9', 's1', 9, 'n9')",
        )
        .await
        .unwrap();
    server
        .exec("DELETE FROM sorted_turns WHERE id = 'a8'")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM sorted_turns WHERE session_id = 's1' ORDER BY seq DESC LIMIT 2")
        .await
        .unwrap();
    assert_eq!(rows, vec!["a9", "a7"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_sees_its_own_staged_row_in_order() {
    let server = TestServer::start().await;
    seed_strict(&server).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "INSERT INTO sorted_turns (id, session_id, seq, note) \
             VALUES ('a10', 's1', 10, 'n10')",
        )
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT id FROM sorted_turns WHERE session_id = 's1' ORDER BY seq DESC LIMIT 2")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();
    assert_eq!(rows, vec!["a10", "a8"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_schemaless_collection_orders_through_the_index() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION sorted_docs").await.unwrap();
    server
        .exec("CREATE INDEX ON sorted_docs(kind)")
        .await
        .unwrap();
    for (id, kind, rank) in [
        ("d1", "x", 3),
        ("d2", "x", 1),
        ("d3", "y", 9),
        ("d4", "x", 2),
    ] {
        server
            .exec(&format!(
                "INSERT INTO sorted_docs {{ id: '{id}', kind: '{kind}', rank: {rank} }}"
            ))
            .await
            .unwrap();
    }

    let rows = server
        .query_text("SELECT id FROM sorted_docs WHERE kind = 'x' ORDER BY rank DESC")
        .await
        .unwrap();
    assert_eq!(rows, vec!["d1", "d4", "d2"]);
}
