// SPDX-License-Identifier: BUSL-1.1

//! A guarded UPDATE together with another write in one transaction commits.
//!
//! `WHERE id = 'b1' AND state = 'pending'` names no primary-key point, so the
//! UPDATE plans as a predicate write. With an INSERT on another collection the
//! commit spans vShards and goes through Calvin. The staged predicate write
//! must carry its predicted rows into the Calvin stage, or every COMMIT aborts
//! with a retry hint that no retry satisfies. Both protocols run the exact
//! statements from the report.

use crate::harness::TestServer;

async fn setup(server: &TestServer, bk: &str, le: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {bk} (id TEXT PRIMARY KEY, state TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE COLLECTION {le} (id TEXT PRIMARY KEY, amount_sen BIGINT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {bk} (id, state) VALUES ('b1', 'pending')"
        ))
        .await
        .unwrap();
}

async fn assert_committed(server: &TestServer, bk: &str, le: &str) {
    let state = server
        .query_text(&format!("SELECT state FROM {bk} WHERE id = 'b1'"))
        .await
        .unwrap();
    assert_eq!(
        state,
        vec!["confirmed".to_string()],
        "the guarded UPDATE applied"
    );
    let amount = server
        .query_text(&format!("SELECT amount_sen FROM {le} WHERE id = 'e1'"))
        .await
        .unwrap();
    assert_eq!(amount, vec!["1".to_string()], "the INSERT applied");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guarded_update_with_another_write_commits_simple_protocol() {
    let server = TestServer::start().await;
    setup(&server, "guard_bk_s", "guard_le_s").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO guard_le_s (id, amount_sen) VALUES ('e1', 1)")
        .await
        .unwrap();
    server
        .exec("UPDATE guard_bk_s SET state = 'confirmed' WHERE id = 'b1' AND state = 'pending'")
        .await
        .unwrap();
    server
        .exec("COMMIT")
        .await
        .unwrap_or_else(|e| panic!("COMMIT of a guarded UPDATE plus an INSERT failed: {e}"));

    assert_committed(&server, "guard_bk_s", "guard_le_s").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guarded_update_with_another_write_commits_extended_protocol() {
    let server = TestServer::start().await;
    setup(&server, "guard_bk_x", "guard_le_x").await;

    let client = &server.client;
    client.execute("BEGIN", &[]).await.unwrap();
    client
        .execute(
            "INSERT INTO guard_le_x (id, amount_sen) VALUES ($1, $2)",
            &[&"e1", &1_i64],
        )
        .await
        .unwrap();
    client
        .execute(
            "UPDATE guard_bk_x SET state = 'confirmed' WHERE id = $1 AND state = $2",
            &[&"b1", &"pending"],
        )
        .await
        .unwrap();
    client
        .execute("COMMIT", &[])
        .await
        .unwrap_or_else(|e| panic!("COMMIT of a guarded UPDATE plus an INSERT failed: {e}"));

    assert_committed(&server, "guard_bk_x", "guard_le_x").await;
}
