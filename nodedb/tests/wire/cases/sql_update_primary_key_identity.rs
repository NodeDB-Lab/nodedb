// SPDX-License-Identifier: BUSL-1.1

//! An UPDATE keeps a document's primary key.
//!
//! A document row stays stored under the key its primary key held at insert,
//! and an UPDATE rewrites the body in place. A changed key column would make
//! SQL reads and key reads name different documents, so a write that changes
//! it is refused with `23000`. Assigning the value the key already holds is
//! accepted. Changing a key is a DELETE plus an INSERT.

use crate::harness::TestServer;

/// SQLSTATE of a statement that must fail, or `None` when it succeeded.
async fn sqlstate_of(server: &TestServer, sql: &str) -> Option<String> {
    match server.client.simple_query(sql).await {
        Ok(_) => None,
        Err(e) => Some(
            e.as_db_error()
                .unwrap_or_else(|| panic!("expected a DbError from {sql}, got: {e}"))
                .code()
                .code()
                .to_string(),
        ),
    }
}

async fn assert_key_change_refused(server: &TestServer, sql: &str) {
    let state = sqlstate_of(server, sql)
        .await
        .unwrap_or_else(|| panic!("a primary-key change must be refused, but ran: {sql}"));
    assert_eq!(
        state, "23000",
        "a primary-key change is an integrity violation, got SQLSTATE {state} for: {sql}"
    );
}

/// Create `name` with `CREATE COLLECTION {name}{shape}` and rows `k1` / `k2`.
async fn seeded(server: &TestServer, name: &str, shape: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name}{shape}"))
        .await
        .expect("create collection");
    server
        .exec(&format!(
            "INSERT INTO {name} (id, v) VALUES ('k1', 'one'), ('k2', 'two')"
        ))
        .await
        .expect("seed rows");
}

async fn assert_rows_unchanged(server: &TestServer, name: &str) {
    let ids = server
        .query_text(&format!("SELECT id FROM {name} ORDER BY id"))
        .await
        .expect("scan ids");
    assert_eq!(ids, vec!["k1".to_string(), "k2".to_string()]);
    let by_key = server
        .query_text(&format!("SELECT v FROM {name} WHERE id = 'k1'"))
        .await
        .expect("point read");
    assert_eq!(
        by_key,
        vec!["one".to_string()],
        "k1 stays addressable by its key"
    );
}

const SHAPES: [&str; 3] = [
    "",
    " (id TEXT PRIMARY KEY, v TEXT)",
    " (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_update_refuses_a_new_primary_key() {
    let server = TestServer::start().await;
    for (i, shape) in SHAPES.iter().enumerate() {
        let name = format!("pk_point_{i}");
        seeded(&server, &name, shape).await;
        assert_key_change_refused(
            &server,
            &format!("UPDATE {name} SET id = 'moved' WHERE id = 'k1'"),
        )
        .await;
        assert_rows_unchanged(&server, &name).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn predicate_and_multi_key_updates_refuse_a_new_primary_key() {
    let server = TestServer::start().await;
    for (i, shape) in SHAPES.iter().enumerate() {
        let name = format!("pk_bulk_{i}");
        seeded(&server, &name, shape).await;
        assert_key_change_refused(
            &server,
            &format!("UPDATE {name} SET id = 'moved' WHERE v = 'one'"),
        )
        .await;
        assert_key_change_refused(
            &server,
            &format!("UPDATE {name} SET id = v WHERE id IN ('k1', 'k2')"),
        )
        .await;
        assert_rows_unchanged(&server, &name).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactional_update_refuses_a_new_primary_key() {
    let server = TestServer::start().await;
    seeded(&server, "pk_txn", "").await;
    server.exec("BEGIN").await.expect("begin");
    assert_key_change_refused(&server, "UPDATE pk_txn SET id = 'moved' WHERE id = 'k1'").await;
    server
        .client
        .simple_query("ROLLBACK")
        .await
        .expect("rollback");
    assert_rows_unchanged(&server, "pk_txn").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_update_refuses_a_new_primary_key() {
    let server = TestServer::start().await;
    seeded(&server, "pk_conflict", " (id TEXT PRIMARY KEY, v TEXT)").await;
    assert_key_change_refused(
        &server,
        "INSERT INTO pk_conflict (id, v) VALUES ('k1', 'again') \
         ON CONFLICT (id) DO UPDATE SET id = 'moved'",
    )
    .await;
    assert_rows_unchanged(&server, "pk_conflict").await;
    server
        .exec(
            "INSERT INTO pk_conflict (id, v) VALUES ('k1', 'one') \
             ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, v = EXCLUDED.v",
        )
        .await
        .expect("assigning the conflicting key keeps the identity");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assigning_the_current_primary_key_is_accepted() {
    let server = TestServer::start().await;
    for (i, shape) in SHAPES.iter().enumerate() {
        let name = format!("pk_same_{i}");
        seeded(&server, &name, shape).await;
        server
            .exec(&format!(
                "UPDATE {name} SET id = 'k1', v = 'one' WHERE id = 'k1'"
            ))
            .await
            .expect("assigning the current key keeps the identity");
        server
            .exec(&format!("UPDATE {name} SET id = id WHERE v = 'two'"))
            .await
            .expect("a self-assignment keeps the identity");
        assert_rows_unchanged(&server, &name).await;
    }
}
