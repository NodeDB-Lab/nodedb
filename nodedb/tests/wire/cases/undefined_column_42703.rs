// SPDX-License-Identifier: BUSL-1.1

//! Unknown column references raise `42703` (undefined_column) at plan time.
//!
//! A reference that names nothing in scope used to plan as a field lookup and
//! evaluate to `NULL` per row — silently wrong `WHERE` row sets, no-op
//! `ORDER BY`, zero-row `UPDATE`/`DELETE`. These tests pin the plan-time gate
//! across every clause shape and engine, plus the one deliberate carve-out:
//! a schemaless collection that declares no fields keeps resolving dynamic
//! fields (the `NULL` fold is the documented behavior there), including when
//! its primary key was renamed.

use crate::harness::TestServer;

fn assert_42703(result: &Result<(), String>, collection: &str, column: &str) {
    let message = match result {
        Ok(()) => panic!("expected 42703 for {collection}.{column}, statement succeeded"),
        Err(message) => message,
    };
    assert!(
        message.contains("42703"),
        "expected SQLSTATE 42703 for {collection}.{column}, got: {message}"
    );
    assert!(
        message.contains(column),
        "error must name the column '{column}': {message}"
    );
}

async fn create(server: &TestServer, name: &str, columns: &str, engine: Option<&str>) {
    let stmt = match engine {
        Some(e) => format!("CREATE COLLECTION {name} ({columns}) WITH (engine = '{e}')"),
        None => format!("CREATE COLLECTION {name} ({columns})"),
    };
    server
        .exec(&stmt)
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// The issue's repro shape: a default-engine collection with declared columns
/// must raise on a typo in every clause — projection, WHERE (equality and
/// NULL test), ORDER BY, UPDATE predicate, DELETE predicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_default_document_raises_42703_in_every_clause() {
    let server = TestServer::start().await;
    create(&server, "u42703_probe", "id INT PRIMARY KEY, x INT", None).await;
    server
        .exec("INSERT INTO u42703_probe (id, x) VALUES (1, 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO u42703_probe (id, x) VALUES (2, 2)")
        .await
        .unwrap();

    assert_42703(
        &server
            .exec("SELECT nonexistent_col FROM u42703_probe")
            .await,
        "u42703_probe",
        "nonexistent_col",
    );
    assert_42703(
        &server
            .exec("SELECT count(*) FROM u42703_probe WHERE nonexistent_col = 1")
            .await,
        "u42703_probe",
        "nonexistent_col",
    );
    assert_42703(
        &server
            .exec("SELECT count(*) FROM u42703_probe WHERE nonexistent_col IS NULL")
            .await,
        "u42703_probe",
        "nonexistent_col",
    );
    assert_42703(
        &server
            .exec("SELECT x FROM u42703_probe ORDER BY nonexistent_col")
            .await,
        "u42703_probe",
        "nonexistent_col",
    );
    assert_42703(
        &server
            .exec("UPDATE u42703_probe SET x = 99 WHERE nonexistent_col = 1")
            .await,
        "u42703_probe",
        "nonexistent_col",
    );
    assert_42703(
        &server
            .exec("DELETE FROM u42703_probe WHERE nonexistent_col = 1")
            .await,
        "u42703_probe",
        "nonexistent_col",
    );

    // The valid counterpart must still answer.
    let rows = server
        .query_named_rows("SELECT id, x FROM u42703_probe ORDER BY id")
        .await
        .expect("valid projection must pass");
    assert_eq!(rows.len(), 2, "two rows survive: {rows:?}");
}

/// Fixed-schema engines fold identically on main; all must raise now.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_kv_columnar_raise_42703() {
    let server = TestServer::start().await;

    create(
        &server,
        "u42703_strict",
        "a INT4 PRIMARY KEY, b INT8",
        Some("document_strict"),
    )
    .await;
    server
        .exec("INSERT INTO u42703_strict (a, b) VALUES (1, 2)")
        .await
        .unwrap();
    assert_42703(
        &server.exec("SELECT ghost FROM u42703_strict").await,
        "u42703_strict",
        "ghost",
    );
    assert_42703(
        &server
            .exec("SELECT count(*) FROM u42703_strict WHERE ghost = 5")
            .await,
        "u42703_strict",
        "ghost",
    );

    create(
        &server,
        "u42703_kv",
        "k TEXT PRIMARY KEY, v TEXT",
        Some("kv"),
    )
    .await;
    server
        .exec("INSERT INTO u42703_kv (k, v) VALUES ('a', '1')")
        .await
        .unwrap();
    assert_42703(
        &server.exec("SELECT ghost FROM u42703_kv").await,
        "u42703_kv",
        "ghost",
    );
    assert_42703(
        &server
            .exec("UPDATE u42703_kv SET v = '2' WHERE ghost = 1")
            .await,
        "u42703_kv",
        "ghost",
    );

    create(
        &server,
        "u42703_col",
        "id INT PRIMARY KEY, x INT",
        Some("columnar"),
    )
    .await;
    server
        .exec("INSERT INTO u42703_col (id, x) VALUES (1, 1)")
        .await
        .unwrap();
    assert_42703(
        &server.exec("SELECT ghost FROM u42703_col").await,
        "u42703_col",
        "ghost",
    );
}

/// The carve-out: a schemaless collection that declares no fields keeps
/// resolving dynamic fields — reads fold to NULL for missing ones and return
/// values for present ones, with no error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undeclared_schemaless_keeps_dynamic_field_fold() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION u42703_open WITH (engine = 'document_schemaless')")
        .await
        .unwrap();

    server
        .exec("INSERT INTO u42703_open (id, dyn_a) VALUES ('r1', 42)")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT dyn_a FROM u42703_open")
        .await
        .expect("dynamic field read must not raise");
    assert_eq!(rows.len(), 1, "one row: {rows:?}");
    assert_eq!(
        rows[0].get("dyn_a").map(String::as_str),
        Some("42"),
        "dynamic field value must resolve: {rows:?}"
    );

    // A missing dynamic field still folds to NULL (pre-existing behavior).
    let counts = server
        .query_named_rows("SELECT count(*) AS n FROM u42703_open WHERE missing IS NULL")
        .await
        .expect("fold over missing dynamic field must not raise");
    assert_eq!(
        counts[0].get("n").map(String::as_str),
        Some("1"),
        "{counts:?}"
    );
}

/// The open/closed line is the declared column count, not the key name: a
/// renamed primary key on an otherwise undeclared schemaless collection
/// keeps the fold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renamed_pk_only_schemaless_stays_open() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION u42703_sku WITH (engine = 'document_schemaless', primary_key = 'sku')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO u42703_sku (sku, dyn_b) VALUES ('s1', 5)")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT dyn_b FROM u42703_sku")
        .await
        .expect("dynamic field read under renamed PK must not raise");
    assert_eq!(
        rows[0].get("dyn_b").map(String::as_str),
        Some("5"),
        "{rows:?}"
    );
}
