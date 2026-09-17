// SPDX-License-Identifier: BUSL-1.1

//! Declared-type coercion for MERGE literals.
//!
//! A literal reaching storage through `INSERT VALUES` or `UPDATE SET` passes
//! the declared-type coercion the planner applies once, in
//! `declared_type_coerce`. A MERGE `WHEN NOT MATCHED THEN INSERT` arm and a
//! `WHEN MATCHED THEN UPDATE SET` arm carry the same literals through the same
//! planner and must be coerced the same way, on every engine that declares
//! columns.

use crate::harness::TestServer;

async fn create_typed_target(server: &TestServer, name: &str, engine: &str, id_type: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id {id_type} PRIMARY KEY, n INT, at TIMESTAMP) \
             WITH (engine='{engine}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name} ({engine}): {e}"));
}

async fn create_typed_source(server: &TestServer, name: &str, engine: &str, id_type: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id {id_type} PRIMARY KEY) WITH (engine='{engine}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name} ({engine}): {e}"));
}

/// `MERGE ... WHEN NOT MATCHED THEN INSERT` runs its literals through the same
/// declared-type coercion an ordinary `INSERT VALUES` gets: a text literal
/// into `n INT` stores an integer, and an epoch-millisecond numeric literal
/// into `at TIMESTAMP` stores that instant, on every engine whose rule
/// plans MERGE (the key-value and columnar rules refuse MERGE outright).
async fn merge_insert_literals_take_the_declared_column_types_on(engine: &str, id_type: &str) {
    let server = TestServer::start().await;
    create_typed_target(&server, "merge_typed_target", engine, id_type).await;
    create_typed_source(&server, "merge_typed_source", engine, id_type).await;

    let source_id = if id_type == "TEXT" { "'1'" } else { "1" };
    server
        .exec(&format!(
            "INSERT INTO merge_typed_source (id) VALUES ({source_id})"
        ))
        .await
        .expect("seed source");

    server
        .exec(
            "MERGE INTO merge_typed_target t \
             USING merge_typed_source s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, n, at) \
             VALUES (s.id, '1', 1583402400000)",
        )
        .await
        .expect("MERGE INSERT of a coercible literal must succeed");

    let rows = server
        .query_rows("SELECT n, at FROM merge_typed_target")
        .await
        .expect("select inserted row");
    assert_eq!(rows.len(), 1, "one row inserted: {rows:?}");
    assert_eq!(
        rows[0],
        vec!["1".to_string(), "2020-03-05T10:00:00.000000Z".to_string()],
        "n and at must take the declared column types: {rows:?}"
    );

    let doubled = server
        .query_rows("SELECT n * 2 FROM merge_typed_target")
        .await
        .expect("select n * 2");
    assert_eq!(
        doubled,
        vec![vec!["2".to_string()]],
        "n must be numeric, not text, for arithmetic to succeed: {doubled:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_insert_literals_take_the_declared_column_types_document_strict() {
    merge_insert_literals_take_the_declared_column_types_on("document_strict", "INT").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_insert_literals_take_the_declared_column_types_document_schemaless() {
    merge_insert_literals_take_the_declared_column_types_on("document_schemaless", "INT").await;
}

/// `MERGE ... WHEN MATCHED THEN UPDATE SET` runs its literals through the
/// same declared-type coercion, on the row it rewrites rather than the row it
/// inserts, on every engine whose rule plans MERGE.
async fn merge_update_literals_take_the_declared_column_types_on(engine: &str) {
    let server = TestServer::start().await;
    create_typed_target(&server, "merge_upd_target", engine, "INT").await;
    create_typed_source(&server, "merge_upd_source", engine, "INT").await;

    server
        .exec("INSERT INTO merge_upd_target (id, n, at) VALUES (1, 0, 1583402400000)")
        .await
        .expect("seed target");
    server
        .exec("INSERT INTO merge_upd_source (id) VALUES (1)")
        .await
        .expect("seed source");

    server
        .exec(
            "MERGE INTO merge_upd_target t \
             USING merge_upd_source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET n = '2', at = 1583402400001",
        )
        .await
        .expect("MERGE UPDATE of coercible literals must succeed");

    let rows = server
        .query_rows("SELECT at FROM merge_upd_target")
        .await
        .expect("select updated row");
    assert_eq!(
        rows,
        vec![vec!["2020-03-05T10:00:00.001000Z".to_string()]],
        "at must take the declared TIMESTAMP type: {rows:?}"
    );

    let doubled = server
        .query_rows("SELECT n * 2 FROM merge_upd_target")
        .await
        .expect("select n * 2");
    assert_eq!(
        doubled,
        vec![vec!["4".to_string()]],
        "n must be numeric, not text, for arithmetic to succeed: {doubled:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_update_literals_take_the_declared_column_types_document_strict() {
    merge_update_literals_take_the_declared_column_types_on("document_strict").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_update_literals_take_the_declared_column_types_document_schemaless() {
    merge_update_literals_take_the_declared_column_types_on("document_schemaless").await;
}

/// A literal the declared column type cannot hold is refused, naming the
/// column, on both a MERGE INSERT arm and a MERGE UPDATE arm — exactly as an
/// ordinary `INSERT VALUES` / `UPDATE SET` refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_refuses_a_literal_the_column_cannot_hold() {
    let server = TestServer::start().await;
    create_typed_target(&server, "merge_bad_target", "document_strict", "INT").await;
    create_typed_source(&server, "merge_bad_source", "document_strict", "INT").await;

    server
        .exec("INSERT INTO merge_bad_source (id) VALUES (1)")
        .await
        .expect("seed source");

    let insert_err = server
        .exec(
            "MERGE INTO merge_bad_target t \
             USING merge_bad_source s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, n, at) \
             VALUES (s.id, 'abc', 1583402400000)",
        )
        .await
        .expect_err("MERGE INSERT of 'abc' into an INT column must be refused");
    assert!(
        insert_err.contains("'n'"),
        "error must name column 'n': {insert_err}"
    );

    server
        .exec("INSERT INTO merge_bad_target (id, n, at) VALUES (1, 0, 1583402400000)")
        .await
        .expect("seed target for the UPDATE arm");

    let update_err = server
        .exec(
            "MERGE INTO merge_bad_target t \
             USING merge_bad_source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET at = true",
        )
        .await
        .expect_err("MERGE UPDATE of a boolean into a TIMESTAMP column must be refused");
    assert!(
        update_err.contains("'at'"),
        "error must name column 'at': {update_err}"
    );
}
