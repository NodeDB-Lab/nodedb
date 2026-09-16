// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Document (strict) engine. `CREATE TABLE`
//! defaults to document_strict mode (Binary Tuple storage, schema
//! enforced). Covers typed schema, index on typed column, upsert, delete,
//! count, and WAL restart durability.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_insert_typed_schema() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE strict_basic (id TEXT PRIMARY KEY, name TEXT, score FLOAT)")
        .await
        .unwrap();

    srv.exec("INSERT INTO strict_basic (id, name, score) VALUES ('s1', 'Alice', 9.5)")
        .await
        .unwrap();
    srv.exec("INSERT INTO strict_basic (id, name, score) VALUES ('s2', 'Bob', 7.2)")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id, name FROM strict_basic ORDER BY name")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], "Alice");
    assert_eq!(rows[1][1], "Bob");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_on_typed_column() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE strict_idx (id TEXT PRIMARY KEY, region TEXT, value INT)")
        .await
        .unwrap();
    srv.exec("CREATE INDEX ON strict_idx (region)")
        .await
        .unwrap();

    srv.exec("INSERT INTO strict_idx (id, region, value) VALUES ('i1', 'us', 100)")
        .await
        .unwrap();
    srv.exec("INSERT INTO strict_idx (id, region, value) VALUES ('i2', 'eu', 200)")
        .await
        .unwrap();
    srv.exec("INSERT INTO strict_idx (id, region, value) VALUES ('i3', 'us', 150)")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM strict_idx WHERE region = 'us' ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "i1");
    assert_eq!(rows[1][0], "i3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_updates_field() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE strict_upsert (id TEXT PRIMARY KEY, status TEXT)")
        .await
        .unwrap();

    srv.exec("INSERT INTO strict_upsert (id, status) VALUES ('u1', 'pending')")
        .await
        .unwrap();
    srv.exec("UPSERT INTO strict_upsert (id, status) VALUES ('u1', 'done')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT status FROM strict_upsert WHERE id = 'u1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "done");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_removes_row() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE strict_del (id TEXT PRIMARY KEY, label TEXT)")
        .await
        .unwrap();

    srv.exec("INSERT INTO strict_del (id, label) VALUES ('d1', 'keep')")
        .await
        .unwrap();
    srv.exec("INSERT INTO strict_del (id, label) VALUES ('d2', 'remove')")
        .await
        .unwrap();
    srv.exec("DELETE FROM strict_del WHERE id = 'd2'")
        .await
        .unwrap();

    let rows = srv.query_rows("SELECT id FROM strict_del").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "d1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn count_aggregation() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE strict_cnt (id TEXT PRIMARY KEY, v INT)")
        .await
        .unwrap();

    for i in 0..4u32 {
        srv.exec(&format!(
            "INSERT INTO strict_cnt (id, v) VALUES ('c{i}', {i})"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM strict_cnt")
        .await
        .unwrap();
    assert_eq!(rows[0][0].parse::<u32>().unwrap(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_restart_durability() {
    let srv = TestServer::start().await;
    srv.exec("CREATE TABLE strict_wal (id TEXT PRIMARY KEY, data TEXT)")
        .await
        .unwrap();
    srv.exec("INSERT INTO strict_wal (id, data) VALUES ('w1', 'persisted')")
        .await
        .unwrap();

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;

    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    let rows = srv2
        .query_rows("SELECT data FROM strict_wal WHERE id = 'w1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "persisted");
}

/// Create a `document_strict` collection with a `VECTOR(3)` column, the shape
/// the `ARRAY[...]` literal case needs.
async fn create_strict_vector(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, embedding VECTOR(3)) \
             WITH (engine = 'document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// The planner folds `ARRAY[0.1, 0.2, 0.3]` to `Decimal` elements. Coercion
/// reads Float, Integer, Decimal and numeric String elements, so the literal
/// keeps its element count and reaches the column intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_strict_inserts_a_decimal_array_literal_into_a_vector_column() {
    let server = TestServer::start().await;
    create_strict_vector(&server, "vec_array_literal").await;

    server
        .exec("INSERT INTO vec_array_literal (id, embedding) VALUES ('a1', ARRAY[0.1, 0.2, 0.3])")
        .await
        .expect("ARRAY[0.1, 0.2, 0.3] must insert into a VECTOR(3) column");

    let rows = server
        .query_rows("SELECT id FROM vec_array_literal")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the row must be stored: {rows:?}");
}

/// An element the coercion cannot read is a value error: it names the element
/// and its type, and it never surfaces as `XX000`. The house mapping for a
/// bad request is `42601`; the assertion pins the code so a reclassification
/// cannot regress silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_strict_reports_a_non_numeric_element_by_index_and_type() {
    let server = TestServer::start().await;
    create_strict_vector(&server, "vec_array_bad").await;

    server
        .expect_error(
            "INSERT INTO vec_array_bad (id, embedding) VALUES ('b1', ARRAY[0.1, 'nope', 0.3])",
            "VECTOR element 1",
        )
        .await;
    server
        .expect_error(
            "INSERT INTO vec_array_bad (id, embedding) VALUES ('b2', ARRAY[0.1, 'nope', 0.3])",
            "SQLSTATE 42601",
        )
        .await;
}
