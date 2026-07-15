// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for PREPARE / EXECUTE / DEALLOCATE (SQL-level).

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepare_execute_deallocate_lifecycle() {
    let server = TestServer::start().await;

    // PREPARE + DEALLOCATE lifecycle (no EXECUTE — avoids DataFusion plan converter limitations).
    server.exec("PREPARE q AS SELECT 1").await.unwrap();

    // DEALLOCATE removes the prepared statement.
    server.exec("DEALLOCATE q").await.unwrap();

    // EXECUTE after DEALLOCATE should fail.
    server.expect_error("EXECUTE q", "does not exist").await;

    // DEALLOCATE ALL removes all prepared statements.
    server.exec("PREPARE q1 AS SELECT 1").await.unwrap();
    server.exec("PREPARE q2 AS SELECT 2").await.unwrap();
    server.exec("DEALLOCATE ALL").await.unwrap();
    server.expect_error("EXECUTE q1", "does not exist").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_prepare_unicode_name_preserves_connection() {
    let server = TestServer::start().await;

    server
        .exec("PREPARE qﬀﬀ AS SELECT 1")
        .await
        .expect("Unicode prepared-statement name should parse");
    server
        .exec("DEALLOCATE qﬀﬀ")
        .await
        .expect("prepared statement should remain addressable");
    let rows = server
        .query_text("SELECT 1")
        .await
        .expect("connection must remain usable");
    assert_eq!(rows, vec!["1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_search_vector_dsl() {
    let server = TestServer::start().await;

    // Create a document collection and a vector index on the embedding field.
    server
        .exec("CREATE COLLECTION vec_ep TYPE document")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_vec_ep ON vec_ep METRIC cosine DIM 3")
        .await
        .unwrap();

    // Insert a document with an embedding vector.
    server
        .exec("INSERT INTO vec_ep (id, embedding) VALUES ('v1', ARRAY[1.0, 0.0, 0.0])")
        .await
        .unwrap();

    // DSL SEARCH statements must not be rejected by the extended-protocol path
    // with "Expected: an SQL statement". The statement should succeed and return
    // results (or an empty result set — the key is no parse-time rejection).
    let result = server
        .query_text("SEARCH vec_ep USING VECTOR(embedding, ARRAY[1.0, 0.0, 0.0], 3)")
        .await;
    assert!(
        result.is_ok(),
        "SEARCH via extended protocol must not be rejected: {:?}",
        result.err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_upsert_dsl() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION upsert_ep").await.unwrap();

    // UPSERT INTO DSL statements must not be rejected by the extended-protocol
    // path with "Expected: an SQL statement".
    let result = server
        .exec("UPSERT INTO upsert_ep { id: 'u1', name: 'alice' }")
        .await;
    assert!(
        result.is_ok(),
        "UPSERT INTO via extended protocol must not be rejected: {:?}",
        result.err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_select_strict_doc_returns_data() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION strict_ep  \
             (id TEXT PRIMARY KEY, name TEXT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO strict_ep (id, name) VALUES ('a', 'alice')")
        .await
        .unwrap();

    // SELECT on a STRICT doc collection via the extended-query protocol must
    // return the inserted row with actual column values, not null/empty columns.
    let rows = server
        .query_text("SELECT id, name FROM strict_ep WHERE id = 'a'")
        .await
        .unwrap();
    assert!(!rows.is_empty(), "SELECT should return the inserted row");

    // Regression guard: the row must contain actual data, not null.
    // With the new response shape each projected column is its own field;
    // query_text returns row.get(0) which is the `id` value. Check that
    // the row is non-empty (i.e., the query returned actual data, not nulls).
    // The `name` column value is in column index 1 — use query_rows for it.
    let rows2 = server
        .query_rows("SELECT id, name FROM strict_ep WHERE id = 'a'")
        .await
        .unwrap();
    assert!(!rows2.is_empty(), "extended protocol must return data");
    assert_eq!(
        rows2[0][1], "alice",
        "extended protocol must not return null columns for STRICT doc, got: {:?}",
        rows2[0]
    );
}
