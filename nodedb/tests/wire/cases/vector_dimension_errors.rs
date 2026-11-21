// SPDX-License-Identifier: BUSL-1.1

//! A vector of the wrong dimension is the caller's data error, SQLSTATE
//! `22000`, on every vector path.
//!
//! - A search whose query width differs from the index fails with `22000`
//!   instead of panicking the Data Plane core, and the server keeps serving:
//!   the next search on the same collection succeeds.
//! - An insert whose vector width differs from the index fails with `22000`,
//!   not `23505` (`unique_violation`), and inserts nothing.

use crate::harness::TestServer;

/// A collection holding four 3-wide vectors under a `DIM 3` index.
async fn seeded(name: &str) -> TestServer {
    let srv = TestServer::start().await;
    srv.exec(&format!(
        "CREATE COLLECTION {name} WITH (engine='document_schemaless')"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "CREATE VECTOR INDEX idx_{name} ON {name} (embedding) METRIC l2 DIM 3"
    ))
    .await
    .unwrap();
    for (id, emb) in [
        ("v1", "[1.0, 0.0, 0.0]"),
        ("v2", "[0.0, 1.0, 0.0]"),
        ("v3", "[0.0, 0.0, 1.0]"),
        ("v4", "[0.7, 0.7, 0.0]"),
    ] {
        srv.exec(&format!(
            "INSERT INTO {name} {{ id: '{id}', embedding: {emb} }}"
        ))
        .await
        .unwrap();
    }
    srv
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_dimension_search_is_22000_and_the_server_keeps_serving() {
    let srv = seeded("vd_search").await;

    let error = srv
        .query_text(
            "SELECT id FROM vd_search \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0]) LIMIT 2",
        )
        .await
        .expect_err("a 2-wide query against a 3-wide index must fail");
    assert!(
        error.contains("22000") && error.contains("vector dimension mismatch: expected 3, got 2"),
        "expected 22000 naming both widths, got: {error}"
    );

    let rows = srv
        .query_rows(
            "SELECT id FROM vd_search \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]) LIMIT 2",
        )
        .await
        .expect("the next search on the same collection succeeds");
    assert_eq!(rows.first().map(|r| r[0].as_str()), Some("v1"), "{rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_dimension_insert_is_22000_and_inserts_nothing() {
    let srv = seeded("vd_insert").await;

    let error = srv
        .exec("INSERT INTO vd_insert { id: 'bad', embedding: [0.5, 0.5] }")
        .await
        .expect_err("a 2-wide vector must not enter a 3-wide index");
    assert!(
        error.contains("22000") && error.contains("vector dimension mismatch"),
        "expected 22000, got: {error}"
    );
    assert!(!error.contains("23505"), "not a unique violation: {error}");

    let rows = srv
        .query_rows("SELECT id FROM vd_insert WHERE id = 'bad'")
        .await
        .expect("read after the refused insert");
    assert!(rows.is_empty(), "the refused row is not stored: {rows:?}");
}

/// The first write has no index yet, so the declared width alone refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_write_of_the_wrong_width_is_refused_and_inserts_nothing() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vd_first WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec("CREATE VECTOR INDEX idx_vd_first ON vd_first METRIC cosine DIM 3")
        .await
        .unwrap();

    let error = srv
        .exec("INSERT INTO vd_first { id: 'w2', embedding: [1.0, 0.0] }")
        .await
        .expect_err("a 2-wide first write into a DIM 3 index must fail");
    assert!(
        error.contains("22000") && error.contains("expected 3, got 2"),
        "expected 22000 naming both widths, got: {error}"
    );

    srv.exec("INSERT INTO vd_first { id: 'ok3', embedding: [1.0, 0.0, 0.0] }")
        .await
        .expect("a 3-wide write succeeds after the refused one");
    let rows = srv
        .query_rows("SELECT id FROM vd_first ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
}
