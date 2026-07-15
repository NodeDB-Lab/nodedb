// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for UPDATE RETURNING and DELETE RETURNING via the pgwire
//! simple-query and extended-query protocols.
//!
//! Each test spins up a fresh single-core server, performs DML with a
//! RETURNING clause, and asserts that the correct columns and values come
//! back as a multi-column row result.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::types::Type;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Set up a schemaless collection with a single document {id, name, score}.
async fn seed_docs(server: &TestServer) {
    server
        .exec("CREATE COLLECTION items TYPE DOCUMENT (id STRING, name STRING, score INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, name, score) VALUES ('a', 'alpha', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, name, score) VALUES ('b', 'beta', 20)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, name, score) VALUES ('c', 'gamma', 30)")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// UPDATE RETURNING *
// ---------------------------------------------------------------------------

/// Point UPDATE with `RETURNING *` must return all fields of the post-update
/// document as a multi-column row.
#[tokio::test]
async fn point_update_returning_star() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows("UPDATE items SET score = 99 WHERE id = 'a' RETURNING *")
        .await
        .expect("UPDATE RETURNING * should succeed");

    assert_eq!(rows.len(), 1, "expected exactly one returned row");

    // The row must contain the updated score.
    let row = &rows[0];
    let joined = row.join(",");
    assert!(
        joined.contains("99"),
        "returned row must reflect updated score=99, got: {joined}"
    );
    assert!(
        joined.contains("alpha"),
        "returned row must retain name=alpha, got: {joined}"
    );
}

// ---------------------------------------------------------------------------
// UPDATE RETURNING named columns
// ---------------------------------------------------------------------------

/// Point UPDATE with named column list must return only the requested columns,
/// in spec order, using the alias where provided.
#[tokio::test]
async fn point_update_returning_named_columns() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows("UPDATE items SET score = 55 WHERE id = 'b' RETURNING id, score AS new_score")
        .await
        .expect("UPDATE RETURNING named should succeed");

    assert_eq!(rows.len(), 1, "expected one row");
    // Two columns: id, new_score.
    let row = &rows[0];
    assert_eq!(row.len(), 2, "expected 2 columns, got {}", row.len());
    assert_eq!(row[0], "b", "first column (id) must be 'b'");
    assert_eq!(row[1], "55", "second column (new_score) must be '55'");
}

// ---------------------------------------------------------------------------
// DELETE RETURNING *
// ---------------------------------------------------------------------------

/// Point DELETE with `RETURNING *` must return the pre-deletion document as a
/// multi-column row.
#[tokio::test]
async fn point_delete_returning_star() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows("DELETE FROM items WHERE id = 'c' RETURNING *")
        .await
        .expect("DELETE RETURNING * should succeed");

    assert_eq!(rows.len(), 1, "expected one returned row for deleted doc");
    let row = &rows[0];
    let joined = row.join(",");
    assert!(
        joined.contains("gamma"),
        "returned row must contain pre-deletion name=gamma, got: {joined}"
    );
    assert!(
        joined.contains("30"),
        "returned row must contain pre-deletion score=30, got: {joined}"
    );
}

// ---------------------------------------------------------------------------
// DELETE RETURNING named columns
// ---------------------------------------------------------------------------

/// Point DELETE with named RETURNING columns must return only those columns.
#[tokio::test]
async fn point_delete_returning_named_columns() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows("DELETE FROM items WHERE id = 'a' RETURNING id, name")
        .await
        .expect("DELETE RETURNING named should succeed");

    assert_eq!(rows.len(), 1, "expected one row");
    let row = &rows[0];
    assert_eq!(row.len(), 2, "expected 2 columns, got {}", row.len());
    assert_eq!(row[0], "a");
    assert_eq!(row[1], "alpha");
}

// ---------------------------------------------------------------------------
// Bulk UPDATE RETURNING
// ---------------------------------------------------------------------------

/// Bulk UPDATE (no WHERE clause) with RETURNING * must return one row per
/// matched document, each reflecting the post-update value.
#[tokio::test]
async fn bulk_update_returning() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows("UPDATE items SET score = 0 RETURNING id, score")
        .await
        .expect("bulk UPDATE RETURNING should succeed");

    // All three documents must be returned.
    assert_eq!(
        rows.len(),
        3,
        "expected 3 returned rows, got {}",
        rows.len()
    );

    // Every returned row must show score = 0.
    for row in &rows {
        assert_eq!(
            row.len(),
            2,
            "each row must have 2 columns, got {}",
            row.len()
        );
        assert_eq!(
            row[1], "0",
            "updated score must be 0 in every row, got {}",
            row[1]
        );
    }
}

// ---------------------------------------------------------------------------
// Bulk DELETE RETURNING
// ---------------------------------------------------------------------------

/// Bulk DELETE with a WHERE clause that matches two rows must return two
/// pre-deletion documents.
#[tokio::test]
async fn bulk_delete_returning() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    // Delete the two rows with score >= 20.
    let rows = server
        .query_rows("DELETE FROM items WHERE score >= 20 RETURNING id, score")
        .await
        .expect("bulk DELETE RETURNING should succeed");

    assert_eq!(rows.len(), 2, "expected 2 deleted rows, got {}", rows.len());

    // Collect returned ids.
    let mut ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, ["b", "c"], "returned ids must be b and c");
}

// ---------------------------------------------------------------------------
// Extended-query (prepared statement) RETURNING *
// ---------------------------------------------------------------------------

/// RETURNING * via the extended-query protocol (parameterised prepared
/// statement) must surface multi-column rows, not a JSON envelope.
#[tokio::test]
async fn extended_query_update_returning_star() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    // Use the extended-query (prepared-statement) path via tokio-postgres
    // `.query()` which drives Parse / Bind / Execute.
    let stmt = server
        .client
        .prepare_typed(
            "UPDATE items SET score = $1 WHERE id = $2 RETURNING *",
            &[Type::UNKNOWN, Type::UNKNOWN],
        )
        .await
        .expect("prepare should succeed");
    let rows = server
        .client
        .query(&stmt, &[&"77", &"b"])
        .await
        .expect("prepared UPDATE RETURNING should succeed");

    assert_eq!(rows.len(), 1, "expected one row");
    // Must have at least 2 columns (id + score at minimum).
    assert!(
        rows[0].len() >= 2,
        "row must expose multiple columns, got {}",
        rows[0].len()
    );
}

// ---------------------------------------------------------------------------
// UTF-8 statement boundaries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unicode_identifier_before_returning_preserves_connection() {
    let server = TestServer::start().await;

    server
        .expect_error("DELETE FROM missingﬀﬀ RETURNING *", "not found")
        .await;
    let rows = server
        .query_text("SELECT 1")
        .await
        .expect("connection must remain usable after the statement error");
    assert_eq!(rows, vec!["1"]);
}

// ---------------------------------------------------------------------------
// Arithmetic expression in RETURNING — error path
// ---------------------------------------------------------------------------

/// A RETURNING clause containing an arithmetic expression must be rejected
/// with a typed error. NodeDB only supports column references and aliases
/// in RETURNING, not computed expressions.
#[tokio::test]
async fn returning_arithmetic_expression_rejected() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    server
        .expect_error(
            "UPDATE items SET score = 1 WHERE id = 'a' RETURNING score + 1",
            "not supported",
        )
        .await;
}
