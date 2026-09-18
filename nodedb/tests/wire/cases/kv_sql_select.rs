// SPDX-License-Identifier: BUSL-1.1

//! SQL SELECT on KV collections must return rows with each projected
//! column as its own field, the same way every other engine does.
//! Protocol dictates response shape, not engine.
//!
//! These tests exercise the simple-query path: each row carries the
//! projected columns in their declared order. Extended-query coverage
//! for the same invariant lives in `pgwire_extended_query.rs`.

use crate::harness::TestServer;

/// `SELECT key, value FROM kv WHERE key = 'x'` must return one row
/// with two columns: the key and the value.
#[tokio::test]
async fn kv_sql_point_select_returns_key_and_value() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('hello', 'world')")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT key, value FROM kv WHERE key = 'hello'")
        .await
        .expect("point SELECT should succeed");

    assert_eq!(rows.len(), 1, "expected exactly one row");
    let row = &rows[0];
    assert_eq!(
        row.len(),
        2,
        "expected 2 projected columns, got {}",
        row.len()
    );
    assert_eq!(row[0], "hello", "column 0 (key) mismatch");
    assert_eq!(row[1], "world", "column 1 (value) mismatch");
}

/// Full-table SELECT must return one row per stored entry, each with
/// the projected columns.
#[tokio::test]
async fn kv_sql_full_scan_returns_all_rows() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('a', 'one')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('b', 'two')")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT key, value FROM kv")
        .await
        .expect("full scan SELECT should succeed");

    assert_eq!(rows.len(), 2, "expected 2 rows, got {}", rows.len());

    let mut pairs: Vec<(String, String)> =
        rows.iter().map(|r| (r[0].clone(), r[1].clone())).collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("a".to_string(), "one".to_string()),
            ("b".to_string(), "two".to_string()),
        ]
    );
}

/// Star projection over KV currently returns a single column carrying a
/// JSON envelope of every stored column. Explicit projection (`SELECT a, b`)
/// returns separate columns. Both shapes carry the same data; the
/// inconsistency between `*` and explicit lists is tracked for follow-up.
#[tokio::test]
async fn kv_sql_star_projection_returns_all_columns() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('hello', 'world')")
        .await
        .unwrap();

    // SELECT * expands the row into one pgwire field per declared column
    // (PG-compatible). The test harness joins them with a tab separator;
    // both 'hello' and 'world' must appear in projection order.
    let rows = server
        .query_text_joined("SELECT * FROM kv WHERE key = 'hello'")
        .await
        .expect("star SELECT should succeed");

    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("hello") && rows[0].contains("world"),
        "star projection must include every declared column: {:?}",
        rows[0]
    );
}

/// KV collections with multi-column typed values must expose every
/// declared column in projection order. Uses `key` as the PK column
/// because the current KV INSERT planner only recognises a PK column
/// literally named `key`.
#[tokio::test]
async fn kv_sql_typed_columns_point_select() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION users (key STRING PRIMARY KEY, name STRING, age INT) WITH (engine='kv')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO users (key, name, age) VALUES ('u1', 'alice', 30)")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT key, name, age FROM users WHERE key = 'u1'")
        .await
        .expect("multi-column point SELECT should succeed");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 3);
    assert_eq!(row[0], "u1");
    assert_eq!(row[1], "alice");
    let age: i64 = row[2].parse().expect("age must decode as integer");
    assert_eq!(age, 30);
}

/// Single-column projection returns the requested field.
#[tokio::test]
async fn kv_sql_single_column_projection() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('hello', 'world')")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT value FROM kv WHERE key = 'hello'")
        .await
        .expect("single-column point SELECT should succeed");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 1);
    assert_eq!(row[0], "world");
}

/// A computed column over a kv scan evaluates per row instead of returning
/// NULL, matching the document/columnar/timeseries scan handlers.
#[tokio::test]
async fn computed_column_over_kv_evaluates_per_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t (key STRING PRIMARY KEY, id INT) WITH (engine='kv')")
        .await
        .unwrap();
    for i in 1..=3i64 {
        server
            .exec(&format!("INSERT INTO t (key, id) VALUES ('k{i}', {i})"))
            .await
            .unwrap();
    }

    let rows = server
        .query_rows("SELECT id, id * 2 AS d FROM t ORDER BY id")
        .await
        .expect("computed-column SELECT should succeed");

    assert_eq!(rows.len(), 3);
    let pairs: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| (r[0].parse().unwrap(), r[1].parse().unwrap()))
        .collect();
    assert_eq!(pairs, vec![(1, 2), (2, 4), (3, 6)]);
}

/// A division-by-zero inside a computed column over a kv scan raises
/// SQLSTATE 22012 instead of materializing NULL into the response.
#[tokio::test]
async fn computed_column_division_by_zero_over_kv_errors_22012() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t (key STRING PRIMARY KEY, id INT) WITH (engine='kv')")
        .await
        .unwrap();
    for i in 1..=3i64 {
        server
            .exec(&format!("INSERT INTO t (key, id) VALUES ('k{i}', {i})"))
            .await
            .unwrap();
    }

    server
        .expect_error("SELECT id, 1 / (id - 2) AS d FROM t", "22012")
        .await;
}

/// A function call in the projection list over a kv scan evaluates per row.
#[tokio::test]
async fn function_projection_over_kv_evaluates() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t (key STRING PRIMARY KEY, id INT, v STRING) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (key, id, v) VALUES ('k1', 1, 'a')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (key, id, v) VALUES ('k2', 2, 'b')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (key, id, v) VALUES ('k3', 3, 'c')")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT upper(v) AS u FROM t ORDER BY id")
        .await
        .expect("function-projection SELECT should succeed");

    let values: Vec<String> = rows.iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        values,
        vec!["A".to_string(), "B".to_string(), "C".to_string()]
    );
}
