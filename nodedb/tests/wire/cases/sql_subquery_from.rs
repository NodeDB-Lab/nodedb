// SPDX-License-Identifier: BUSL-1.1

//! Derived-table subqueries in the `FROM` clause: `FROM (SELECT ...) AS t`
//! must plan as a single source relation, not be rejected as multi-table
//! FROM.

use std::collections::HashMap;

use crate::harness::TestServer;

async fn create_items(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION items \
             COLUMNS (id TEXT PRIMARY KEY, category TEXT, qty INTEGER) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO items (id, category, qty) VALUES \
             ('i1', 'a', 1), \
             ('i2', 'a', 2), \
             ('i3', 'b', 3), \
             ('i4', 'b', 4), \
             ('i5', 'c', 5)",
        )
        .await
        .unwrap();
}

/// `FROM (SELECT ... WHERE ...) AS t` must plan as a derived table.
/// The pre-fix symptom was `unsupported: multi-table FROM without
/// JOIN` — the parenthesised SELECT was being misread as a second
/// base relation. With the fix, the inner filter applies and the
/// outer SELECT sees the filtered rows.
#[tokio::test]
async fn derived_with_inner_filter_is_supported() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    let rows = srv
        .query_rows("SELECT category FROM (SELECT category FROM items WHERE qty > 2) AS t")
        .await
        .expect(
            "derived table `(SELECT ... WHERE ...) AS t` must plan as a single \
             source relation, not be rejected as multi-table FROM",
        );

    // Three rows satisfy qty > 2: i3=b, i4=b, i5=c.
    assert_eq!(
        rows.len(),
        3,
        "inner WHERE qty > 2 should yield 3 rows; got {rows:?}"
    );
    let cats: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    let mut sorted = cats.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec!["b", "b", "c"],
        "filtered categories should be {{b, b, c}}; got {cats:?}"
    );
}

/// `FROM (SELECT ... GROUP BY ...) AS agg` must plan as a derived
/// table over the grouped inner query. This is the canonical
/// "aggregate then post-process" pattern. The fix has to propagate
/// the inner GROUP BY's three output rows up to the outer SELECT
/// without the planner choking on the derived-table FROM.
#[tokio::test]
async fn derived_group_by_in_from_is_supported() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    let rows = srv
        .query_rows(
            "SELECT category, total \
             FROM (SELECT category, SUM(qty) AS total \
                   FROM items GROUP BY category) AS agg",
        )
        .await
        .expect(
            "derived table over a grouped inner SELECT must plan as a single \
             relation — this is the canonical 'aggregate then post-process' \
             shape",
        );

    assert_eq!(rows.len(), 3, "three categories expected, got {rows:?}");

    // Assert correctness on contents, not order — ORDER BY after
    // GROUP BY is currently best-effort, so the inner rows can come
    // back in any order. Categories alphabetically: a (1+2=3), b
    // (3+4=7), c (5).
    let totals: HashMap<String, f64> = rows
        .iter()
        .map(|r| (r[0].clone(), r[1].parse::<f64>().unwrap()))
        .collect();
    assert_eq!(totals.get("a"), Some(&3.0), "category a total should be 3");
    assert_eq!(totals.get("b"), Some(&7.0), "category b total should be 7");
    assert_eq!(totals.get("c"), Some(&5.0), "category c total should be 5");
}

/// A computed column over a constant derived table must evaluate. The
/// constant body lowers to a provider row, and the outer projection must
/// run over that row instead of resolving `x * 2` by name to NULL.
#[tokio::test]
async fn computed_column_over_constant_derived_table_evaluates() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_rows("SELECT x * 2 AS doubled FROM (SELECT 1 AS x) AS s")
        .await
        .expect("computed column over a constant derived table must plan");

    assert_eq!(rows, vec![vec!["2".to_string()]], "got {rows:?}");
}

/// A computed column over a derived table whose body is an aggregate must
/// evaluate over the aggregate's output row.
#[tokio::test]
async fn computed_column_over_aggregate_derived_table_evaluates() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    let rows = srv
        .query_rows("SELECT total * 2 AS doubled FROM (SELECT SUM(qty) AS total FROM items) AS s")
        .await
        .expect("computed column over an aggregate derived table must plan");

    assert_eq!(rows, vec![vec!["30".to_string()]], "got {rows:?}");
}

/// A computed column over a grouped derived table must evaluate per output
/// row and keep every projected column.
#[tokio::test]
async fn computed_column_over_grouped_derived_table_evaluates() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    let rows = srv
        .query_rows(
            "SELECT category, total * 2 AS doubled \
             FROM (SELECT category, SUM(qty) AS total FROM items GROUP BY category) AS agg",
        )
        .await
        .expect("computed column over a grouped derived table must plan");

    let mut got: Vec<(String, String)> =
        rows.iter().map(|r| (r[0].clone(), r[1].clone())).collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("a".to_string(), "6".to_string()),
            ("b".to_string(), "14".to_string()),
            ("c".to_string(), "10".to_string()),
        ],
        "got {rows:?}"
    );
}

/// A computed column over a UNION ALL derived table must evaluate per row.
#[tokio::test]
async fn computed_column_over_union_derived_table_evaluates() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_rows("SELECT x * 2 AS doubled FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) AS s")
        .await
        .expect("computed column over a UNION ALL derived table must plan");

    let mut got: Vec<String> = rows.iter().map(|r| r[0].clone()).collect();
    got.sort();
    assert_eq!(got, vec!["2".to_string(), "4".to_string()], "got {rows:?}");
}

/// Division by zero in the projection over a constant derived table must
/// raise `22012`, never fold to a NULL row.
#[tokio::test]
async fn projection_division_by_zero_over_constant_derived_table_errors_22012() {
    let srv = TestServer::start().await;

    srv.expect_error("SELECT x / 0 FROM (SELECT 1 AS x) AS s", "22012")
        .await;
}

/// Division by zero in an aggregate argument over a constant derived table
/// must raise `22012`, never fold to a NULL aggregate.
#[tokio::test]
async fn aggregate_argument_division_by_zero_over_constant_derived_table_errors_22012() {
    let srv = TestServer::start().await;

    srv.expect_error("SELECT SUM(x / 0) FROM (SELECT 1 AS x) AS s", "22012")
        .await;
}

/// Division by zero in a GROUP BY key over a constant derived table must
/// raise `22012`, never return an empty result with a missing column.
#[tokio::test]
async fn group_by_key_division_by_zero_over_constant_derived_table_errors_22012() {
    let srv = TestServer::start().await;

    srv.expect_error(
        "SELECT x, COUNT(*) FROM (SELECT 1 AS x) AS s GROUP BY x / 0",
        "22012",
    )
    .await;
}

/// Division by zero in a window PARTITION BY over a constant derived table
/// must raise `22012`, never fold to a NULL window value.
#[tokio::test]
async fn window_partition_division_by_zero_over_constant_derived_table_errors_22012() {
    let srv = TestServer::start().await;

    srv.expect_error(
        "SELECT SUM(x) OVER (PARTITION BY x / 0) FROM (SELECT 1 AS x) AS s",
        "22012",
    )
    .await;
}

/// Division by zero in the projection over an aggregate derived table must
/// raise `22012`.
#[tokio::test]
async fn projection_division_by_zero_over_aggregate_derived_table_errors_22012() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    srv.expect_error(
        "SELECT total / 0 FROM (SELECT SUM(qty) AS total FROM items) AS s",
        "22012",
    )
    .await;
}

/// A grouped query over a constant derived table must keep every projected
/// column in the result, not drop the non-aggregate column.
#[tokio::test]
async fn group_by_over_constant_derived_table_keeps_projected_columns() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_rows("SELECT x, COUNT(*) AS n FROM (SELECT 1 AS x) AS s GROUP BY x")
        .await
        .expect("GROUP BY over a constant derived table must plan");

    assert_eq!(
        rows,
        vec![vec!["1".to_string(), "1".to_string()]],
        "got {rows:?}"
    );
}

/// Control: the same projection over a derived table that scans a
/// collection raises `22012`. The derived table itself is not the trigger.
#[tokio::test]
async fn projection_division_by_zero_over_scan_derived_table_errors_22012() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    srv.expect_error(
        "SELECT x / 0 FROM (SELECT qty AS x FROM items) AS s",
        "22012",
    )
    .await;
}

/// A computed column that references an inner alias (`qty AS x`) over a
/// scanning derived table must resolve through the alias. Merging the outer
/// projection onto the inner scan must not discard the inner rename.
#[tokio::test]
async fn computed_column_over_aliased_scan_derived_table_evaluates() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    let rows = srv
        .query_rows(
            "SELECT x * 2 AS doubled FROM (SELECT qty AS x FROM items WHERE id = 'i3') AS s",
        )
        .await
        .expect("computed column over an aliased scan derived table must plan");

    assert_eq!(rows, vec![vec!["6".to_string()]], "got {rows:?}");
}

/// A computed column over a grouped derived table with an outer ORDER BY
/// (which routes through the row post-processor) must evaluate per row.
#[tokio::test]
async fn computed_column_over_grouped_derived_table_with_order_by_evaluates() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    let rows = srv
        .query_rows(
            "SELECT category, total * 2 AS doubled \
             FROM (SELECT category, SUM(qty) AS total FROM items GROUP BY category) AS agg \
             ORDER BY category",
        )
        .await
        .expect("computed column over a grouped derived table with ORDER BY must plan");

    assert_eq!(
        rows,
        vec![
            vec!["a".to_string(), "6".to_string()],
            vec!["b".to_string(), "14".to_string()],
            vec!["c".to_string(), "10".to_string()],
        ],
        "got {rows:?}"
    );
}

/// An aggregate over a grouped derived table (aggregate of aggregates) must
/// run over the inner group rows, not over an empty collection.
#[tokio::test]
async fn aggregate_over_grouped_derived_table_evaluates() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    let rows = srv
        .query_rows(
            "SELECT SUM(total) AS grand, COUNT(*) AS groups \
             FROM (SELECT category, SUM(qty) AS total FROM items GROUP BY category) AS agg",
        )
        .await
        .expect("aggregate over a grouped derived table must plan");

    // SUM renders as a float text today; compare numerically.
    assert_eq!(rows.len(), 1, "got {rows:?}");
    let grand: f64 = rows[0][0].parse().expect("grand total must be numeric");
    assert_eq!(grand, 15.0, "got {rows:?}");
    assert_eq!(rows[0][1], "3", "got {rows:?}");
}

/// An aggregate over a UNION ALL derived table must run over the union rows.
#[tokio::test]
async fn aggregate_over_union_derived_table_evaluates() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_rows("SELECT SUM(x) AS total FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) AS s")
        .await
        .expect("aggregate over a UNION ALL derived table must plan");

    // SUM renders as a float text today; compare numerically.
    assert_eq!(rows.len(), 1, "got {rows:?}");
    let total: f64 = rows[0][0].parse().expect("total must be numeric");
    assert_eq!(total, 3.0, "got {rows:?}");
}

/// A window function over a grouped derived table must rank the inner group
/// rows.
#[tokio::test]
async fn window_over_grouped_derived_table_evaluates() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    let rows = srv
        .query_rows(
            "SELECT category, RANK() OVER (ORDER BY total DESC) AS rnk \
             FROM (SELECT category, SUM(qty) AS total FROM items GROUP BY category) AS agg",
        )
        .await
        .expect("window function over a grouped derived table must plan");

    let mut got: Vec<(String, String)> =
        rows.iter().map(|r| (r[0].clone(), r[1].clone())).collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("a".to_string(), "3".to_string()),
            ("b".to_string(), "1".to_string()),
            ("c".to_string(), "2".to_string()),
        ],
        "got {rows:?}"
    );
}
