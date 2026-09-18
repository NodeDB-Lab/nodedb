// SPDX-License-Identifier: BUSL-1.1

//! Per-row evaluation of sequence accessors (`nextval`/`currval`/`setval`) in
//! the SELECT list of a top-level SELECT over a relation.
//!
//! The rule: a sequence accessor in the SELECT list of a top-level SELECT
//! over a relation evaluates once per output row, in output order, on the
//! Control Plane after the rows are final. Every other row-scope clause
//! (ORDER BY, GROUP BY, HAVING, JOIN ON, window, aggregate argument, a
//! nested subquery, UPDATE SET, INSERT ... SELECT source) still refuses with
//! `0A000` — see `sql_sequence_row_scope_refusals.rs`.

use crate::harness::TestServer;

/// Create sequence `s` and a kv collection `t` (`id BIGINT PRIMARY KEY, v
/// TEXT`) with rows `(1,'a'), (2,'b'), (3,'c')`.
async fn seed(server: &TestServer) {
    server.exec("CREATE SEQUENCE s").await.unwrap();
    server
        .exec("CREATE COLLECTION t (id BIGINT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
}

/// `nextval` in the SELECT list of a scan advances once per output row, in
/// `ORDER BY` order, then the session continues from the last row's value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_in_a_select_list_advances_once_per_output_row_in_output_order() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_rows("SELECT id, nextval('s') AS n FROM t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "1".to_string()],
            vec!["2".to_string(), "2".to_string()],
            vec!["3".to_string(), "3".to_string()],
        ]
    );

    let next = server.query_text("SELECT nextval('s')").await.unwrap();
    assert_eq!(next, vec!["4".to_string()]);
}

/// `LIMIT` truncates the row stream before evaluation, so only the emitted
/// rows consume an allocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_with_limit_consumes_exactly_the_emitted_rows() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT nextval('s') AS n FROM t ORDER BY id LIMIT 2")
        .await
        .unwrap();
    assert_eq!(rows, vec!["1".to_string(), "2".to_string()]);

    let next = server.query_text("SELECT nextval('s')").await.unwrap();
    assert_eq!(next, vec!["3".to_string()]);
}

/// The accessor's result feeds an arithmetic expression, one evaluation per
/// row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_inside_an_arithmetic_expression_evaluates_per_row() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_rows("SELECT id, nextval('s') * 10 AS n FROM t ORDER BY id")
        .await
        .unwrap();
    let n: Vec<&str> = rows.iter().map(|r| r[1].as_str()).collect();
    assert_eq!(n, vec!["10", "20", "30"]);
}

/// `nextval` referencing a row column sees that row, and the projection
/// carries exactly one output column (`n`) — no leaked pass-through `id`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_referencing_a_row_column_sees_that_row() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_named_rows("SELECT nextval('s') + id AS n FROM t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "three rows expected: {rows:?}");
    for (row, expected) in rows.iter().zip(["2", "4", "6"]) {
        assert_eq!(row.len(), 1, "exactly one column expected: {row:?}");
        assert_eq!(row.get("n").map(String::as_str), Some(expected));
    }
}

/// `currval` inside the same row as a preceding `nextval` reads that row's
/// value, not a stale session-wide one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn currval_after_nextval_in_the_same_row_reads_that_rows_value() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_rows("SELECT nextval('s') AS a, currval('s') AS b FROM t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "1".to_string()],
            vec!["2".to_string(), "2".to_string()],
            vec!["3".to_string(), "3".to_string()],
        ]
    );
}

/// `setval` in the SELECT list positions the sequence per row, and the
/// session continues from the last row's position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setval_in_a_select_list_positions_per_row() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT setval('s', id * 100) AS v FROM t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec!["100".to_string(), "200".to_string(), "300".to_string()]
    );

    let next = server.query_text("SELECT nextval('s')").await.unwrap();
    assert_eq!(next, vec!["301".to_string()]);
}

/// A SELECT list consisting only of the accessor emits exactly that column,
/// one row per source row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bare_accessor_projection_emits_only_its_column() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_rows("SELECT nextval('s') FROM t")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "three rows expected: {rows:?}");
    for row in &rows {
        assert_eq!(row.len(), 1, "exactly one column expected: {row:?}");
    }
}

/// A derived table (subquery in FROM) still evaluates the accessor once per
/// outer output row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_over_a_derived_table_evaluates_per_outer_row() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_rows("SELECT x, nextval('s') AS n FROM (SELECT id AS x FROM t) AS d ORDER BY x")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "1".to_string()],
            vec!["2".to_string(), "2".to_string()],
            vec!["3".to_string(), "3".to_string()],
        ]
    );
}

/// A `GROUP BY` query evaluates the accessor once per emitted group row, in
/// output order, after grouping has collapsed the input.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_over_a_grouped_query_evaluates_per_group_row() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE s").await.unwrap();
    server
        .exec("CREATE COLLECTION t (id BIGINT PRIMARY KEY, grp TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, grp) VALUES (1, 'a'), (2, 'a'), (3, 'b')")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT grp, COUNT(*) AS c, nextval('s') AS n FROM t GROUP BY grp ORDER BY grp")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec!["a".to_string(), "2".to_string(), "1".to_string()],
            vec!["b".to_string(), "1".to_string(), "2".to_string()],
        ]
    );
}

/// The DDL shape per engine, copied from `sql_sequences.rs`'s per-engine
/// tests: `(id BIGINT PRIMARY KEY, v TEXT)` for the document and kv engines,
/// and `COLUMNS (id BIGINT, v TEXT)` (no PRIMARY KEY) for columnar.
fn create_table_sql(name: &str, engine: &str) -> String {
    if engine == "columnar" {
        format!("CREATE COLLECTION {name} COLUMNS (id BIGINT, v TEXT) WITH (engine='columnar')")
    } else {
        format!("CREATE COLLECTION {name} (id BIGINT PRIMARY KEY, v TEXT) WITH (engine='{engine}')")
    }
}

/// `nextval` in the SELECT list evaluates on every engine, yielding 3
/// distinct increasing values in `ORDER BY id` order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_in_a_select_list_evaluates_on_every_engine() {
    let server = TestServer::start().await;

    for engine in ["document_schemaless", "document_strict", "kv", "columnar"] {
        let seq = format!("seq_engine_{engine}");
        let table = format!("t_engine_{engine}");

        server
            .exec(&format!("CREATE SEQUENCE {seq}"))
            .await
            .unwrap();
        server
            .exec(&create_table_sql(&table, engine))
            .await
            .unwrap();
        server
            .exec(&format!(
                "INSERT INTO {table} (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
            ))
            .await
            .unwrap();

        let rows = server
            .query_text(&format!(
                "SELECT nextval('{seq}') AS n FROM {table} ORDER BY id"
            ))
            .await
            .unwrap_or_else(|e| panic!("engine {engine}: {e}"));

        assert_eq!(
            rows.len(),
            3,
            "engine {engine}: three rows expected: {rows:?}"
        );
        let values: Vec<i64> = rows
            .iter()
            .map(|v| {
                v.parse()
                    .unwrap_or_else(|_| panic!("engine {engine}: non-numeric {v}"))
            })
            .collect();
        assert_eq!(
            values,
            vec![values[0], values[0] + 1, values[0] + 2],
            "engine {engine}: values must be distinct and increasing, got {values:?}"
        );
    }
}

/// A 500-row scan keeps `n == id` for every row: streaming evaluation must
/// not reorder or skip a row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_over_a_large_result_stays_ordered() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE s").await.unwrap();
    server
        .exec("CREATE COLLECTION t (id BIGINT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();

    let values: Vec<String> = (1..=500).map(|i| format!("({i}, 'v{i}')")).collect();
    server
        .exec(&format!(
            "INSERT INTO t (id, v) VALUES {}",
            values.join(", ")
        ))
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, nextval('s') AS n FROM t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 500, "500 rows expected: got {}", rows.len());
    for row in &rows {
        assert_eq!(
            row[0], row[1],
            "n must equal id for every row, got id={} n={}",
            row[0], row[1]
        );
    }
}

/// `EXPLAIN` plans the statement without executing it, so a per-row accessor
/// inside it must not advance the sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explain_of_a_per_row_accessor_does_not_advance() {
    let server = TestServer::start().await;
    seed(&server).await;

    server
        .exec("EXPLAIN SELECT nextval('s') FROM t")
        .await
        .unwrap();

    let next = server.query_text("SELECT nextval('s')").await.unwrap();
    assert_eq!(next, vec!["1".to_string()]);
}

/// A statement that fails partway through (division by zero on a later row)
/// leaves no partial result and no partial allocation footprint visible to
/// the caller — the whole statement errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_statement_does_not_leave_partial_rows() {
    let server = TestServer::start().await;
    seed(&server).await;

    server
        .expect_error(
            "SELECT nextval('s') AS n, 1 / (id - 2) AS d FROM t ORDER BY id",
            "22012",
        )
        .await;
}

/// `INSERT ... RETURNING` evaluates the accessor once per inserted row, in
/// insertion order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_nextval_evaluates_per_inserted_row() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_rows(
            "INSERT INTO t (id, v) VALUES (10, 'x'), (11, 'y') RETURNING id, nextval('s') AS n",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec!["10".to_string(), "1".to_string()],
            vec!["11".to_string(), "2".to_string()],
        ]
    );
}

/// A plain arithmetic `RETURNING` expression evaluates against the inserted
/// row, same as any other per-row `RETURNING` expression.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_an_arithmetic_expression_evaluates() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("INSERT INTO t (id, v) VALUES (12, 'z') RETURNING id * 2 AS d")
        .await
        .unwrap();
    assert_eq!(rows, vec!["24".to_string()]);
}

/// `UPDATE ... RETURNING` evaluates the accessor once per updated row.
/// `RETURNING` carries no `ORDER BY`, so the assertion checks the set of
/// `(id, n)` pairs rather than a fixed row order: every id appears exactly
/// once, and `n` takes each of `1..=3` exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_returning_nextval_evaluates_per_updated_row() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_rows("UPDATE t SET v = 'touched' RETURNING id, nextval('s') AS n")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "three updated rows expected: {rows:?}");

    let mut ids: Vec<i64> = rows.iter().map(|r| r[0].parse().unwrap()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3], "every id must appear once: {rows:?}");

    let mut ns: Vec<i64> = rows.iter().map(|r| r[1].parse().unwrap()).collect();
    ns.sort_unstable();
    assert_eq!(
        ns,
        vec![1, 2, 3],
        "n must take each allocation exactly once: {rows:?}"
    );
}
