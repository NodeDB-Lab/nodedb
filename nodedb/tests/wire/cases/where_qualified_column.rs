// SPDX-License-Identifier: BUSL-1.1

//! A table-qualified WHERE column (`u.name` / `users.name`) on a
//! single-table query must not silently match zero rows: the qualifier
//! must not leak into the scan filter's field name.

use crate::harness::TestServer;

async fn seed(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION t (\
                id STRING PRIMARY KEY, \
                name STRING, \
                age INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, name, age) VALUES ('t1', 'bob', 30)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, name, age) VALUES ('t2', 'alice', 25)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_name_qualified_where_matches_rows() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT id FROM t WHERE t.name = 'bob'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "table-qualified WHERE must not return 0 rows"
    );
    assert!(rows[0].contains("t1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alias_qualified_where_matches_rows() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT alias_t.id FROM t alias_t WHERE alias_t.name = 'bob'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "alias-qualified WHERE must not return 0 rows"
    );
    assert!(rows[0].contains("t1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_pk_qualified_column_matches() {
    let server = TestServer::start().await;
    seed(&server).await;

    // `age` is not the primary key, so this proves the fix isn't only
    // covering the TEXT-PK-equality fast path.
    let rows = server
        .query_text("SELECT id FROM t WHERE t.age = 30")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("t1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compound_qualified_predicate_matches() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT id FROM t WHERE t.name = 'bob' AND t.age = 30")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("t1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unqualified_where_still_works() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT id FROM t WHERE name = 'bob'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("t1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn qualified_order_by_sorts_single_table() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_text("SELECT id FROM t WHERE t.age > 20 ORDER BY t.age")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "both rows have age > 20");
    // alice (25) then bob (30) — ascending by the qualified sort key.
    assert!(rows[0].contains("t2"), "ascending order: t2 (age 25) first");
    assert!(
        rows[1].contains("t1"),
        "ascending order: t1 (age 30) second"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mismatched_qualifier_errors_instead_of_zero_rows() {
    let server = TestServer::start().await;
    seed(&server).await;

    let result = server
        .query_text("SELECT id FROM t WHERE wrong.name = 'bob'")
        .await;
    assert!(
        result.is_err(),
        "a qualifier that matches neither the table name nor its alias must be a typed error, \
         not a silent 0-row result"
    );
}

// ── Every predicate shape, not only `=` ──────────────────────────────────

async fn seeded_predicates() -> TestServer {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION qp1 (id TEXT PRIMARY KEY, n INT, label TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO qp1 (id, n, label) VALUES ('a', 1, 'x'), ('b', 2, 'y'), ('c', 3, 'z')")
        .await
        .unwrap();
    srv
}

async fn ids(srv: &TestServer, sql: &str) -> Vec<String> {
    srv.query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_qualified_column_matches_in_every_predicate_shape() {
    let srv = seeded_predicates().await;
    for (predicate, expected) in [
        ("qp1.id IN ('a', 'b')", vec!["a", "b"]),
        ("qp1.id NOT IN ('a', 'b')", vec!["c"]),
        ("qp1.n BETWEEN 1 AND 2", vec!["a", "b"]),
        ("qp1.label LIKE 'x%'", vec!["a"]),
    ] {
        let sql = format!("SELECT id FROM qp1 WHERE {predicate} ORDER BY id");
        assert_eq!(ids(&srv, &sql).await, expected, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_alias_qualified_column_matches_in_every_predicate_shape() {
    let srv = seeded_predicates().await;
    for (predicate, expected) in [
        ("z.id IN ('a', 'b')", vec!["a", "b"]),
        ("z.n BETWEEN 2 AND 3", vec!["b", "c"]),
    ] {
        let sql = format!("SELECT z.id FROM qp1 AS z WHERE {predicate} ORDER BY z.id");
        assert_eq!(ids(&srv, &sql).await, expected, "{sql}");
    }
}
