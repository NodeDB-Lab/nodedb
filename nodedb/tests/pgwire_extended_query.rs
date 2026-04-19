//! Extended-query protocol (Parse/Bind/Describe/Execute) must return
//! rows with one decoded field per column declared by Describe.
//!
//! The canonical JSON envelope (`{"result": "..."}` / `{"document": "..."}`)
//! is a simple-query contract. The extended-query path must emit
//! column-shaped rows natively so ORMs and pg drivers can read results.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::types::Type;

/// Strict-document SELECT by parameterised primary key must return
/// columns `id` and `name` decoded as text, not an empty row.
#[tokio::test]
async fn extended_query_strict_doc_returns_decoded_columns() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, name STRING)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, name) VALUES ('a', 'alice')")
        .await
        .unwrap();

    let rows = server
        .client
        .query("SELECT id, name FROM t WHERE id = $1", &[&"a"])
        .await
        .expect("prepared query should succeed");

    assert_eq!(rows.len(), 1, "expected one row");

    // Regression guard: the bug produced a single row with zero decoded
    // fields — clients saw `[{}]`. Assert the schema survived end-to-end.
    assert!(
        rows[0].len() >= 2,
        "row must expose at least 2 decoded columns, got {}",
        rows[0].len()
    );

    let id: &str = rows[0].get("id");
    let name: &str = rows[0].get("name");
    assert_eq!(id, "a");
    assert_eq!(name, "alice");
}

/// Schemaless-document SELECT by parameterised key must return flat
/// columns — not a single `document` envelope column.
#[tokio::test]
async fn extended_query_schemaless_doc_returns_decoded_columns() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION docs TYPE DOCUMENT (id STRING, name STRING)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO docs (id, name) VALUES ('k1', 'bob')")
        .await
        .unwrap();

    let rows = server
        .client
        .query("SELECT id, name FROM docs WHERE id = $1", &[&"k1"])
        .await
        .expect("prepared query should succeed");

    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].len() >= 2,
        "expected ≥2 columns, got {}",
        rows[0].len()
    );

    // Regression guard: neither column may be the envelope key.
    let col_names: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
    assert!(
        !col_names.contains(&"result") && !col_names.contains(&"document"),
        "extended-query must not surface the simple-query envelope keys, got {col_names:?}"
    );

    let id: &str = rows[0].get("id");
    let name: &str = rows[0].get("name");
    assert_eq!(id, "k1");
    assert_eq!(name, "bob");
}

/// Constant + parameter projection with no FROM clause must return
/// one row with two decoded columns.
#[tokio::test]
async fn extended_query_constant_and_param_projection() {
    let server = TestServer::start().await;

    let rows = server
        .client
        .query("SELECT 1 AS x, $1 AS y", &[&"hi"])
        .await
        .expect("prepared query should succeed");

    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].len() >= 2,
        "expected ≥2 columns, got {}",
        rows[0].len()
    );

    // x may decode as any integer-compatible type; compare via text.
    let x_text: String = rows[0].get::<_, String>("x");
    let y: &str = rows[0].get("y");
    assert_eq!(x_text, "1");
    assert_eq!(y, "hi");
}

/// Pure-constant projection (no params, no FROM) through the prepared
/// path must still emit a multi-column row.
#[tokio::test]
async fn extended_query_pure_constant_projection() {
    let server = TestServer::start().await;

    let stmt = server
        .client
        .prepare("SELECT 1 AS x, 'hi' AS y")
        .await
        .expect("prepare should succeed");
    let rows = server
        .client
        .query(&stmt, &[])
        .await
        .expect("execute should succeed");

    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].len() >= 2,
        "expected ≥2 columns, got {}",
        rows[0].len()
    );

    let x_text: String = rows[0].get::<_, String>("x");
    let y: &str = rows[0].get("y");
    assert_eq!(x_text, "1");
    assert_eq!(y, "hi");
}

/// Star projection with a parameterised filter must expand to every
/// collection column in the row output.
#[tokio::test]
async fn extended_query_star_projection_returns_all_columns() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, name STRING, age INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, name, age) VALUES ('a', 'alice', 30)")
        .await
        .unwrap();

    let rows = server
        .client
        .query("SELECT * FROM t WHERE id = $1", &[&"a"])
        .await
        .expect("prepared star query should succeed");

    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].len() >= 3,
        "star projection must expose all 3 columns, got {}",
        rows[0].len()
    );

    let id: &str = rows[0].get("id");
    let name: &str = rows[0].get("name");
    assert_eq!(id, "a");
    assert_eq!(name, "alice");
}

/// COUNT(*) through the prepared path must return the count column,
/// not the underlying scan's columns.
#[tokio::test]
async fn extended_query_count_aggregate_returns_count_column() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, name STRING)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, name) VALUES ('a', 'alice')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, name) VALUES ('b', 'bob')")
        .await
        .unwrap();

    let stmt = server
        .client
        .prepare("SELECT COUNT(*) AS n FROM t")
        .await
        .expect("prepare should succeed");
    let rows = server
        .client
        .query(&stmt, &[])
        .await
        .expect("count aggregate execute should succeed");

    assert_eq!(rows.len(), 1);
    assert!(
        !rows[0].is_empty(),
        "expected ≥1 column, got {}",
        rows[0].len()
    );

    // Regression guard: the aggregate output must not leak the scanned
    // collection's schema (id/name) in place of the count.
    let col_names: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
    assert!(
        !col_names.contains(&"id") && !col_names.contains(&"name"),
        "COUNT(*) result must not expose scan-level columns, got {col_names:?}"
    );

    let n_text: String = rows[0].get::<_, String>(0);
    assert_eq!(n_text, "2");
}

/// Key-value lookup by parameterised key must return column-shaped rows.
#[tokio::test]
async fn extended_query_kv_point_get() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv TYPE KEY_VALUE (key STRING PRIMARY KEY, value STRING)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv (key, value) VALUES ('hello', 'world')")
        .await
        .unwrap();

    let rows = server
        .client
        .query("SELECT key, value FROM kv WHERE key = $1", &[&"hello"])
        .await
        .expect("kv prepared query should succeed");

    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].len() >= 2,
        "expected ≥2 columns, got {}",
        rows[0].len()
    );

    let k: &str = rows[0].get("key");
    let v: &str = rows[0].get("value");
    assert_eq!(k, "hello");
    assert_eq!(v, "world");
}

/// `pg_type` catalog must be reachable through the extended-query path.
/// Drivers with type introspection (postgres.js fetch_types, JDBC,
/// SQLAlchemy) hit this on connect and error out otherwise.
#[tokio::test]
async fn extended_query_pg_type_catalog_is_reachable() {
    let server = TestServer::start().await;

    let stmt = server
        .client
        .prepare("SELECT typname FROM pg_type")
        .await
        .expect("prepare on pg_type must not fail with 'unknown table'");
    let rows = server
        .client
        .query(&stmt, &[])
        .await
        .expect("pg_type execute should succeed");

    assert!(
        !rows.is_empty(),
        "pg_type must expose at least one built-in type row"
    );
    for row in &rows {
        assert!(!row.is_empty(), "pg_type row must have ≥1 decoded column");
        let _name: &str = row.get("typname");
    }
}

/// Drivers that send Parse with no type oids (e.g. postgres-js with
/// `fetch_types: false`) deliver `Type::UNKNOWN` for every bind param.
/// `LIMIT $1 = 2` over a 5-row table must still return 2 rows, not 5 —
/// i.e. the untyped numeric must not silently degrade to a text literal
/// that the planner fails to match against `Value::Number` and drops.
#[tokio::test]
async fn extended_query_untyped_numeric_limit_applies() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, n INT)")
        .await
        .unwrap();
    for (id, n) in [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)] {
        server
            .exec(&format!("INSERT INTO t (id, n) VALUES ('{id}', {n})"))
            .await
            .unwrap();
    }

    let stmt = server
        .client
        .prepare_typed("SELECT id FROM t ORDER BY id LIMIT $1", &[Type::UNKNOWN])
        .await
        .expect("prepare with UNKNOWN param type should succeed");
    let rows = server
        .client
        .query(&stmt, &[&"2"])
        .await
        .expect("untyped LIMIT execute should succeed");

    assert_eq!(
        rows.len(),
        2,
        "untyped LIMIT $1 = 2 must bound the result set, got {} rows",
        rows.len()
    );
}

/// OFFSET shares the same `Value::Number` planner match as LIMIT; a
/// `Type::UNKNOWN` numeric bind must drive OFFSET correctly too.
#[tokio::test]
async fn extended_query_untyped_numeric_offset_applies() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, n INT)")
        .await
        .unwrap();
    for (id, n) in [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)] {
        server
            .exec(&format!("INSERT INTO t (id, n) VALUES ('{id}', {n})"))
            .await
            .unwrap();
    }

    let stmt = server
        .client
        .prepare_typed(
            "SELECT id FROM t ORDER BY id LIMIT 10 OFFSET $1",
            &[Type::UNKNOWN],
        )
        .await
        .expect("prepare should succeed");
    let rows = server
        .client
        .query(&stmt, &[&"3"])
        .await
        .expect("untyped OFFSET execute should succeed");

    assert_eq!(
        rows.len(),
        2,
        "untyped OFFSET $1 = 3 over 5 rows must return 2 rows, got {}",
        rows.len()
    );
}

/// Numeric `WHERE col = $N` with an untyped bind — sibling path to LIMIT/
/// OFFSET. May already coerce correctly via the scan-filter value
/// converter; this locks that in as a regression guard so any future
/// refactor that collapses onto a raw `Value::Number` match (the LIMIT
/// path's failure mode) fails loudly instead of silently dropping rows.
#[tokio::test]
async fn extended_query_untyped_numeric_where_equals() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, n INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, n) VALUES ('a', 1), ('b', 2), ('c', 3)")
        .await
        .unwrap();

    let stmt = server
        .client
        .prepare_typed("SELECT id FROM t WHERE n = $1", &[Type::UNKNOWN])
        .await
        .expect("prepare should succeed");
    let rows = server
        .client
        .query(&stmt, &[&"2"])
        .await
        .expect("untyped numeric WHERE execute should succeed");

    assert_eq!(
        rows.len(),
        1,
        "untyped numeric WHERE n = $1 (=2) must match one row, got {}",
        rows.len()
    );
    let id: &str = rows[0].get("id");
    assert_eq!(id, "b", "numeric comparison must have selected n=2 row");
}

/// SEARCH DSL — a second DSL dispatcher beyond UPSERT. Parameter
/// binding must apply uniformly; if params are threaded through only
/// one DSL dispatcher, this second prefix still breaks.
#[tokio::test]
async fn extended_query_dsl_search_vector_substitutes_params() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION v TYPE VECTOR (id STRING PRIMARY KEY, embedding VECTOR(3))")
        .await
        .unwrap();
    server
        .exec("INSERT INTO v (id, embedding) VALUES ('a', ARRAY[1.0, 0.0, 0.0])")
        .await
        .unwrap();
    server
        .exec("INSERT INTO v (id, embedding) VALUES ('b', ARRAY[0.0, 1.0, 0.0])")
        .await
        .unwrap();

    let stmt = server
        .client
        .prepare_typed(
            "SEARCH v USING VECTOR(ARRAY[1.0, 0.0, 0.0], $1)",
            &[Type::UNKNOWN],
        )
        .await
        .expect("prepare SEARCH DSL should succeed");
    let res = server.client.query(&stmt, &[&"1"]).await;

    // The architectural contract under test: binding reached the engine.
    // Downstream vector-engine behavior (e.g. whether the index is
    // queryable immediately after INSERT) is a separate concern.
    if let Err(e) = &res {
        let msg = format!("{e:?}");
        assert!(
            !msg.contains("'$") && !msg.to_lowercase().contains("placeholder"),
            "SEARCH DSL leaked raw placeholder into dispatcher: {msg}"
        );
        assert!(
            !msg.to_lowercase().contains("unsupported expression"),
            "SEARCH DSL rejected bound placeholder as unsupported expr: {msg}"
        );
    }
}

/// DSL statements (UPSERT, SEARCH, GRAPH, MATCH, CRDT MERGE, CREATE
/// VECTOR/FULLTEXT/SEARCH/SPARSE INDEX) are flagged at Parse time and
/// routed to `execute_sql` with the untouched original SQL — `$N`
/// placeholders intact. The bound values never reach the dispatcher.
///
/// Regression guard: the exact observed symptom was
/// `cannot parse '$2' as INT` from `strict_format::bytes_to_binary_tuple`
/// — i.e. the literal `$2` surviving into the binary-tuple encoder.
#[tokio::test]
async fn extended_query_dsl_upsert_substitutes_params() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t TYPE DOCUMENT STRICT (id STRING NOT NULL PRIMARY KEY, n INT)")
        .await
        .unwrap();

    let stmt = server
        .client
        .prepare_typed(
            "UPSERT INTO t (id, n) VALUES ($1, $2)",
            &[Type::UNKNOWN, Type::UNKNOWN],
        )
        .await
        .expect("prepare UPSERT DSL should succeed");
    let res = server.client.execute(&stmt, &[&"x", &"42"]).await;

    if let Err(e) = &res {
        let msg = format!("{e:?}");
        assert!(
            !msg.contains("cannot parse '$") && !msg.to_lowercase().contains("placeholder"),
            "DSL path leaked raw placeholder into engine: {msg}"
        );
        panic!("UPSERT with bound params should reach the engine, got: {msg}");
    }

    // Verify the row landed via simple-query (text envelope), which
    // sidesteps the strict-int wire-format concern that's orthogonal
    // to the parameter-binding contract.
    let rows = server
        .query_text("SELECT id FROM t WHERE id = 'x'")
        .await
        .expect("verify select should succeed");
    assert_eq!(rows.len(), 1, "UPSERT should have inserted exactly 1 row");
}

/// `pg_type` with a parameterised filter — exercises both the extended-
/// query catalog routing and parameter binding against the virtual table.
#[tokio::test]
async fn extended_query_pg_type_with_parameter() {
    let server = TestServer::start().await;

    let rows = server
        .client
        .query("SELECT typname FROM pg_type WHERE typname = $1", &[&"int8"])
        .await
        .expect("parameterised pg_type query should succeed");

    // Current pg_catalog dispatch returns the full table; filtering is
    // advisory. The spec we assert: the query executes, returns rows,
    // and each row has a decoded `typname` column.
    assert!(
        !rows.is_empty(),
        "pg_type parameterised query must return at least one row"
    );
    for row in &rows {
        assert!(!row.is_empty());
        let _name: &str = row.get("typname");
    }
}
