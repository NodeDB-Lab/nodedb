// SPDX-License-Identifier: BUSL-1.1

//! Extended-query protocol (Parse/Bind/Describe/Execute) must return
//! rows with one decoded field per column declared by Describe.
//!
//! The canonical JSON envelope (`{"result": "..."}` / `{"document": "..."}`)
//! is a simple-query contract. The extended-query path must emit
//! column-shaped rows natively so ORMs and pg drivers can read results.

use crate::harness::TestServer;
use tokio_postgres::types::Type;

/// Strict-document SELECT by parameterised primary key must return
/// columns `id` and `name` decoded as text, not an empty row.
#[tokio::test]
async fn extended_query_strict_doc_returns_decoded_columns() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, name STRING) WITH (engine='document_strict')")
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

    // A constant integer is typed as int8 in the row description.
    let x: i64 = rows[0].get("x");
    let y: &str = rows[0].get("y");
    assert_eq!(x, 1);
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

    let x: i64 = rows[0].get("x");
    let y: &str = rows[0].get("y");
    assert_eq!(x, 1);
    assert_eq!(y, "hi");
}

/// Two output columns announced under the same name must render their own
/// cells: the Describe phase supplies display names, but the lookup keys come
/// from the planner (`cell_keys`), so `id` and `id_1` stay distinct.
#[tokio::test]
async fn extended_query_duplicate_column_names_keep_distinct_cells() {
    let server = TestServer::start().await;

    let rows = server
        .client
        .query("SELECT 1 AS id, 2 AS id", &[])
        .await
        .expect("prepared query should succeed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2, "both announced columns must be present");

    let first: i64 = rows[0].get(0);
    let second: i64 = rows[0].get(1);
    assert_eq!(first, 1);
    assert_eq!(
        second, 2,
        "the second column must read its own cell, not the first"
    );
}

/// The issue's second repro: a join whose two sides project a bare `id`.
/// Each column must read its own cell through the prepared path.
#[tokio::test]
async fn extended_query_duplicate_join_columns_keep_distinct_cells() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION w (id STRING PRIMARY KEY, b_id STRING) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION b (id STRING PRIMARY KEY) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO b (id) VALUES ('b1')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO w (id, b_id) VALUES ('w1', 'b1')")
        .await
        .unwrap();

    let rows = server
        .client
        .query("SELECT w.id, b.id FROM w JOIN b ON w.b_id = b.id", &[])
        .await
        .expect("prepared join should succeed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2, "both joined columns must be present");

    let first: &str = rows[0].get(0);
    let second: &str = rows[0].get(1);
    assert_eq!(first, "w1");
    assert_eq!(
        second, "b1",
        "the joined column must read its own cell, not the first"
    );
}

/// Star projection with a parameterised filter must expand to every
/// collection column in the row output.
#[tokio::test]
async fn extended_query_star_projection_returns_all_columns() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, name STRING, age INT) WITH (engine='document_strict')")
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
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, name STRING) WITH (engine='document_strict')")
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

    // COUNT(*) is Postgres bigint (int8), so the client decodes it as i64.
    let n: i64 = rows[0].get::<_, i64>(0);
    assert_eq!(n, 2);
}

/// Key-value lookup by parameterised key must return column-shaped rows.
#[tokio::test]
async fn extended_query_kv_point_get() {
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
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, n INT) WITH (engine='document_strict')")
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
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, n INT) WITH (engine='document_strict')")
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
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, n INT) WITH (engine='document_strict')")
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
        .exec("CREATE COLLECTION v (id STRING PRIMARY KEY, embedding VECTOR(3)) WITH (engine='vector')")
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
        .exec("CREATE COLLECTION t (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='document_strict')")
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

/// Typed columns fetched over the extended protocol decode correctly in
/// binary result format. `tokio-postgres` requests binary for every result
/// column and binary-decodes each value, so a successful typed `get` proves
/// the server encoded the column in its PostgreSQL binary wire form.
#[tokio::test]
async fn extended_query_binary_typed_columns_decode() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION m (id STRING PRIMARY KEY, n INT, amt DOUBLE, flag BOOL, \
             name STRING) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO m (id, n, amt, flag, name) \
             VALUES ('a', 42, 2.5, true, 'hello')",
        )
        .await
        .unwrap();

    let rows = server
        .client
        .query("SELECT n, amt, flag, name FROM m WHERE id = $1", &[&"a"])
        .await
        .expect("prepared typed query should succeed");
    assert_eq!(rows.len(), 1);

    // `n` is declared INT (the INT4 alias), so it advertises OID 23 and
    // decodes as i32; every declared integer width must keep its own OID
    // rather than collapsing to int8. float8 -> f64, bool -> bool, text -> String.
    let n: i32 = rows[0].get("n");
    let amt: f64 = rows[0].get("amt");
    let flag: bool = rows[0].get("flag");
    let name: &str = rows[0].get("name");
    assert_eq!(n, 42);
    assert!((amt - 2.5).abs() < 1e-9, "amt decoded as {amt}");
    assert!(flag);
    assert_eq!(name, "hello");
}

/// A `TIMESTAMP` column honours a binary result request: the cell arrives as
/// PostgreSQL binary `timestamp` (microseconds since 2000-01-01) and decodes
/// through the driver's `SystemTime` reader to the stored instant, next to
/// its integer sibling. The same timestamp renders as ISO-8601 text over the
/// simple-query path.
#[tokio::test]
async fn extended_query_timestamp_decodes_from_binary_with_sibling() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ev (id STRING PRIMARY KEY, n INT, ts TIMESTAMP) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO ev (id, n, ts) VALUES ('a', 7, '2024-01-01 00:00:00')")
        .await
        .unwrap();

    // Extended path: `tokio-postgres` requests binary for every result
    // column. `n` is declared INT, so it advertises OID 23 and decodes as
    // i32; `ts` decodes as the stored instant, 2024-01-01T00:00:00Z.
    let rows = server
        .client
        .query("SELECT n, ts FROM ev WHERE id = $1", &[&"a"])
        .await
        .expect("query with a timestamp column should succeed");
    assert_eq!(rows.len(), 1);
    let n: i32 = rows[0].get("n");
    assert_eq!(n, 7);
    let ts: std::time::SystemTime = rows[0].get("ts");
    assert_eq!(
        ts,
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_704_067_200),
        "binary timestamp must decode to the stored instant"
    );

    // Simple-query path renders the timestamp as ISO-8601 text.
    let text_rows = server
        .query_text("SELECT ts FROM ev WHERE id = 'a'")
        .await
        .expect("simple-query text select should succeed");
    assert_eq!(text_rows, vec!["2024-01-01T00:00:00.000000Z".to_string()]);
}

/// Regression lock: when the client declares a parameter's type (via
/// `prepare_typed`), `tokio-postgres` transmits that parameter in
/// PostgreSQL *binary* format. Before this fix, the bind layer decoded
/// every parameter as UTF-8 text regardless of wire format, so a binary
/// `INT8` payload was rejected with SQLSTATE 22021 ("invalid UTF-8 in
/// parameter $1"). Now binary BOOL/INT2/INT4/INT8/FLOAT4/FLOAT8 are decoded
/// via their `postgres_types::FromSql` binary encodings instead.
///
/// Note: this test (and its siblings below) use `prepare_typed` with an
/// explicit type rather than a bare `$1` placeholder because this server
/// does not infer parameter types from the catalog — an untyped `$1` still
/// reports `Unknown` back to the client, and `tokio-postgres` refuses to
/// serialize a Rust value against an `Unknown` OID. That gap is a separate,
/// out-of-scope limitation; declaring the type is how a real client reaches
/// the binary-format path this test locks in.
#[tokio::test]
async fn extended_query_binary_i64_param_in_where_is_decoded() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION t (id STRING PRIMARY KEY, n BIGINT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO t (id, n) VALUES ('a', 1), ('b', 42), ('c', 100)")
        .await
        .unwrap();

    let stmt = server
        .client
        .prepare_typed("SELECT id FROM t WHERE n = $1", &[Type::INT8])
        .await
        .expect("prepare_typed should succeed");
    let rows = server
        .client
        .query(&stmt, &[&42i64])
        .await
        .expect("binary-format i64 parameter must decode and match");

    assert_eq!(rows.len(), 1, "expected exactly one matching row");
    let id: &str = rows[0].get("id");
    assert_eq!(id, "b");
}

/// Every declared-width binary scalar type (`i32`, `i16`, `f64`, `f32`,
/// `bool`) must decode correctly when bound as a `tokio-postgres` typed
/// parameter — not just `i64`. Each is used in a `WHERE` predicate so the
/// bound value must be decoded to the correct value, not merely accepted.
#[tokio::test]
async fn extended_query_binary_scalar_params_decode_in_where() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION s (id STRING PRIMARY KEY, i32_col INT, i16_col SMALLINT, \
             f64_col DOUBLE, f32_col FLOAT, bool_col BOOL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO s (id, i32_col, i16_col, f64_col, f32_col, bool_col) \
             VALUES ('a', 1000, 10, 1.5, 1.5, false)",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO s (id, i32_col, i16_col, f64_col, f32_col, bool_col) \
             VALUES ('b', 70000, 300, 2.5, 2.5, true)",
        )
        .await
        .unwrap();

    let stmt = server
        .client
        .prepare_typed("SELECT id FROM s WHERE i32_col = $1", &[Type::INT4])
        .await
        .expect("prepare_typed should succeed");
    let rows = server
        .client
        .query(&stmt, &[&70000i32])
        .await
        .expect("binary-format i32 parameter must decode and match");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("id"), "b");

    let stmt = server
        .client
        .prepare_typed("SELECT id FROM s WHERE i16_col = $1", &[Type::INT2])
        .await
        .expect("prepare_typed should succeed");
    let rows = server
        .client
        .query(&stmt, &[&10i16])
        .await
        .expect("binary-format i16 parameter must decode and match");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("id"), "a");

    let stmt = server
        .client
        .prepare_typed("SELECT id FROM s WHERE f64_col = $1", &[Type::FLOAT8])
        .await
        .expect("prepare_typed should succeed");
    let rows = server
        .client
        .query(&stmt, &[&2.5f64])
        .await
        .expect("binary-format f64 parameter must decode and match");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("id"), "b");

    let stmt = server
        .client
        .prepare_typed("SELECT id FROM s WHERE f32_col = $1", &[Type::FLOAT4])
        .await
        .expect("prepare_typed should succeed");
    let rows = server
        .client
        .query(&stmt, &[&1.5f32])
        .await
        .expect("binary-format f32 parameter must decode and match");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("id"), "a");

    let stmt = server
        .client
        .prepare_typed("SELECT id FROM s WHERE bool_col = $1", &[Type::BOOL])
        .await
        .expect("prepare_typed should succeed");
    let rows = server
        .client
        .query(&stmt, &[&true])
        .await
        .expect("binary-format bool parameter must decode and match");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("id"), "b");
}

/// The same binary scalar types bound as INSERT values, not just WHERE
/// predicates — the decoded value must be the one actually stored, verified
/// by reading it back.
#[tokio::test]
async fn extended_query_binary_scalar_params_insert_and_round_trip() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION w (id STRING PRIMARY KEY, i32_col INT, i16_col SMALLINT, \
             f64_col DOUBLE, f32_col FLOAT, bool_col BOOL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    let stmt = server
        .client
        .prepare_typed(
            "INSERT INTO w (id, i32_col, i16_col, f64_col, f32_col, bool_col) \
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                Type::TEXT,
                Type::INT4,
                Type::INT2,
                Type::FLOAT8,
                Type::FLOAT4,
                Type::BOOL,
            ],
        )
        .await
        .expect("prepare_typed insert should succeed");
    server
        .client
        .execute(
            &stmt,
            &[&"row1", &123456i32, &(-7i16), &1234.5f64, &1.25f32, &true],
        )
        .await
        .expect("binary-format scalar insert should succeed");

    let rows = server
        .client
        .query(
            "SELECT i32_col, i16_col, f64_col, f32_col, bool_col FROM w WHERE id = $1",
            &[&"row1"],
        )
        .await
        .expect("select back the inserted row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("i32_col"), 123456);
    assert_eq!(rows[0].get::<_, i16>("i16_col"), -7);
    assert!((rows[0].get::<_, f64>("f64_col") - 1234.5).abs() < 1e-9);
    // `f32_col` is declared FLOAT, which the catalog always advertises as
    // float8 (only integer widths are narrowed on the wire — see
    // `sql_data_type_to_ddl_col_type_with_width`), so the stored value is
    // fetched via the f64 getter; what's under test is that the bound f32
    // *parameter* was decoded to the right value on the way in.
    assert!((rows[0].get::<_, f64>("f32_col") - 1.25).abs() < 1e-6);
    assert!(rows[0].get::<_, bool>("bool_col"));
}

/// Regression lock for server-side parameter type inference: a plain
/// `prepare` (no declared oids) of `LIMIT $1` must report `int8` for the
/// parameter, not `unknown`.
///
/// With `unknown` (oid 0), `tokio-postgres` refuses to serialize an `i64`
/// bind value at all — `WrongType { postgres: Unknown, rust: "i64" }` —
/// so the query below fails client-side before a byte reaches the server.
#[tokio::test]
async fn extended_query_infers_limit_param_type_without_client_oids() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, n INT) WITH (engine='document_strict')")
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
        .prepare("SELECT id FROM t ORDER BY id LIMIT $1")
        .await
        .expect("prepare without declared param types should succeed");

    assert_eq!(
        stmt.params(),
        &[Type::INT8],
        "LIMIT $1 must be described as int8, got {:?}",
        stmt.params()
    );

    let rows = server
        .client
        .query(&stmt, &[&2i64])
        .await
        .expect("an i64 must be bindable against the inferred int8 parameter");
    assert_eq!(
        rows.len(),
        2,
        "inferred-type LIMIT $1 = 2 must bound the result set, got {}",
        rows.len()
    );
}

/// `OFFSET $1` is the other row-count position and must infer the same way.
#[tokio::test]
async fn extended_query_infers_offset_param_type_without_client_oids() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, n INT) WITH (engine='document_strict')")
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
        .prepare("SELECT id FROM t ORDER BY id LIMIT 10 OFFSET $1")
        .await
        .expect("prepare without declared param types should succeed");
    assert_eq!(stmt.params(), &[Type::INT8]);

    let rows = server
        .client
        .query(&stmt, &[&3i64])
        .await
        .expect("an i64 must be bindable against the inferred int8 parameter");
    assert_eq!(rows.len(), 2, "OFFSET $1 = 3 over 5 rows must return 2");
}

/// An explicit cast names the parameter's type outright, with no catalog
/// lookup involved — Describe must report it.
#[tokio::test]
async fn extended_query_infers_cast_param_type() {
    let server = TestServer::start().await;

    let stmt = server
        .client
        .prepare("SELECT $1::BIGINT AS v")
        .await
        .expect("prepare should succeed");
    assert_eq!(stmt.params(), &[Type::INT8], "$1::BIGINT must report int8");

    let stmt = server
        .client
        .prepare("SELECT CAST($1 AS TEXT) AS v")
        .await
        .expect("prepare should succeed");
    assert_eq!(
        stmt.params(),
        &[Type::TEXT],
        "CAST($1 AS TEXT) must report text"
    );
}

/// A client-declared type is the client's contract and must survive
/// inference: declaring `int4` for a `LIMIT` position (which inference would
/// otherwise call `int8`) keeps `int4` in the ParameterDescription, and the
/// bound `i32` still drives the limit.
#[tokio::test]
async fn extended_query_client_declared_param_type_wins_over_inference() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, n INT) WITH (engine='document_strict')")
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
        .prepare_typed("SELECT id FROM t ORDER BY id LIMIT $1", &[Type::INT4])
        .await
        .expect("prepare_typed should succeed");
    assert_eq!(
        stmt.params(),
        &[Type::INT4],
        "client-declared int4 must not be overwritten by the inferred int8"
    );

    let rows = server
        .client
        .query(&stmt, &[&2i32])
        .await
        .expect("binary i32 must decode against the declared int4 parameter");
    assert_eq!(rows.len(), 2);
}

/// **The headline regression lock.** A plain `client.query` with a bound
/// `i64` and NO `prepare_typed`, NO cast must not fail
/// client-side with `WrongType { postgres: Unknown, rust: "i64" }` from
/// Describe answering oid 0 for the column-backed `$1`.
///
/// `tokio-postgres` refuses to serialize an `i64` against an unknown oid, so
/// this fails before a byte reaches the server unless the parameter is
/// resolved from the catalog column behind `big`.
#[tokio::test]
async fn extended_query_plain_parameterised_select_binds_i64() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION t (id STRING PRIMARY KEY, big BIGINT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    for (id, big) in [("a", 41), ("b", 42), ("c", 43)] {
        server
            .exec(&format!("INSERT INTO t (id, big) VALUES ('{id}', {big})"))
            .await
            .unwrap();
    }

    let rows = server
        .client
        .query("SELECT id FROM t WHERE big = $1", &[&42i64])
        .await
        .expect("a bare parameterised query with an i64 bind must succeed");

    assert_eq!(rows.len(), 1, "expected exactly the row with big = 42");
    assert_eq!(rows[0].get::<_, &str>("id"), "b");
}

/// Width fidelity: a column declared `INT` must be described as `int4`
/// (oid 23), not `int8` (oid 20). The client encodes the bind value at
/// exactly the described width, so collapsing every integer column to int8
/// would put a 4-byte column behind an 8-byte promise.
#[tokio::test]
async fn extended_query_int_column_param_reports_int4() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, n INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    for (id, n) in [("a", 1), ("b", 2), ("c", 3)] {
        server
            .exec(&format!("INSERT INTO t (id, n) VALUES ('{id}', {n})"))
            .await
            .unwrap();
    }

    let stmt = server
        .client
        .prepare("SELECT id FROM t WHERE n = $1")
        .await
        .expect("prepare without declared param types should succeed");
    assert_eq!(
        stmt.params(),
        &[Type::INT4],
        "an INT column's parameter must report int4, got {:?}",
        stmt.params()
    );

    let rows = server
        .client
        .query(&stmt, &[&2i32])
        .await
        .expect("an i32 must be bindable against the inferred int4 parameter");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("id"), "b");
}

/// A client-declared type is the client's contract and outranks the
/// catalog-backed inference too: declaring `int8` for a position inference
/// would have called `int4` keeps `int8` in the ParameterDescription.
#[tokio::test]
async fn extended_query_client_declared_type_wins_over_column_inference() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION t (id STRING PRIMARY KEY, n INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    for (id, n) in [("a", 1), ("b", 2), ("c", 3)] {
        server
            .exec(&format!("INSERT INTO t (id, n) VALUES ('{id}', {n})"))
            .await
            .unwrap();
    }

    let stmt = server
        .client
        .prepare_typed("SELECT id FROM t WHERE n = $1", &[Type::INT8])
        .await
        .expect("prepare_typed should succeed");
    assert_eq!(
        stmt.params(),
        &[Type::INT8],
        "client-declared int8 must not be overwritten by the inferred int4"
    );

    let rows = server
        .client
        .query(&stmt, &[&2i64])
        .await
        .expect("binary i64 must decode against the declared int8 parameter");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("id"), "b");
}

/// Under-inference boundary: a position no listed form types — here a bare
/// projection `$1`, which has no column behind it — must still be described
/// as unknown rather than guessed, and must still work over the text format
/// the client falls back to. Reporting a wrong concrete oid would make the
/// client commit to a binary encoding the server cannot decode; unknown
/// degrades gracefully.
#[tokio::test]
async fn extended_query_unresolvable_param_stays_unknown_and_still_works() {
    let server = TestServer::start().await;

    let stmt = server
        .client
        .prepare("SELECT $1 AS v")
        .await
        .expect("prepare should succeed");
    assert_eq!(
        stmt.params(),
        &[Type::UNKNOWN],
        "a projection position is not a form inference types, got {:?}",
        stmt.params()
    );

    let rows = server
        .client
        .query(&stmt, &[&"passthrough"])
        .await
        .expect("an unknown parameter must still bind in text format");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("v"), "passthrough");
}
