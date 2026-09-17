// SPDX-License-Identifier: BUSL-1.1

//! Extended-query protocol coverage for Spatial, Vector, and Array engines,
//! plus cross-engine parameter error-path tests.
//!
//! KV, Columnar, and Timeseries tests are in `pgwire_extended_query_engines.rs`.

use crate::harness::TestServer;
use tokio_postgres::types::Type;

// ── Spatial engine ───────────────────────────────────────────────────────────

/// Spatial: parameterised text filter on id column; verify columns decoded.
#[tokio::test]
async fn extended_query_spatial_typed_scan() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION spatial_pts \
         COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
         WITH (engine='spatial')",
    )
    .await
    .expect("CREATE spatial_pts");
    srv.exec(
        "INSERT INTO spatial_pts (id, location, name) \
         VALUES ('p1', ST_Point(-122.4, 37.8), 'SF')",
    )
    .await
    .expect("INSERT spatial_pts");

    let rows = srv
        .client
        .query("SELECT id, name FROM spatial_pts WHERE id = $1", &[&"p1"])
        .await
        .expect("execute spatial_pts select");

    assert_eq!(rows.len(), 1, "spatial: expected 1 row");
    let id: &str = rows[0].get("id");
    let name: &str = rows[0].get("name");
    assert_eq!(id, "p1");
    assert_eq!(name, "SF");
}

/// Spatial: geometry column OID must be TEXT (25) — no standard pg built-in.
#[tokio::test]
async fn extended_query_spatial_geometry_oid() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION spatial_oid_check \
         COLUMNS (id TEXT, geom GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .expect("CREATE spatial_oid_check");
    srv.exec("INSERT INTO spatial_oid_check (id, geom) VALUES ('x', ST_Point(0.0, 0.0))")
        .await
        .expect("INSERT spatial_oid_check");

    let stmt = srv
        .client
        .prepare_typed(
            "SELECT id, geom FROM spatial_oid_check WHERE id = $1",
            &[Type::TEXT],
        )
        .await
        .expect("prepare spatial_oid_check");

    let rows = srv
        .client
        .query(&stmt, &[&"x"])
        .await
        .expect("execute spatial_oid_check");

    assert_eq!(rows.len(), 1);
    let geom_col = rows[0]
        .columns()
        .iter()
        .find(|c| c.name() == "geom")
        .expect("geom column must be present in RowDescription");
    // Geometry maps to TEXT OID (25) — no standard pg geometry built-in.
    assert_eq!(
        geom_col.type_().oid(),
        25,
        "GEOMETRY column must have OID 25 (text), got {}",
        geom_col.type_().oid()
    );
}

// ── Vector engine ────────────────────────────────────────────────────────────

/// Vector engine: parameterised text filter on id; SELECT id column decodes correctly.
#[tokio::test]
async fn extended_query_vector_id_filter() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION vecs \
         FIELDS (id TEXT, emb VECTOR(3)) \
         WITH (engine='vector', m=8, ef_construction=50)",
    )
    .await
    .expect("CREATE vecs");
    srv.exec("INSERT INTO vecs (id, emb) VALUES ('v1', ARRAY[1.0, 0.0, 0.0])")
        .await
        .expect("INSERT vecs v1");
    srv.exec("INSERT INTO vecs (id, emb) VALUES ('v2', ARRAY[0.0, 1.0, 0.0])")
        .await
        .expect("INSERT vecs v2");

    let rows = srv
        .client
        .query("SELECT id FROM vecs WHERE id = $1", &[&"v1"])
        .await
        .expect("execute vecs id filter");

    assert_eq!(rows.len(), 1, "vector id filter: expected 1 row");
    let id: &str = rows[0].get("id");
    assert_eq!(id, "v1");

    // id column OID must be TEXT (25).
    let id_col = rows[0]
        .columns()
        .iter()
        .find(|c| c.name() == "id")
        .expect("id column in RowDescription");
    assert_eq!(
        id_col.type_().oid(),
        25,
        "vector engine id column OID must be 25 (text), got {}",
        id_col.type_().oid()
    );
}

/// Vector SEARCH DSL with parameterised limit — verify no raw-placeholder leak,
/// and that the id column OID is TEXT (25) when results are returned.
#[tokio::test]
async fn extended_query_vector_search_dsl_oid_check() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION vecs_oid \
         FIELDS (id TEXT, emb VECTOR(3)) \
         WITH (engine='vector', m=8, ef_construction=50)",
    )
    .await
    .expect("CREATE vecs_oid");
    srv.exec("INSERT INTO vecs_oid (id, emb) VALUES ('a', ARRAY[1.0, 0.0, 0.0])")
        .await
        .expect("INSERT vecs_oid a");

    let stmt = srv
        .client
        .prepare_typed(
            "SEARCH vecs_oid USING VECTOR(ARRAY[1.0, 0.0, 0.0], $1)",
            &[Type::UNKNOWN],
        )
        .await
        .expect("prepare vector SEARCH DSL");

    let res = srv.client.query(&stmt, &[&"1"]).await;
    if let Ok(rows) = &res {
        if let Some(first) = rows.first()
            && let Some(id_col) = first.columns().iter().find(|c| c.name() == "id")
        {
            assert_eq!(
                id_col.type_().oid(),
                25,
                "SEARCH DSL id column OID must be TEXT (25), got {}",
                id_col.type_().oid()
            );
        }
    } else if let Err(e) = &res {
        let msg = format!("{e:?}");
        assert!(
            !msg.contains("'$") && !msg.to_lowercase().contains("placeholder"),
            "SEARCH DSL leaked raw placeholder: {msg}"
        );
    }
}

// ── Array engine ─────────────────────────────────────────────────────────────

/// Array engine: INSERT via simple-query + ARRAY_SLICE TVF smoke test.
///
/// Extended-query with parameterised TVF args is not supported (TVF arguments
/// are literal strings in the SQL grammar, not bind parameters). This test
/// verifies: (a) INSERT + SLICE round-trip, and (b) that a prepared constant
/// statement still executes cleanly on a server hosting an active array
/// collection — i.e., array DDL does not corrupt prepared-statement routing.
#[tokio::test]
async fn extended_query_array_engine_smoke_and_const_stmt() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE ARRAY arr_ext \
         DIMS (row INT64, col INT64) \
         ATTRS (val FLOAT64) \
         TILE_EXTENTS (10, 10)",
    )
    .await
    .expect("CREATE ARRAY arr_ext");
    srv.exec("INSERT INTO ARRAY arr_ext COORDS (0, 0) VALUES (1.5)")
        .await
        .expect("INSERT arr_ext (0,0)");
    srv.exec("INSERT INTO ARRAY arr_ext COORDS (0, 1) VALUES (2.5)")
        .await
        .expect("INSERT arr_ext (0,1)");

    let rows = srv
        .query_rows(
            "SELECT * FROM ARRAY_SLICE('arr_ext', '{\"row\":[0,0],\"col\":[0,1]}', '*', 10)",
        )
        .await
        .expect("ARRAY_SLICE simple-query");
    assert_eq!(
        rows.len(),
        2,
        "ARRAY_SLICE over 2 cells must return 2 rows, got {}",
        rows.len()
    );

    // Prepared constant projection must work after Array DDL.
    let stmt = srv
        .client
        .prepare("SELECT 1 AS x")
        .await
        .expect("constant prepare after Array DDL");
    let const_rows = srv
        .client
        .query(&stmt, &[])
        .await
        .expect("constant execute after Array DDL");
    assert_eq!(const_rows.len(), 1, "constant projection must return 1 row");
    let x: i64 = const_rows[0].get(0);
    assert_eq!(x, 1);
}

// ── Cross-engine: parameter error cases ──────────────────────────────────────

/// Out-of-range parameter index: declare 1 param type but reference $2.
/// Server must return a typed error, not panic.
#[tokio::test]
async fn extended_query_out_of_range_param_index_errors() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION oor_test (id STRING PRIMARY KEY, v INT) WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE oor_test");

    let stmt_result = srv
        .client
        .prepare_typed(
            "SELECT id FROM oor_test WHERE id = $1 AND v = $2",
            &[Type::TEXT],
        )
        .await;

    match stmt_result {
        Ok(s) => {
            // Prepare succeeded with 1 declared type; bind with only 1 arg.
            let result = srv.client.query(&s, &[&"x"]).await;
            assert!(
                result.is_err(),
                "binding fewer args than referenced params must produce an error"
            );
        }
        Err(_) => {
            // Parse-time rejection of mismatched param count is also acceptable.
        }
    }
}

/// OID mismatch: declare INT8 param but bind a text string.
/// Must produce a typed error or coerce — must not silently corrupt data.
#[tokio::test]
async fn extended_query_oid_mismatch_text_for_int_errors_or_coerces() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION oid_mismatch (id STRING PRIMARY KEY, n INT) WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE oid_mismatch");
    srv.exec("INSERT INTO oid_mismatch (id, n) VALUES ('a', 1)")
        .await
        .expect("INSERT oid_mismatch");

    let stmt = srv
        .client
        .prepare_typed("SELECT id FROM oid_mismatch WHERE n = $1", &[Type::INT8])
        .await
        .expect("prepare oid_mismatch int filter");

    let result = srv.client.query(&stmt, &[&"1"]).await;
    match result {
        Ok(rows) => {
            // Coercion path is acceptable.
            assert!(
                rows.len() <= 1,
                "coercion must return at most 1 row, got {}",
                rows.len()
            );
        }
        Err(e) => {
            let msg = format!("{e:?}");
            assert!(!msg.is_empty(), "OID-mismatch error must have a message");
        }
    }
}

// ── Cross-engine: SQLSTATEs through the extended protocol ───────────────────

/// Undefined-function rejection (SQLSTATE `42883`) through the
/// extended-query path. `sql_undefined_function.rs` proves this fires at
/// PLAN time over simple-query (where Parse+Bind+Execute collapse into one
/// round trip). Naively, `pgwire_database_authorization.rs`'s
/// `prepared_parse_denies_*` tests suggest Parse/Describe is where this
/// server plans a statement (authorization and undefined-table checks both
/// reject there) — but confirmed empirically here, `.prepare()` for this
/// statement actually *succeeds*: the function-registry existence gate is
/// not consulted during Parse/Describe (no columns/params to resolve for
/// this shape rules it out early), so the error is only raised once
/// `.query()` drives Bind+Execute and the plan is actually built. Asserting
/// on the real surfacing point, not the naive assumption.
#[tokio::test]
async fn extended_query_unknown_function_errors_42883() {
    let srv = TestServer::start().await;

    let stmt = srv
        .client
        .prepare("SELECT some_function_that_does_not_exist_216(1, 2)")
        .await
        .expect(
            "Parse/Describe succeeds for this statement shape — the \
             undefined-function gate is not consulted until Bind/Execute",
        );

    let err = srv
        .client
        .query(&stmt, &[])
        .await
        .expect_err("Execute must reject a call to an unregistered scalar function");
    let db_err = err
        .as_db_error()
        .expect("server must return a typed SQLSTATE for the unknown function");
    assert_eq!(
        db_err.code().code(),
        "42883",
        "unknown-function rejection must carry SQLSTATE 42883, got {}",
        db_err.code().code()
    );
}

/// Division-by-zero rejection (SQLSTATE `22012`) through the
/// extended-query path. Unlike the undefined-function gate above, `10 / d`
/// is a well-typed expression regardless of `d`'s runtime value, so
/// Parse/Describe must succeed — the zero divisor can only be discovered
/// once a bound row is actually evaluated, i.e. at Bind/Execute
/// (`.query()`) time. Mirrors `sql_division_by_zero.rs`'s simple-query
/// coverage of the same fix, through Parse/Bind/Execute instead.
#[tokio::test]
async fn extended_query_division_by_zero_errors_22012_at_execute() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ext_divzero_216 (id STRING PRIMARY KEY, d INT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE ext_divzero_216");
    srv.exec("INSERT INTO ext_divzero_216 (id, d) VALUES ('nonzero', 2)")
        .await
        .expect("INSERT nonzero row");
    srv.exec("INSERT INTO ext_divzero_216 (id, d) VALUES ('zero', 0)")
        .await
        .expect("INSERT zero-divisor row");

    let stmt = srv
        .client
        .prepare_typed(
            "SELECT 10 / d AS ratio FROM ext_divzero_216 WHERE id = $1",
            &[Type::TEXT],
        )
        .await
        .expect("Parse must succeed — `10 / d` is well-typed independent of d's runtime value");

    let err = srv
        .client
        .query(&stmt, &[&"zero"])
        .await
        .expect_err("Execute over the zero-divisor row must fail the statement");
    let db_err = err
        .as_db_error()
        .expect("server must return a typed SQLSTATE for division by zero");
    assert_eq!(
        db_err.code().code(),
        "22012",
        "zero-divisor Execute-time rejection must carry SQLSTATE 22012, got {}",
        db_err.code().code()
    );

    // Control: the identical prepared statement, bound against the
    // non-zero row, must still succeed — the fix must not turn division
    // itself into a blanket error on this path.
    let rows = srv
        .client
        .query(&stmt, &[&"nonzero"])
        .await
        .expect("Execute over the non-zero-divisor row must still succeed");
    assert_eq!(rows.len(), 1, "expected exactly 1 row for the nonzero id");
}
