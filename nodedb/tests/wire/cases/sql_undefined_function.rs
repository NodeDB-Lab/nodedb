// SPDX-License-Identifier: BUSL-1.1

//! Pgwire coverage for undefined scalar function calls: a call to a scalar
//! function with no registered definition must fail the whole statement at
//! PLAN time with
//! SQLSTATE `42883` (`undefined_function`), not silently evaluate to a NULL
//! row (the pre-fix behavior — the runtime evaluator's `try_eval` chain in
//! `nodedb-query/src/functions/mod.rs` ends with
//! `eval_geo_function(name, args).unwrap_or(Value::Null)`, so an unknown
//! name never surfaced as an error before this fix landed).

use crate::harness::TestServer;

/// The baseline case: an unknown scalar function name in a `SELECT` with no
/// `FROM` must be rejected with SQLSTATE `42883`.
#[tokio::test]
async fn unknown_scalar_function_errors_42883() {
    let srv = TestServer::start().await;

    srv.expect_error("SELECT some_function_that_does_not_exist(1, 2)", "42883")
        .await;
}

/// Plan-time proof: the error must fire even when the statement would scan
/// zero rows. If the rejection happened during row-by-row evaluation
/// instead of at plan time, a `LIMIT 0` (or an empty collection) would
/// short-circuit before evaluating any row and the call would go
/// unnoticed. Pairing an existing,
/// genuinely empty collection with `LIMIT 0` gives two independent reasons
/// no row would ever reach the evaluator, so an error here can only come
/// from planning.
#[tokio::test]
async fn unknown_scalar_function_errors_with_zero_rows_scanned() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION undefined_fn_empty")
        .await
        .unwrap();

    srv.expect_error(
        "SELECT bogus_fn_zero_rows(1) FROM undefined_fn_empty LIMIT 0",
        "42883",
    )
    .await;
}

/// Positive control: a plain, valid builtin scalar function must keep
/// working — the undefined-function gate must not become a false positive
/// on real functions.
#[tokio::test]
async fn known_scalar_function_still_works() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_text("SELECT UPPER('hello')")
        .await
        .expect("UPPER('hello') must still succeed");
    assert_eq!(rows, vec!["HELLO".to_string()]);
}

/// Positive control for the geo/spatial risk: many
/// `geo_*` utility functions (evaluated by
/// `nodedb_query::geo_functions::eval_geo_function`) were historically
/// absent from the SQL-side `FunctionRegistry` that plan-time validation
/// consults — a naive existence gate would have wrongly rejected them.
/// `geo_as_wkt` is one such function; `ST_Point` supplies its geometry
/// argument. Both must resolve and produce the expected WKT text.
#[tokio::test]
async fn known_geo_function_still_works() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_text("SELECT geo_as_wkt(ST_Point(1.0, 2.0))")
        .await
        .expect("geo_as_wkt(ST_Point(...)) must still succeed");
    assert_eq!(rows, vec!["POINT(1 2)".to_string()]);
}

/// Control re-assert: an undefined *table* must still report `42P01`
/// (`undefined_table`), distinct from the new `42883` undefined-function
/// path this file otherwise covers — proving the new gate didn't fold the
/// two error classes together or regress the existing one.
#[tokio::test]
async fn undefined_table_still_errors_42p01() {
    let srv = TestServer::start().await;

    srv.expect_error("SELECT * FROM this_collection_does_not_exist_216", "42P01")
        .await;
}

/// Registry-completeness smoke tests (adversarial-review follow-up):
/// `FunctionRegistry` was originally seeded from only the *old* undocumented
/// function surface, while `nodedb_query::functions::*::try_eval`'s match
/// arms cover a much larger set the runtime evaluator has supported all
/// along. Before those registrations were added, every call below failed
/// plan time with `42883` even though the runtime fully implements them —
/// exactly the false-positive risk this file's `known_geo_function_still_works`
/// test already calls out for the `geo_*` family. One representative per
/// function family, each routed through pgwire like a real client query.
#[tokio::test]
async fn registry_completeness_smoke_math_sqrt() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT sqrt(16.0)")
        .await
        .expect("sqrt(16.0) must succeed now that math.rs registers it");
    // `16.0` is numeric, so the root is numeric at PostgreSQL's scale.
    assert_eq!(rows, vec!["4.000000000000000".to_string()]);
}

#[tokio::test]
async fn registry_completeness_smoke_math_mod() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT mod(10, 3)")
        .await
        .expect("mod(10, 3) must succeed now that math.rs registers it");
    assert_eq!(rows, vec!["1".to_string()]);
}

#[tokio::test]
async fn registry_completeness_smoke_datetime_hour_via_date_part() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT date_part('hour', '2024-01-15T13:30:00Z')")
        .await
        .expect("date_part('hour', ...) must succeed now that datetime.rs registers it");
    assert_eq!(rows, vec!["13".to_string()]);
}

#[tokio::test]
async fn registry_completeness_smoke_conditional_greatest() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT greatest(1, 5, 3)")
        .await
        .expect("greatest(1, 5, 3) must succeed now that misc.rs registers it");
    assert_eq!(rows, vec!["5".to_string()]);
}

#[tokio::test]
async fn registry_completeness_smoke_array_append() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT array_append(ARRAY[1, 2], 3)")
        .await
        .expect("array_append(...) must succeed now that array_elem.rs registers it");
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains('3'),
        "expected the appended element 3 in the result array, got: {}",
        rows[0]
    );
}

#[tokio::test]
async fn registry_completeness_smoke_id_gen_random_uuid() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT gen_random_uuid()")
        .await
        .expect("gen_random_uuid() must succeed now that id_fn.rs registers it");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].len(),
        36,
        "expected a canonical 36-char UUID string, got: {}",
        rows[0]
    );
}

#[tokio::test]
async fn registry_completeness_smoke_types_typeof() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT type_of(42)")
        .await
        .expect("type_of(42) must succeed now that misc.rs registers it");
    assert_eq!(rows, vec!["int".to_string()]);
}
