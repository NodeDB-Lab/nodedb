// SPDX-License-Identifier: BUSL-1.1

//! Pgwire coverage for division-by-zero handling: division and modulo by a
//! zero divisor must fail the statement at RUNTIME with SQLSTATE `22012`
//! (`division_by_zero`), not silently evaluate to a NULL row — the pre-fix
//! behavior. `nodedb-query/src/expr/binary.rs`'s `eval_binary_op` folded a
//! zero-divisor `/` or `%` to `Value::Null` for both the f64 and Decimal
//! arithmetic paths; `nodedb-query/src/functions/math.rs`'s `mod` scalar
//! function did the same. This is a runtime concern, distinct from unit 1's
//! plan-time `UndefinedFunction` gate: `1/0` is a perfectly well-typed,
//! well-formed expression — the error can only be known once the divisor
//! value is evaluated.
//!
//! ## Two paths, covered separately
//!
//! Tests with `FROM <collection>` and a column divisor exercise the row-scope
//! evaluator: a column reference never folds, so the expression reaches it.
//!
//! A bare `SELECT <expr>` with no `FROM` never reaches that evaluator. The
//! planner turns it into a plan-time `SqlPlan::ConstantResult` computed by
//! `eval_constant_expr`, which is
//! `const_fold::fold_constant(expr, ..).unwrap_or(SqlValue::Null)`. That
//! `None` means both "not a constant, defer to runtime" and "constant, and it
//! errored", and a from-less SELECT has no runtime to defer to — so both
//! become NULL. Integer division and modulo have no fold arm at all, so even
//! a well-defined `6/3` lands there. The constant-fold tests below pin that
//! path; `sql_arithmetic_overflow.rs` still hedges on the overflow case,
//! which is the same swallow.
//!
//! A column reference is never constant-foldable (`fold_constant` has no
//! arm for `SqlExpr::Column`), so `FROM <collection>` with the divisor read
//! from a column guarantees the expression reaches the real row-scope
//! evaluator these tests are meant to exercise — the same path a real
//! `SELECT ... FROM t WHERE <predicate-referencing-a-column>` query takes.
//!
//! ## Why the float-path test divides an integer literal by a float column
//!
//! A decimal-point numeric literal (e.g. `1.5`) combined arithmetically with
//! a stored `FLOAT` column is, independently of this fix, always NULL in
//! this codebase — confirmed by direct diagnosis: `SELECT 4.0 + fdenom`
//! (fdenom = 2.0, a perfectly valid addition) already returns NULL with no
//! error on an unmodified evaluator, while `SELECT 4 + fdenom` (integer
//! literal) correctly returns `6`. That is a pre-existing, general
//! decimal-literal/float-column type-coercion gap, unrelated to division or
//! to zero divisors (it reproduces with `+`), and out of scope for a
//! division/modulo-by-zero-only fix. `2/fdenom` (integer literal ÷ float
//! column) sidesteps it while still exercising the f64 arithmetic branch of
//! `eval_binary_op` (neither operand is `Value::Decimal`, so the Decimal
//! branch is provably not what's under test here).

use crate::harness::TestServer;

async fn seed(srv: &TestServer, collection: &str) {
    srv.exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom, fdenom) VALUES ('a', 1, 1.0)"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom, fdenom) VALUES ('b', 0, 0.0)"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom, fdenom) VALUES ('c', 2, 2.0)"
    ))
    .await
    .unwrap();
}

/// Integer division by a zero-valued column.
#[tokio::test]
async fn integer_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_int_216").await;

    srv.expect_error(
        "SELECT 1/denom FROM divzero_int_216 WHERE id = 'b'",
        "22012",
    )
    .await;
}

/// Float division by a zero-valued column — the f64 arithmetic path in
/// `eval_binary_op`, distinct from the Decimal/Integer path (see the module
/// doc comment for why the dividend is an integer literal, not `1.5`).
#[tokio::test]
async fn float_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_float_216").await;

    srv.expect_error(
        "SELECT 2/fdenom FROM divzero_float_216 WHERE id = 'b'",
        "22012",
    )
    .await;

    // Control: the same expression over a non-zero float column still
    // succeeds with the correct value.
    let rows = srv
        .query_text("SELECT 2/fdenom FROM divzero_float_216 WHERE id = 'c'")
        .await
        .expect("2 / 2.0 must still succeed");
    assert_eq!(rows, vec!["1".to_string()]);
}

/// Modulo by a zero-valued column.
#[tokio::test]
async fn modulo_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_mod_216").await;

    srv.expect_error(
        "SELECT 5 % denom FROM divzero_mod_216 WHERE id = 'b'",
        "22012",
    )
    .await;
}

/// The WHERE-clause behavior (Postgres-correct): a WHERE
/// predicate that divides by a column whose
/// value is zero for some row must fail the whole statement with `22012`,
/// exactly like evaluating the same expression in the SELECT list would —
/// not fold that row's predicate to `Value::Null`/false and silently exclude
/// it from the result, which would make a division-by-zero indistinguishable
/// from a legitimate non-match.
#[tokio::test]
async fn where_clause_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_where_216").await;

    srv.expect_error(
        "SELECT * FROM divzero_where_216 WHERE 10 / denom > 1",
        "22012",
    )
    .await;
}

/// An aggregate over an erroring predicate must fail, not report zero rows.
/// `COUNT(*)` returns exactly one row for any predicate that evaluates, so
/// an empty result set is indistinguishable from "nothing matched" and hides
/// the error behind a success tag.
#[tokio::test]
async fn count_over_erroring_predicate_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_count_where").await;

    srv.expect_error(
        "SELECT COUNT(*) FROM divzero_count_where WHERE 10 / denom = 10",
        "22012",
    )
    .await;
}

// ── Constant-folded arithmetic (no FROM clause) ─────────────────────
//
// A from-less SELECT is planned as a constant result, so these never reach
// the row-scope evaluator. `fold_constant` returns `Option`, and its `None`
// means both "not a constant, defer to runtime" and "constant, and it
// errored" — the caller renders both as NULL. There is no runtime to defer
// to here, so the error becomes a NULL row and the statement reports success.

/// `SELECT 1/0` must raise, not return a NULL row.
#[tokio::test]
async fn constant_integer_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;

    srv.expect_error("SELECT 1/0", "22012").await;
}

/// `SELECT 1%0` must raise. Modulo has its own runtime zero guard, which the
/// constant path does not reach.
#[tokio::test]
async fn constant_modulo_by_zero_errors_22012() {
    let srv = TestServer::start().await;

    srv.expect_error("SELECT 1%0", "22012").await;
}

/// `SELECT 1.0/0` must raise on the decimal path too.
#[tokio::test]
async fn constant_decimal_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;

    srv.expect_error("SELECT 1.0/0", "22012").await;
}

/// Guards the specific failure mode: whatever else changes, a constant
/// division by zero must never come back as a NULL row with a success tag.
#[tokio::test]
async fn constant_division_by_zero_is_never_a_null_row() {
    let srv = TestServer::start().await;

    if let Ok(rows) = srv.query_text("SELECT 1/0").await {
        panic!("SELECT 1/0 must not succeed; returned {rows:?}");
    }
}

/// A well-defined constant division must produce its value. The folder has
/// no integer-division arm, so it returns "not foldable" and the caller
/// renders that as NULL — a correct expression answered with NULL, which is
/// the same swallow as the zero-divisor case and not limited to it.
#[tokio::test]
async fn valid_constant_division_still_folds() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_text("SELECT 6/3")
        .await
        .expect("a well-defined constant division must still fold");
    assert_eq!(rows.first().map(String::as_str), Some("2"));
}

/// The same hole for modulo, which has no fold arm for any operand pair.
#[tokio::test]
async fn valid_constant_modulo_still_folds() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_text("SELECT 7%3")
        .await
        .expect("a well-defined constant modulo must still fold");
    assert_eq!(rows.first().map(String::as_str), Some("1"));
}

/// CASE laziness end-to-end: an untaken CASE branch containing a
/// division-by-zero expression must never be evaluated, so the statement
/// succeeds and returns the ELSE value. Uses a column reference (`denom`)
/// so the CASE expression is not plan-time constant-folded away — see the
/// module doc comment; `SqlExpr::Case` is never constant-foldable anyway,
/// so this exercises the row-scope evaluator's own CASE laziness
/// (`nodedb-query/src/expr/eval.rs`), which is exactly what's under test.
#[tokio::test]
async fn case_laziness_untaken_branch_succeeds() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_case_216").await;

    let rows = srv
        .query_text(
            "SELECT CASE WHEN false THEN 1/denom ELSE 42 END \
             FROM divzero_case_216 WHERE id = 'b'",
        )
        .await
        .expect("untaken CASE branch with 1/denom must not raise an error");
    assert_eq!(rows, vec!["42".to_string()]);
}

/// Control: a valid (non-zero-divisor) division still succeeds and returns
/// the correct value — the fix must not turn division itself into an error.
#[tokio::test]
async fn valid_division_still_succeeds() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_valid_216").await;

    let rows = srv
        .query_text("SELECT 10/denom FROM divzero_valid_216 WHERE id = 'c'")
        .await
        .expect("10 / 2 must still succeed");
    assert_eq!(rows, vec!["5".to_string()]);
}

// ── Columnar engine coverage (adversarial-review follow-up) ───────────────────
//
// The Document-engine tests above exercise `nodedb-query`'s row-scope
// evaluator directly. The columnar engine has its own scan handler
// (`nodedb/src/data/executor/handlers/columnar_read/scan.rs`) with its own
// error-plumbing from that same evaluator back out to a pgwire response —
// before this fix, four blocks there wrapped a division/modulo-by-zero
// `EvalError`/`crate::Error` as `ErrorCode::Internal { detail: e.to_string() }`
// instead of propagating the typed error, so a columnar query hit generic
// `XX000` instead of the correct `22012`. These tests prove the columnar
// path end-to-end, independent of the Document-engine coverage above.

async fn seed_columnar(srv: &TestServer, collection: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, denom INT) WITH (engine='columnar')"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom) VALUES ('a', 1)"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom) VALUES ('b', 0)"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom) VALUES ('c', 2)"
    ))
    .await
    .unwrap();
}

/// Columnar engine, WHERE-clause division by zero: `row_matches_filters`'s
/// `Err` arm in `execute_columnar_scan`'s live-memtable phase.
#[tokio::test]
async fn columnar_where_clause_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed_columnar(&srv, "cdivzero_where_216").await;

    srv.expect_error(
        "SELECT * FROM cdivzero_where_216 WHERE 10 / denom > 1",
        "22012",
    )
    .await;
}

/// Columnar engine, computed SELECT column division by zero:
/// `row_to_projected_value`'s `Err` arm in `execute_columnar_scan`'s
/// live-memtable phase (a real, non-empty `computed_cols`).
///
/// `denom` must be listed explicitly alongside the computed `ratio` column
/// (not just referenced inside the expression): `row_to_projected_value`
/// only includes a stored column in the row object it hands to the computed
/// expression evaluator when that column is itself in the projection list
/// (or force-included) — an explicit, non-computed `SELECT` entry for every
/// column a computed expression reads from is how a real client query would
/// request the same shape.
#[tokio::test]
async fn columnar_computed_column_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed_columnar(&srv, "cdivzero_select_216").await;

    srv.expect_error(
        "SELECT id, denom, 10/denom AS ratio FROM cdivzero_select_216 WHERE id = 'b'",
        "22012",
    )
    .await;
}

/// Control: a valid (non-zero-divisor) columnar computed column still
/// succeeds — the fix must not turn columnar division itself into an error.
#[tokio::test]
async fn columnar_computed_column_valid_division_still_succeeds() {
    let srv = TestServer::start().await;
    seed_columnar(&srv, "cdivzero_valid_216").await;

    let rows = srv
        .query_named_rows(
            "SELECT id, denom, 10/denom AS ratio FROM cdivzero_valid_216 WHERE id = 'c'",
        )
        .await
        .expect("10 / 2 must still succeed on the columnar engine");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("ratio").map(String::as_str), Some("5"));
}
