//! Integration coverage for integer overflow handling in SQL expressions.
//!
//! The const-folder and procedural executor must detect integer overflow
//! and return an error rather than panicking (debug) or silently wrapping
//! (release). Float divide-by-zero must return an error, not ±Inf.

mod common;

use common::pgwire_harness::TestServer;

// ---------------------------------------------------------------------------
// Const-folder overflow (nodedb-sql planner)
// ---------------------------------------------------------------------------

/// `i64::MAX + 1` in a constant expression must not panic (debug) or wrap
/// to a negative number (release). An error or null are both acceptable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn const_fold_addition_overflow_does_not_wrap() {
    let server = TestServer::start().await;

    let result = server.query_text("SELECT 9223372036854775807 + 1").await;

    match result {
        Err(_) => { /* error is acceptable */ }
        Ok(rows) => {
            if let Some(val) = rows.first() {
                assert!(
                    !val.contains("-9223372036854775808"),
                    "i64::MAX + 1 must not silently wrap to i64::MIN: got {val}"
                );
            }
        }
    }
}

/// `i64::MAX * 2` must not panic or wrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn const_fold_multiplication_overflow_does_not_wrap() {
    let server = TestServer::start().await;

    let result = server.query_text("SELECT 9223372036854775807 * 2").await;

    match result {
        Err(_) => { /* error is acceptable */ }
        Ok(rows) => {
            if let Some(val) = rows.first() {
                // Wrapped value would be -2.
                assert!(
                    !val.contains("\"-2\""),
                    "i64::MAX * 2 must not silently wrap: got {val}"
                );
            }
        }
    }
}

/// `i64::MIN - 1` must not panic or wrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn const_fold_subtraction_overflow_does_not_wrap() {
    let server = TestServer::start().await;

    let result = server.query_text("SELECT -9223372036854775808 - 1").await;

    match result {
        Err(_) => { /* error is acceptable */ }
        Ok(rows) => {
            if let Some(val) = rows.first() {
                // Wrapped value would be i64::MAX = 9223372036854775807.
                assert!(
                    !val.contains("9223372036854775807"),
                    "i64::MIN - 1 must not silently wrap to i64::MAX: got {val}"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Const-folder in INSERT context
// ---------------------------------------------------------------------------

/// Overflow in an INSERT VALUES expression should be caught before storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn const_fold_overflow_in_insert_values() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION overflow_tbl TYPE DOCUMENT STRICT (\
                id TEXT PRIMARY KEY, \
                v BIGINT)",
        )
        .await
        .unwrap();

    let result = server
        .exec("INSERT INTO overflow_tbl (id, v) VALUES ('k', 9223372036854775807 * 2)")
        .await;

    assert!(
        result.is_err(),
        "INSERT with overflowing constant should fail, not store wrapped value"
    );
}

// ---------------------------------------------------------------------------
// Procedural executor overflow (triggers / DO blocks)
// ---------------------------------------------------------------------------

/// Integer overflow in a DO block's variable arithmetic should error, not
/// panic or silently wrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn procedural_eval_integer_overflow_returns_error() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION proc_ov TYPE DOCUMENT STRICT (id TEXT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    // A DO block (or procedure) that overflows during evaluation.
    let result = server
        .exec(
            "DO $$ \
             DECLARE x BIGINT := 9223372036854775807; \
             BEGIN \
               x := x + 1; \
               INSERT INTO proc_ov (id, v) VALUES ('k', x); \
             END $$",
        )
        .await;

    assert!(
        result.is_err(),
        "integer overflow in procedural block should error, not wrap"
    );
}

/// `i64::MIN / -1` is undefined behavior in two's complement and must error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn procedural_eval_min_div_neg1_returns_error() {
    let server = TestServer::start().await;

    let result = server
        .exec(
            "DO $$ \
             DECLARE x BIGINT := -9223372036854775808; \
             DECLARE y BIGINT; \
             BEGIN \
               y := x / -1; \
             END $$",
        )
        .await;

    assert!(
        result.is_err(),
        "i64::MIN / -1 in procedural block should error, not panic"
    );
}

/// Float divide by negative zero should return an error or NULL, not ±Inf.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn procedural_eval_float_div_neg_zero() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION fdiv TYPE DOCUMENT STRICT (id TEXT PRIMARY KEY, v FLOAT)")
        .await
        .unwrap();

    // -0.0 passes the `!= 0.0` guard in f64 comparison but produces -inf.
    let result = server
        .exec(
            "DO $$ \
             DECLARE a FLOAT := 1.0; \
             DECLARE b FLOAT := -0.0; \
             DECLARE c FLOAT; \
             BEGIN \
               c := a / b; \
               INSERT INTO fdiv (id, v) VALUES ('k', c); \
             END $$",
        )
        .await;

    // Either error, or if it succeeds, the stored value must not be infinity.
    if result.is_ok() {
        let rows = server
            .query_text("SELECT v FROM fdiv WHERE id = 'k'")
            .await
            .unwrap();
        if let Some(val) = rows.first() {
            assert!(
                !val.to_lowercase().contains("inf"),
                "float / -0.0 should not produce infinity: got {val}"
            );
        }
    }
}
