// SPDX-License-Identifier: BUSL-1.1

//! Pgwire coverage for math-function domain faults. Each fails the statement
//! with PostgreSQL's SQLSTATE: `2201E` (invalid_argument_for_logarithm) for
//! `ln` or `log` of zero or a negative number, and `2201F`
//! (invalid_argument_for_power_function) for the square root of a negative
//! number and for `power` outside its domain.
//!
//! A from-less `SELECT` folds at plan time. A call over a column runs in the
//! row-scope evaluator on the Data Plane. Both paths are covered.

use crate::harness::TestServer;

const LOGARITHM: &str = "SQLSTATE 2201E";
const POWER_FUNCTION: &str = "SQLSTATE 2201F";

async fn seed(srv: &TestServer, collection: &str) {
    srv.exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, zero, neg) VALUES ('a', 0, -1)"
    ))
    .await
    .unwrap();
}

/// `SELECT sqrt(-1)` folds at plan time and raises `2201F`.
#[tokio::test]
async fn constant_sqrt_of_negative_errors_2201f() {
    let srv = TestServer::start().await;
    srv.expect_error("SELECT sqrt(-1)", POWER_FUNCTION).await;
}

/// `SELECT ln(0)` folds at plan time and raises `2201E`.
#[tokio::test]
async fn constant_ln_of_zero_errors_2201e() {
    let srv = TestServer::start().await;
    srv.expect_error("SELECT ln(0)", LOGARITHM).await;
}

/// `ln` of a negative number and `power` outside its domain keep their
/// subclasses on the constant path.
#[tokio::test]
async fn constant_log_and_power_faults_keep_their_subclass() {
    let srv = TestServer::start().await;
    srv.expect_error("SELECT ln(-1)", LOGARITHM).await;
    srv.expect_error("SELECT power(0, -1)", POWER_FUNCTION)
        .await;
    srv.expect_error("SELECT power(-8, 0.5)", POWER_FUNCTION)
        .await;
}

/// `sqrt` over a negative column value raises `2201F` from the row-scope
/// evaluator.
#[tokio::test]
async fn row_sqrt_of_negative_errors_2201f() {
    let srv = TestServer::start().await;
    seed(&srv, "math_domain_sqrt").await;
    srv.expect_error(
        "SELECT sqrt(neg) FROM math_domain_sqrt WHERE id = 'a'",
        POWER_FUNCTION,
    )
    .await;
}

/// `ln` over a zero column value raises `2201E` from the row-scope
/// evaluator.
#[tokio::test]
async fn row_ln_of_zero_errors_2201e() {
    let srv = TestServer::start().await;
    seed(&srv, "math_domain_ln").await;
    srv.expect_error(
        "SELECT ln(zero) FROM math_domain_ln WHERE id = 'a'",
        LOGARITHM,
    )
    .await;
}

/// Control: arguments inside the domain still compute.
#[tokio::test]
async fn in_domain_arguments_still_succeed() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT sqrt(4)")
        .await
        .expect("sqrt(4) must succeed");
    assert_eq!(rows, vec!["2".to_string()]);
}
