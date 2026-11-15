// SPDX-License-Identifier: BUSL-1.1

//! Writes to the source of a cross-shard materialized sum whose value column is
//! generated.
//!
//! The Data Plane recomputes `amount` from `price` on every stored row. The
//! balance must follow the recomputed `amount`, so it equals the source sum.
//!
//! The fixture inserts `e0` price 5 (amount 10) and `e1` price 2 (amount 4)
//! on `acc`, balance 14.

use crate::harness::TestServer;

/// The sum's target collection. The pair hashes to different vShards and
/// cores, as in `materialized_sum_cross_shard_expanders`.
const ACCOUNTS: &str = "ptv_acct";
/// The sum's source collection.
const POSTINGS: &str = "ptv_post";
/// Data Plane cores the server runs.
const CORES: usize = 2;

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

async fn generated_fixture() -> TestServer {
    fixture_with(
        "price FLOAT",
        "amount FLOAT GENERATED ALWAYS AS (price * 2)",
        "balance FLOAT",
    )
    .await
}

/// A fixture whose source holds `price_col` and the generated `amount_col`,
/// and whose target holds the sum column `balance_col`. Seeds `e0` price 5
/// and `e1` price 2 on `acc`.
async fn fixture_with(price_col: &str, amount_col: &str, balance_col: &str) -> TestServer {
    let server = TestServer::start_multicores(CORES).await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, account_id TEXT, {price_col}, \
             {amount_col}) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &server,
        &format!(
            "ALTER COLLECTION {ACCOUNTS} ADD COLUMN {balance_col} \
             MATERIALIZED_SUM SOURCE {POSTINGS} \
             ON {POSTINGS}.account_id = {ACCOUNTS}.id VALUE {POSTINGS}.amount"
        ),
    )
    .await;
    exec(
        &server,
        &format!("INSERT INTO {ACCOUNTS} (id, owner, balance) VALUES ('acc', 'alice', 0)"),
    )
    .await;
    for (id, price) in [("e0", 5), ("e1", 2)] {
        exec(
            &server,
            &format!(
                "INSERT INTO {POSTINGS} (id, account_id, price) VALUES ('{id}', 'acc', {price})"
            ),
        )
        .await;
    }
    server
}

async fn number(server: &TestServer, sql: &str) -> f64 {
    let rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("read {sql}: {e}"));
    rows.iter()
        .map(|value| value.parse::<f64>().expect("a numeric value"))
        .sum()
}

async fn balance(server: &TestServer) -> f64 {
    number(
        server,
        &format!("SELECT balance FROM {ACCOUNTS} WHERE id = 'acc'"),
    )
    .await
}

async fn source_sum(server: &TestServer) -> f64 {
    number(
        server,
        &format!("SELECT amount FROM {POSTINGS} WHERE account_id = 'acc'"),
    )
    .await
}

/// An `INSERT` credits the generated `amount`, not the absent submitted one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_credits_the_generated_sum_column() {
    let server = generated_fixture().await;

    let sum = source_sum(&server).await;
    assert_eq!(sum, 14.0, "e0 amount 10 plus e1 amount 4");
    assert_eq!(
        balance(&server).await,
        sum,
        "the balance must equal the source sum"
    );
}

/// Bring the balance in line with the seeded rows, so an update cell starts
/// from a correct total whatever the insert path credited.
async fn reconcile(server: &TestServer) {
    let sum = source_sum(server).await;
    exec(
        server,
        &format!("UPDATE {ACCOUNTS} SET balance = {sum} WHERE id = 'acc'"),
    )
    .await;
    assert_eq!(balance(server).await, 14.0, "reconciled balance");
}

/// A point `UPDATE` of `price` moves the generated `amount`, and the balance
/// follows it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_point_update_of_a_generated_sum_dependency_moves_the_balance() {
    let server = generated_fixture().await;
    reconcile(&server).await;

    let outcome = server
        .exec(&format!("UPDATE {POSTINGS} SET price = 10 WHERE id = 'e0'"))
        .await;

    let sum = source_sum(&server).await;
    assert_eq!(
        balance(&server).await,
        sum,
        "the balance must equal the source sum (outcome {outcome:?})"
    );
    assert_eq!(sum, 24.0, "e0 amount 20 plus e1 amount 4");
}

/// A predicate `UPDATE` of `price` moves the generated `amount` of every
/// matched row, and the balance follows it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_update_of_a_generated_sum_dependency_moves_the_balance() {
    let server = generated_fixture().await;
    reconcile(&server).await;

    let outcome = server
        .exec(&format!(
            "UPDATE {POSTINGS} SET price = price + 1 WHERE account_id = 'acc'"
        ))
        .await;

    let sum = source_sum(&server).await;
    assert_eq!(
        balance(&server).await,
        sum,
        "the balance must equal the source sum (outcome {outcome:?})"
    );
    assert_eq!(sum, 18.0, "e0 amount 12 plus e1 amount 6");
}

/// A generated `DECIMAL(10,2)` column stores the value rounded to its scale.
/// The balance credits the stored value, so it equals the source sum.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_credits_the_rounded_decimal_generated_value() {
    let server = fixture_with(
        "price DECIMAL(10,2)",
        "amount DECIMAL(10,2) GENERATED ALWAYS AS (price / 3)",
        "balance DECIMAL",
    )
    .await;

    exec(
        &server,
        &format!("INSERT INTO {POSTINGS} (id, account_id, price) VALUES ('e2', 'acc', 10)"),
    )
    .await;

    let stored = number(
        &server,
        &format!("SELECT amount FROM {POSTINGS} WHERE id = 'e2'"),
    )
    .await;
    assert_eq!(stored, 3.33, "the stored amount is rounded to scale 2");
    let sum = source_sum(&server).await;
    assert_eq!(
        balance(&server).await,
        sum,
        "the balance must equal the source sum"
    );
}

/// An `UPDATE` that leaves the generated inputs alone keeps the stored
/// `DECIMAL(10,2)` generated value. The balance stays equal to the source sum.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unrelated_update_leaves_the_decimal_balance_unchanged() {
    let server = fixture_with(
        "price DECIMAL(10,2), note TEXT",
        "amount DECIMAL(10,2) GENERATED ALWAYS AS (price / 3)",
        "balance DECIMAL",
    )
    .await;
    let before = balance(&server).await;

    for round in 0..3 {
        exec(
            &server,
            &format!("UPDATE {POSTINGS} SET note = 'n{round}' WHERE id = 'e0'"),
        )
        .await;
    }

    let sum = source_sum(&server).await;
    let after = balance(&server).await;
    assert!(
        (after - sum).abs() < 1e-9,
        "the balance {after} must equal the source sum {sum}"
    );
    assert!(
        (after - before).abs() < 1e-9,
        "the balance {after} must equal the balance before the updates {before}"
    );
}
