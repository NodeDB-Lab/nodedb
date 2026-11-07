// SPDX-License-Identifier: BUSL-1.1

//! One predicate write that moves two sum columns of one cross-shard target row.
//!
//! Each column is its own balance, so the write ships two deltas to the same
//! row. The several-writes guard counts balances by column, so the write runs
//! and both columns end equal to their source sums.

use crate::harness::TestServer;

/// The sum's target collection, as in `materialized_sum_cross_shard_expanders`.
/// The pair hashes to different vShards and cores there.
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

/// Accounts `acc` holds `balance` and `fees`, each summed from a postings
/// column. Posting `e0` is 10 and 1, `e1` is 5 and 2.
async fn two_column_fixture() -> TestServer {
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
            "CREATE COLLECTION {POSTINGS} \
             (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT, fee TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    for (column, value) in [("balance", "amount"), ("fees", "fee")] {
        exec(
            &server,
            &format!(
                "ALTER COLLECTION {ACCOUNTS} ADD COLUMN {column} TEXT \
                 MATERIALIZED_SUM SOURCE {POSTINGS} \
                 ON {POSTINGS}.account_id = {ACCOUNTS}.id VALUE {POSTINGS}.{value}"
            ),
        )
        .await;
    }
    exec(
        &server,
        &format!(
            "INSERT INTO {ACCOUNTS} (id, owner, balance, fees) \
             VALUES ('acc', 'alice', '0', '0')"
        ),
    )
    .await;
    for (id, amount, fee) in [("e0", "10", "1"), ("e1", "5", "2")] {
        exec(
            &server,
            &format!(
                "INSERT INTO {POSTINGS} (id, account_id, amount, fee) \
                 VALUES ('{id}', 'acc', '{amount}', '{fee}')"
            ),
        )
        .await;
    }
    server
}

async fn stored(server: &TestServer, column: &str) -> String {
    let rows = server
        .query_text(&format!("SELECT {column} FROM {ACCOUNTS} WHERE id = 'acc'"))
        .await
        .unwrap_or_else(|e| panic!("read {column}: {e}"));
    match rows.as_slice() {
        [value] => value.clone(),
        other => panic!("one {column} row, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_predicate_write_moves_two_sum_columns_of_one_row() {
    let server = two_column_fixture().await;
    assert_eq!(stored(&server, "balance").await, "15", "seed balance");
    assert_eq!(stored(&server, "fees").await, "3", "seed fees");

    server
        .http_exec(&format!(
            "UPDATE {POSTINGS} SET amount = '1', fee = '7' WHERE account_id = 'acc'"
        ))
        .await;

    assert_eq!(stored(&server, "balance").await, "2", "1 + 1");
    assert_eq!(stored(&server, "fees").await, "14", "7 + 7");
}

/// Two predicate writes of one request, each moving a different column of the
/// same target row. Each column settles from its own source column, so the
/// pair runs and both columns still equal their source sums.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_predicate_writes_moving_different_columns_of_one_row_both_apply() {
    let server = two_column_fixture().await;

    server
        .http_exec(&format!(
            "UPDATE {POSTINGS} SET amount = '1' WHERE account_id = 'acc'; \
             UPDATE {POSTINGS} SET fee = '7' WHERE account_id = 'acc'"
        ))
        .await;

    assert_eq!(stored(&server, "balance").await, "2", "1 + 1");
    assert_eq!(stored(&server, "fees").await, "14", "7 + 7");
}
