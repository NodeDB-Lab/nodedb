// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... ON CONFLICT DO NOTHING` on the source of a cross-shard
//! materialized sum.
//!
//! A new row adds its amount to the balance. A row whose key is stored is
//! skipped and leaves the balance unchanged. A page that repeats a key lands
//! the first row only. The balance equals the source sum after each statement.
//!
//! The cases cover HTTP, pgwire, a pgwire transaction block and native SQL.
//! The fixture starts with `e0` = 10 and `e1` = 5 on `acc`, balance 15.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};

use super::sum_cell_support::{balance, source_sum, sum_fixture};
use crate::harness::TestServer;

/// A single-row insert of the new key `e2` with amount 7.
const INSERT_NEW_E2: &str = "INSERT INTO ptv_post (id, account_id, amount) \
                             VALUES ('e2', 'acc', '7') ON CONFLICT DO NOTHING";

/// Run `sql` over HTTP and assert a 2xx answer.
async fn http_applies(server: &TestServer, sql: &str) {
    let (status, body) = server.http_query(sql).await;
    assert!(
        (200..300).contains(&status),
        "the request applies: {status} {body}"
    );
}

/// Assert the source sum and the balance of `acc` both equal `expected`.
async fn assert_balance_follows(server: &TestServer, expected: i64, what: &str) {
    assert_eq!(source_sum(server).await, expected, "source sum: {what}");
    assert_eq!(
        balance(server, "acc").await,
        expected.to_string(),
        "balance: {what}"
    );
}

/// A new key inserts, and the balance gains the row's amount. A balance
/// violation, or a balance short of the source sum, fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_row_insert_that_skips_nothing_adds_its_amount() {
    let server = sum_fixture().await;
    http_applies(&server, INSERT_NEW_E2).await;
    assert_balance_follows(&server, 22, "e0 10, e1 5, e2 7").await;
}

/// An existing key is skipped, and the balance stays. A credit for the
/// skipped row fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_row_insert_that_skips_a_conflict_leaves_the_balance() {
    let server = sum_fixture().await;
    http_applies(
        &server,
        "INSERT INTO ptv_post (id, account_id, amount) VALUES ('e1', 'acc', '7') \
         ON CONFLICT DO NOTHING",
    )
    .await;
    assert_balance_follows(&server, 15, "the skipped row changes nothing").await;
}

/// A page skips the stored `e0` and lands the new `e7`. Only `e7` adds. A
/// credit for `e0` fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_that_skips_one_row_adds_the_rows_it_lands() {
    let server = sum_fixture().await;
    http_applies(
        &server,
        "INSERT INTO ptv_post (id, account_id, amount) \
         VALUES ('e0', 'acc', '4'), ('e7', 'acc', '2') ON CONFLICT DO NOTHING",
    )
    .await;
    assert_balance_follows(&server, 17, "e0 stays 10, e7 adds 2").await;
}

/// A page lands the new `e7` and `e8` on one account. Both amounts add. A
/// balance that holds one credit only fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_of_two_new_rows_on_one_account_adds_both() {
    let server = sum_fixture().await;
    http_applies(
        &server,
        "INSERT INTO ptv_post (id, account_id, amount) \
         VALUES ('e7', 'acc', '2'), ('e8', 'acc', '3') ON CONFLICT DO NOTHING",
    )
    .await;
    assert_balance_follows(&server, 20, "e7 adds 2, e8 adds 3").await;
}

/// A page names the new `e7` twice. The first row lands and the second skips.
/// A credit for the repeat fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_that_repeats_a_new_key_adds_the_first_row_only() {
    let server = sum_fixture().await;
    http_applies(
        &server,
        "INSERT INTO ptv_post (id, account_id, amount) \
         VALUES ('e7', 'acc', '2'), ('e7', 'acc', '3') ON CONFLICT DO NOTHING",
    )
    .await;
    assert_balance_follows(&server, 17, "the first e7 adds 2, the repeat adds 0").await;
}

/// The single-row insert of a new key over pgwire adds its amount. A
/// balance violation, or a balance short of the source sum, fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pgwire_insert_that_skips_nothing_adds_its_amount() {
    let server = sum_fixture().await;
    server
        .exec(INSERT_NEW_E2)
        .await
        .unwrap_or_else(|e| panic!("the pgwire insert applies: {e}"));
    assert_balance_follows(&server, 22, "pgwire e2 adds 7").await;
}

/// The single-row insert of a new key inside a pgwire block adds its amount
/// at COMMIT. A COMMIT error, or a balance short of the source sum, fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_inside_a_block_that_skips_nothing_adds_its_amount() {
    let server = sum_fixture().await;
    for sql in ["BEGIN", INSERT_NEW_E2, "COMMIT"] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} applies: {e}"));
    }
    assert_balance_follows(&server, 22, "the block's e2 adds 7").await;
}

/// The single-row insert of a new key over native SQL adds its amount. A
/// balance violation, or a balance short of the source sum, fails it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_native_insert_that_skips_nothing_adds_its_amount() {
    let server = sum_fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let response = send_sql(&mut stream, 2, INSERT_NEW_E2).await;
    if let Some(error) = &response.error {
        panic!(
            "the native insert applies: {}: {}",
            error.code, error.message
        );
    }
    assert_balance_follows(&server, 22, "native e2 adds 7").await;
}
