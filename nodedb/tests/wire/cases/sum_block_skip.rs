// SPDX-License-Identifier: BUSL-1.1

//! A write that moves no balance, next to a write that moves one, on the
//! source of a cross-shard materialized sum in one transaction.
//!
//! The install folds every row of a source collection against one merged
//! table. So a join value that one op resolves and never ships tells a sibling
//! row to fold that account on the source's core. That core holds no target
//! row. The committed record then refuses at install, and Calvin intake halts.
//!
//! The fixture starts with `e0` = 10 and `e1` = 5 on `acc`, balance 15. Each
//! cell commits, checks the balance against the source sum, then sends a later
//! write. A `57014` on COMMIT, a balance apart from the source, or a later
//! write that times out fails the cell.

use super::sum_cell_support::{balance, source_sum, sum_fixture};
use crate::harness::TestServer;

/// A stored key: the insert skips, and its body resolves `acc`.
const SKIP_E0: &str = "INSERT INTO ptv_post (id, account_id, amount) \
                       VALUES ('e0', 'acc', '99') ON CONFLICT (id) DO NOTHING";
const INSERT_E5: &str = "INSERT INTO ptv_post (id, account_id, amount) VALUES ('e5', 'acc', '4')";
const ZERO_INSERT_Z0: &str =
    "INSERT INTO ptv_post (id, account_id, amount) VALUES ('z0', 'acc', '0')";

/// Run each statement in one `BEGIN .. COMMIT` block.
async fn commit_block(server: &TestServer, statements: &[&str]) {
    let block = ["BEGIN"].iter().chain(statements).chain(["COMMIT"].iter());
    for sql in block {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} applies: {e}"));
    }
}

/// Assert source sum and balance of `acc` equal `expected`, and that a later
/// write settles. A halted Calvin intake answers that write with `57014`.
async fn assert_settled_and_open(server: &TestServer, expected: i64) {
    assert_eq!(source_sum(server).await, expected, "source sum");
    assert_eq!(
        balance(server, "acc").await,
        expected.to_string(),
        "balance"
    );
    server
        .exec("INSERT INTO ptv_post (id, account_id, amount) VALUES ('later', 'acc', '1')")
        .await
        .unwrap_or_else(|e| panic!("a later write applies: {e}"));
    assert_eq!(source_sum(server).await, expected + 1, "source after");
    assert_eq!(
        balance(server, "acc").await,
        (expected + 1).to_string(),
        "balance after"
    );
}

/// The skip leaves `acc` resolved, and the sibling insert folds it on the
/// source's core.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_skip_beside_an_insert_commits_and_settles() {
    let server = sum_fixture().await;
    commit_block(&server, &[SKIP_E0, INSERT_E5]).await;
    assert_settled_and_open(&server, 19).await;
}

/// The same leftover with an upsert as the sibling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_skip_beside_an_upsert_commits_and_settles() {
    let server = sum_fixture().await;
    commit_block(
        &server,
        &[
            SKIP_E0,
            "UPSERT INTO ptv_post (id, account_id, amount) VALUES ('e6', 'acc', '1')",
        ],
    )
    .await;
    assert_settled_and_open(&server, 16).await;
}

/// The same leftover with an update as the sibling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_skip_beside_an_update_commits_and_settles() {
    let server = sum_fixture().await;
    commit_block(
        &server,
        &[SKIP_E0, "UPDATE ptv_post SET amount = '6' WHERE id = 'e1'"],
    )
    .await;
    assert_settled_and_open(&server, 16).await;
}

/// The same leftover with a delete as the sibling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_skip_beside_a_delete_commits_and_settles() {
    let server = sum_fixture().await;
    commit_block(&server, &[SKIP_E0, "DELETE FROM ptv_post WHERE id = 'e1'"]).await;
    assert_settled_and_open(&server, 10).await;
}

/// One simple query holds both statements, in an implicit transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_skip_beside_an_insert_in_one_request_commits_and_settles() {
    let server = sum_fixture().await;
    let sql = format!("{SKIP_E0}; {INSERT_E5}");
    server
        .client
        .simple_query(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} applies: {:?}", e.as_db_error()));
    assert_settled_and_open(&server, 19).await;
}

/// A conflict update keeps the stored join value `acc`, so the body's `acc2`
/// moves nothing and ships nothing. A sibling insert on `acc2` must not fold
/// it on the source's core.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflict_update_whose_body_names_another_account_settles() {
    let server = sum_fixture().await;
    commit_block(
        &server,
        &[
            "INSERT INTO ptv_post (id, account_id, amount) VALUES ('e0', 'acc2', '3') \
             ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount",
            "INSERT INTO ptv_post (id, account_id, amount) VALUES ('e5', 'acc2', '4')",
        ],
    )
    .await;
    assert_eq!(balance(&server, "acc2").await, "4", "acc2 holds e5 only");
    assert_settled_and_open(&server, 8).await;
}

/// A plain insert of amount zero moves no balance and ships no task, so its
/// resolved `acc` stays undeferred. A delete beside it must not fold `acc` on
/// the source's core.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zero_insert_beside_a_delete_commits_and_settles() {
    let server = sum_fixture().await;
    commit_block(
        &server,
        &[ZERO_INSERT_Z0, "DELETE FROM ptv_post WHERE id = 'e1'"],
    )
    .await;
    assert_settled_and_open(&server, 10).await;
}

/// A page whose rows cancel has a zero net delta for `acc`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelling_page_beside_a_delete_commits_and_settles() {
    let server = sum_fixture().await;
    commit_block(
        &server,
        &[
            "INSERT INTO ptv_post (id, account_id, amount) \
             VALUES ('z0', 'acc', '3'), ('z1', 'acc', '-3')",
            "DELETE FROM ptv_post WHERE id = 'e1'",
        ],
    )
    .await;
    assert_settled_and_open(&server, 10).await;
}

/// One simple query holds the zero insert and the delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zero_insert_beside_a_delete_in_one_request_commits_and_settles() {
    let server = sum_fixture().await;
    let sql = format!("{ZERO_INSERT_Z0}; DELETE FROM ptv_post WHERE id = 'e1'");
    server
        .client
        .simple_query(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} applies: {:?}", e.as_db_error()));
    assert_settled_and_open(&server, 10).await;
}
