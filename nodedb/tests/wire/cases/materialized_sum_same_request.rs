// SPDX-License-Identifier: BUSL-1.1

//! Two predicate writes of one materialized-sum source in one HTTP request.
//!
//! The request plans as one task set, and each write settles its balance
//! delta from the committed rows, so the second cannot see the first. The
//! request is refused whole.

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};
use crate::harness::TestServer;
use nodedb::control::planner::calvin::settling_writes::SEVERAL_WRITES_REFUSAL;

/// The sum of the `amount` column of the postings of `acc`.
async fn posted_sum(server: &TestServer) -> i64 {
    let rows = server
        .query_text("SELECT amount FROM ptv_post WHERE account_id = 'acc'")
        .await
        .unwrap_or_else(|e| panic!("read the postings: {e}"));
    rows.iter()
        .map(|amount| {
            amount
                .parse::<i64>()
                .unwrap_or_else(|e| panic!("amount {amount:?}: {e}"))
        })
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_settling_updates_in_one_request_are_refused_and_change_nothing() {
    let server = sum_fixture().await;
    refused_whole(
        &server,
        "UPDATE ptv_post SET amount = '1' WHERE account_id = 'acc'; \
         UPDATE ptv_post SET amount = '50' WHERE id = 'e0'",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_predicate_updates_of_one_balance_are_refused_and_change_nothing() {
    let server = sum_fixture().await;
    refused_whole(
        &server,
        "UPDATE ptv_post SET amount = '1' WHERE account_id = 'acc'; \
         UPDATE ptv_post SET amount = '50' WHERE account_id = 'acc'",
    )
    .await;
}

async fn refused_whole(server: &TestServer, sql: &str) {
    let (status, body) = server.http_query(sql).await;

    // The request is refused whole, so no write lands and the balance still
    // equals the sum of the postings.
    assert!(
        !(200..300).contains(&status),
        "the request must be refused: {body}"
    );
    assert_eq!(status, 400, "a refused request is a 400: {body}");
    assert!(
        body.contains(SEVERAL_WRITES_REFUSAL),
        "refusal text: {body}"
    );
    assert_eq!(posted_sum(server).await, 15);
    assert_eq!(balance(server, "acc").await, "15");
}
