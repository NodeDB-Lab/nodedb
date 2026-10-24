// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE of an edge-bearing source of a materialized sum whose target
//! lives on another vShard and core.
//!
//! An edge-bearing TRUNCATE runs through the recon route as one Calvin
//! transaction. The balance delta the sum settles for the removed rows must
//! reach the target, so the balance ends equal to the remaining source sum.

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};
use crate::harness::TestServer;

const POSTINGS: &str = "ptv_post";

/// The sum of the `acc` postings, read from the source rows.
async fn source_sum(server: &TestServer) -> i64 {
    server
        .query_text(&format!(
            "SELECT amount FROM {POSTINGS} WHERE account_id = 'acc'"
        ))
        .await
        .expect("read the source postings")
        .iter()
        .map(|amount| amount.parse::<i64>().expect("a numeric amount"))
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_of_a_cross_shard_sum_source_zeroes_the_balance() {
    let server = sum_fixture().await;
    // The source holds one edge, so its TRUNCATE takes the edge route.
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{POSTINGS}' FROM 'e0' TO 'e1' TYPE 'l'"
        ))
        .await
        .expect("write one edge into the source");
    assert_eq!(balance(&server, "acc").await, "15", "seed balance");

    server.http_exec(&format!("TRUNCATE {POSTINGS}")).await;

    assert_eq!(source_sum(&server).await, 0, "the source is empty");
    assert_eq!(
        balance(&server, "acc").await,
        "0",
        "the balance must equal the remaining source sum"
    );
}

/// Control: the same TRUNCATE without an edge zeroes the balance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_of_an_edge_free_cross_shard_sum_source_zeroes_the_balance() {
    let server = sum_fixture().await;

    server.http_exec(&format!("TRUNCATE {POSTINGS}")).await;

    assert_eq!(source_sum(&server).await, 0, "the source is empty");
    assert_eq!(
        balance(&server, "acc").await,
        "0",
        "the balance must equal the remaining source sum"
    );
}
