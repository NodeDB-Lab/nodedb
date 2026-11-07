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
/// A second edge-bearing collection for the request of two TRUNCATEs.
const OTHER: &str = "ptv_other";

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

async fn add_edge(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{collection}' FROM 'e0' TO 'e1' TYPE 'l'"
        ))
        .await
        .expect("write one edge into the collection");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_of_a_cross_shard_sum_source_zeroes_the_balance() {
    let server = sum_fixture().await;
    // The source holds one edge, so its TRUNCATE takes the edge route.
    add_edge(&server, POSTINGS).await;
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

/// pgwire sends every TRUNCATE that spans vShards to the recon route, edge-free
/// or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_truncate_of_an_edge_free_source_zeroes_the_balance() {
    let server = sum_fixture().await;

    server
        .exec(&format!("TRUNCATE {POSTINGS}"))
        .await
        .expect("truncate the source");

    assert_eq!(source_sum(&server).await, 0, "the source is empty");
    assert_eq!(
        balance(&server, "acc").await,
        "0",
        "the balance must equal the remaining source sum"
    );
}

/// One request of two TRUNCATEs on the edge route empties both collections and
/// keeps the balance equal to the source sum.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_of_two_truncates_runs_both() {
    let server = sum_fixture().await;
    server
        .exec(&format!(
            "CREATE COLLECTION {OTHER} (id TEXT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the second collection");
    server
        .exec(&format!("INSERT INTO {OTHER} (id, v) VALUES ('r1', 'v')"))
        .await
        .expect("seed the second collection");
    add_edge(&server, POSTINGS).await;
    add_edge(&server, OTHER).await;

    server
        .http_exec(&format!("TRUNCATE {POSTINGS}; TRUNCATE {OTHER}"))
        .await;

    assert_eq!(source_sum(&server).await, 0, "the source is empty");
    assert_eq!(balance(&server, "acc").await, "0", "the balance follows");
    let rows = server
        .query_text(&format!("SELECT v FROM {OTHER}"))
        .await
        .expect("read the second collection");
    assert!(rows.is_empty(), "the second TRUNCATE ran: {rows:?}");
}
