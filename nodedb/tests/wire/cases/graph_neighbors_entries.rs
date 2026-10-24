// SPDX-License-Identifier: BUSL-1.1

//! `GRAPH NEIGHBORS ... DIRECTION both` answers each physical edge once.
//!
//! A self-loop is one edge and sits in both the outbound and the inbound list
//! of its node. Two distinct edges between one pair of nodes stay two entries.

use crate::harness::TestServer;

async fn entries(server: &TestServer, sql: &str) -> Vec<(String, String)> {
    let rows = server.query_text(sql).await.unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&rows.join("")).unwrap();
    let mut out: Vec<(String, String)> = parsed
        .iter()
        .map(|e| {
            (
                e["label"].as_str().unwrap().to_owned(),
                e["node"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    out.sort();
    out
}

fn pair(label: &str, node: &str) -> (String, String) {
    (label.to_owned(), node.to_owned())
}

async fn seed(server: &TestServer, collection: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    for (src, dst) in [("a", "b"), ("a", "a"), ("c", "d"), ("d", "c")] {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN '{collection}' FROM '{src}' TO '{dst}' TYPE 'l'"
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_directions_list_a_self_loop_once() {
    let server = TestServer::start().await;
    seed(&server, "nbr_loop").await;

    let both = entries(
        &server,
        "GRAPH NEIGHBORS IN 'nbr_loop' OF 'a' DIRECTION both",
    )
    .await;
    assert_eq!(both, vec![pair("l", "a"), pair("l", "b")]);

    let out = entries(
        &server,
        "GRAPH NEIGHBORS IN 'nbr_loop' OF 'a' DIRECTION out",
    )
    .await;
    assert_eq!(out, vec![pair("l", "a"), pair("l", "b")]);

    let inbound = entries(&server, "GRAPH NEIGHBORS IN 'nbr_loop' OF 'a' DIRECTION in").await;
    assert_eq!(inbound, vec![pair("l", "a")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_edges_between_one_pair_stay_two_entries() {
    let server = TestServer::start().await;
    seed(&server, "nbr_pair").await;

    let both = entries(
        &server,
        "GRAPH NEIGHBORS IN 'nbr_pair' OF 'c' DIRECTION both",
    )
    .await;
    assert_eq!(both, vec![pair("l", "d"), pair("l", "d")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_self_loop_is_listed_once_inside_a_transaction() {
    let server = TestServer::start().await;
    seed(&server, "nbr_txn").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'nbr_txn' FROM 'x' TO 'x' TYPE 'l'")
        .await
        .unwrap();
    let both = entries(
        &server,
        "GRAPH NEIGHBORS IN 'nbr_txn' OF 'x' DIRECTION both",
    )
    .await;
    server.exec("ROLLBACK").await.unwrap();

    assert_eq!(both, vec![pair("l", "x")]);
}
