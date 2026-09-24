// SPDX-License-Identifier: BUSL-1.1

//! Wire coverage for the batched edge statements (`GRAPH INSERT EDGES` and
//! `GRAPH DELETE EDGES`), the front door added for batched edge writes.
//!
//! The plan, staging, WAL, and Data-Plane layers already carried
//! `EdgePutBatch` and `EdgeDeleteBatch`; these tests lock in the statement
//! layer above them:
//!
//! 1. One statement inserts many edges and the chain is fully traversable.
//! 2. One statement deletes many edges and none survive.
//! 3. A batch over the per-statement cap is rejected and the error names the
//!    cap, so a loader can size its next statement from the message.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_insert_edges_batch_round_trips() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION batch_edges").await.unwrap();

    // One statement, three edges, one response.
    server
        .exec(
            "GRAPH INSERT EDGES IN 'batch_edges' VALUES \
             ('a','b','L'), ('b','c','L'), ('c','d','L')",
        )
        .await
        .unwrap();

    // The chain is only traversable end to end when all three edges landed.
    let rows = server
        .query_text("GRAPH PATH IN 'batch_edges' FROM 'a' TO 'd' MAX_DEPTH 5 LABEL 'L'")
        .await
        .unwrap();
    let blob = rows.join("");
    assert!(blob.contains('b'), "path must traverse b: {blob}");
    assert!(blob.contains('c'), "path must traverse c: {blob}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_delete_edges_batch_removes_every_edge() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION batch_del").await.unwrap();
    server
        .exec("GRAPH INSERT EDGES IN 'batch_del' VALUES ('a','b','L'), ('b','c','L')")
        .await
        .unwrap();

    // Positive control: the path exists before the delete.
    let before = server
        .query_text("GRAPH PATH IN 'batch_del' FROM 'a' TO 'c' MAX_DEPTH 5 LABEL 'L'")
        .await
        .unwrap()
        .join("");
    assert!(before.contains('b'), "pre-delete path must exist: {before}");

    server
        .exec("GRAPH DELETE EDGES IN 'batch_del' VALUES ('a','b','L'), ('b','c','L')")
        .await
        .unwrap();

    // After the delete the traversal must stop finding the chain. A missing
    // path may surface as an empty result or as a read error; both are "not
    // found" for this contract, and neither may report the nodes.
    let after = match server
        .query_text("GRAPH PATH IN 'batch_del' FROM 'a' TO 'c' MAX_DEPTH 5 LABEL 'L'")
        .await
    {
        Ok(rows) => rows.join(""),
        Err(_) => String::new(),
    };
    assert!(
        !after.contains('b'),
        "deleted edges must not survive the batch delete: {after}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_over_the_cap_is_rejected_and_names_the_cap() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION batch_cap").await.unwrap();

    let mut sql = String::from("GRAPH INSERT EDGES IN 'batch_cap' VALUES ");
    for i in 0..=1000 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("('s{i}','d{i}','L')"));
    }

    let err = server
        .exec(&sql)
        .await
        .expect_err("a batch over the cap must be rejected");
    assert!(
        err.contains("at most 1000 edges"),
        "the error must name the cap so a loader can resize: {err}"
    );
}
