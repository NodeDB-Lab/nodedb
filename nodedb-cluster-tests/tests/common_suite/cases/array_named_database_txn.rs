// SPDX-License-Identifier: BUSL-1.1

//! A transaction that writes an array outside the default database, on three
//! nodes. The record's name reaches the write note on every replica, so a name
//! it refused halted every replica. Each test commits, reads the cell, and
//! restarts the cluster.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, read_once_a_leader_exists};

const DATABASE: &str = "cluster_arr_txn_db";

async fn use_database(cluster: &TestCluster) {
    for node in &cluster.nodes {
        node.exec(&format!("USE DATABASE {DATABASE}"))
            .await
            .unwrap_or_else(|e| panic!("USE DATABASE: {e}"));
    }
}

async fn named_array(cluster: &TestCluster, array: &str) {
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("create database");
    use_database(cluster).await;
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE ARRAY {array} DIMS (x INT64 [0..15]) ATTRS (v INT64) TILE_EXTENTS (16) \
             CELL_ORDER ROW_MAJOR"
        ))
        .await
        .expect("create array");
}

/// How many cells every node reads of `array`, once each one agrees on `expected`.
async fn assert_cells(cluster: &TestCluster, array: &str, expected: usize) {
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let sql = format!("SELECT * FROM ARRAY_SLICE('{array}', '{{x: [0, 15]}}', ['v'], 100)");
    for node in &cluster.nodes {
        let messages = read_once_a_leader_exists(
            &sql,
            Duration::from_secs(30),
            Duration::from_millis(100),
            || node.client.simple_query(&sql),
        )
        .await;
        let rows = messages
            .iter()
            .filter(|message| matches!(message, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        assert_eq!(rows, expected, "node {} reads {array}", node.node_id);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_transaction_inserts_an_array_cell_in_a_named_database_on_every_node() {
    const ARRAY: &str = "cluster_arr_txn_put";
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    named_array(&cluster, ARRAY).await;

    let writer = &cluster.nodes[0];
    writer.exec("BEGIN").await.expect("begin");
    writer
        .exec(&format!("INSERT INTO ARRAY {ARRAY} COORDS (1) VALUES (11)"))
        .await
        .expect("insert");
    writer.exec("COMMIT").await.expect("commit");
    assert_cells(&cluster, ARRAY, 1).await;

    // Every core stays up: a later write commits.
    cluster.nodes[1]
        .exec(&format!("INSERT INTO ARRAY {ARRAY} COORDS (2) VALUES (12)"))
        .await
        .expect("a write after the transaction");
    assert_cells(&cluster, ARRAY, 2).await;

    let cluster = cluster.restart_all().await.expect("restart");
    use_database(&cluster).await;
    assert_cells(&cluster, ARRAY, 2).await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_transaction_deletes_an_array_cell_in_a_named_database_on_every_node() {
    const ARRAY: &str = "cluster_arr_txn_del";
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    named_array(&cluster, ARRAY).await;
    let writer = &cluster.nodes[0];
    for coord in [3, 4] {
        writer
            .exec(&format!(
                "INSERT INTO ARRAY {ARRAY} COORDS ({coord}) VALUES (5)"
            ))
            .await
            .expect("insert");
    }

    writer.exec("BEGIN").await.expect("begin");
    writer
        .exec(&format!("DELETE FROM ARRAY {ARRAY} WHERE COORDS IN ((3))"))
        .await
        .expect("delete");
    writer.exec("COMMIT").await.expect("commit");
    assert_cells(&cluster, ARRAY, 1).await;

    let cluster = cluster.restart_all().await.expect("restart");
    use_database(&cluster).await;
    assert_cells(&cluster, ARRAY, 1).await;

    cluster.shutdown().await;
}
