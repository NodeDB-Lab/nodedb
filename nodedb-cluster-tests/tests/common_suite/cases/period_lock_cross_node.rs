// SPDX-License-Identifier: BUSL-1.1
//! A closed period refuses a write on every node of a 3-node cluster.
//!
//! The period closes on one node and the write arrives on another. Each
//! scenario asserts SQLSTATE `23603` and that nothing applied.
//!
//! File name contains "cluster" so nextest applies the cluster test group.

use crate::common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, TestClusterNode, wait_for_async};
use tokio_postgres::SimpleQueryMessage;

const PERIOD_LOCKED: &str = "23603";

async fn setup(cluster: &TestCluster, periods: &str, entries: &str) {
    for sql in [
        format!(
            "CREATE COLLECTION {periods} (period_key TEXT PRIMARY KEY, status TEXT) \
             WITH (engine='document_schemaless')"
        ),
        format!(
            "CREATE COLLECTION {entries} (id TEXT PRIMARY KEY, fiscal_period TEXT, amount INT) \
             WITH (engine='document_schemaless')"
        ),
        format!(
            "ALTER COLLECTION {entries} ADD PERIOD LOCK ON fiscal_period \
             REFERENCES {periods}(period_key) STATUS status \
             ALLOW WRITE WHEN status IN ('OPEN')"
        ),
    ] {
        cluster
            .exec_ddl_on_any_leader(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

async fn rows(node: &TestClusterNode, sql: &str) -> Vec<String> {
    let messages = node.client.simple_query(sql).await.expect("select");
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

async fn status_is(node: &TestClusterNode, periods: &str, want: &str) -> bool {
    rows(
        node,
        &format!("SELECT status FROM {periods} WHERE period_key = '2026-01'"),
    )
    .await
        == [want]
}

async fn wait_status(cluster: &TestCluster, periods: &str, want: &str) {
    for node in &cluster.nodes {
        wait_for_async(
            "the period status reaches the node",
            Duration::from_secs(20),
            Duration::from_millis(100),
            || status_is(node, periods, want),
        )
        .await;
    }
}

async fn assert_refused(node: &TestClusterNode, sql: &str) {
    let err = node
        .client
        .simple_query(sql)
        .await
        .expect_err("the write into a closed period must be refused");
    let code = err.code().map(|c| c.code().to_owned());
    assert_eq!(
        code.as_deref(),
        Some(PERIOD_LOCKED),
        "node {}: expected 23603, got {err:?}",
        node.node_id
    );
}

/// Closed on one node, a write on each node is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_period_closed_on_one_node_refuses_a_write_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    setup(&cluster, "plx_a_periods", "plx_a_entries").await;
    cluster.nodes[0]
        .client
        .simple_query("INSERT INTO plx_a_periods (period_key, status) VALUES ('2026-01', 'OPEN')")
        .await
        .expect("seed period");
    wait_status(&cluster, "plx_a_periods", "OPEN").await;

    cluster.nodes[0]
        .client
        .simple_query("UPDATE plx_a_periods SET status = 'CLOSED' WHERE period_key = '2026-01'")
        .await
        .expect("close the period");
    wait_status(&cluster, "plx_a_periods", "CLOSED").await;

    for (i, node) in cluster.nodes.iter().enumerate() {
        assert_refused(
            node,
            &format!(
                "INSERT INTO plx_a_entries (id, fiscal_period, amount) VALUES ('e{i}', '2026-01', 5)"
            ),
        )
        .await;
    }
    for node in &cluster.nodes {
        assert!(
            rows(node, "SELECT id FROM plx_a_entries").await.is_empty(),
            "node {}: a refused write leaves no entry",
            node.node_id
        );
    }
    cluster.shutdown().await;
}

/// One request on a node closes the period, then writes into it.
/// It is refused on every node and the period stays open everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_request_that_closes_then_writes_is_refused_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    setup(&cluster, "plx_b_periods", "plx_b_entries").await;
    cluster.nodes[0]
        .client
        .simple_query("INSERT INTO plx_b_periods (period_key, status) VALUES ('2026-01', 'OPEN')")
        .await
        .expect("seed period");
    wait_status(&cluster, "plx_b_periods", "OPEN").await;

    for (i, node) in cluster.nodes.iter().enumerate() {
        assert_refused(
            node,
            &format!(
                "UPDATE plx_b_periods SET status = 'CLOSED' WHERE period_key = '2026-01'; \
                 INSERT INTO plx_b_entries (id, fiscal_period, amount) VALUES ('e{i}', '2026-01', 5)"
            ),
        )
        .await;
    }
    wait_status(&cluster, "plx_b_periods", "OPEN").await;
    for node in &cluster.nodes {
        assert!(
            rows(node, "SELECT id FROM plx_b_entries").await.is_empty(),
            "node {}: a refused request applies no entry",
            node.node_id
        );
    }
    cluster.shutdown().await;
}

/// Write first, then close: the request applies on a follower and the
/// entry and the closed period reach every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_request_that_writes_then_closes_applies_from_a_follower() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    setup(&cluster, "plx_c_periods", "plx_c_entries").await;
    cluster.nodes[0]
        .client
        .simple_query("INSERT INTO plx_c_periods (period_key, status) VALUES ('2026-01', 'OPEN')")
        .await
        .expect("seed period");
    wait_status(&cluster, "plx_c_periods", "OPEN").await;

    cluster.nodes[2]
        .client
        .simple_query(
            "INSERT INTO plx_c_entries (id, fiscal_period, amount) VALUES ('e1', '2026-01', 5); \
             UPDATE plx_c_periods SET status = 'CLOSED' WHERE period_key = '2026-01'",
        )
        .await
        .expect("write then close applies");
    wait_status(&cluster, "plx_c_periods", "CLOSED").await;
    for node in &cluster.nodes {
        wait_for_async(
            "the entry reaches the node",
            Duration::from_secs(20),
            Duration::from_millis(100),
            || async { rows(node, "SELECT id FROM plx_c_entries").await == ["e1"] },
        )
        .await;
    }
    cluster.shutdown().await;
}
