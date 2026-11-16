// SPDX-License-Identifier: BUSL-1.1

//! Regression test for the "remote scan truncated to one chunk" cross-node bug.
//!
//! A SELECT whose result exceeds `stream_chunk_size` (default 1000 rows) is
//! emitted by the Data Plane as several `Partial` frames followed by a terminal
//! frame. On the LOCAL dispatch path the Control Plane drains and concatenates
//! every frame. The cross-node remote executor (`LocalPlanExecutor`) must not
//! consume only the FIRST frame off the response channel — doing so would
//! make a SELECT issued from a node that does not lead the collection's
//! vShard return only the first 1000-row chunk and silently drop the rest
//! (and orphan the request's tracker entry).
//!
//! The remote executor must drain the full bounded response, so a
//! SELECT returns every row regardless of which node it is issued from.
//!
//! The collection is a single-vShard-homed standard collection, so all rows
//! live on ONE owning vShard. The node that leads that vShard answers locally
//! (always correct); the other two nodes route the scan to it over QUIC and so
//! exercise the remote-drain path. Asserting the full count from every node
//! therefore covers both.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Run a simple query and return the number of data rows returned.
async fn count_rows(client: &tokio_postgres::Client, sql: &str) -> usize {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    msgs.into_iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// A cross-node SELECT whose result spans multiple stream chunks must return
/// EVERY row from any coordinator node — not just the first chunk the remote
/// shard streamed back.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_select_returns_all_chunks() {
    // More than `stream_chunk_size` (1000) so the remote scan streams as
    // several Partial frames + a terminal frame — 2500 → 3 chunks.
    const ROWS: usize = 2_500;

    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION wide \
             (id TEXT PRIMARY KEY, n TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION wide");

    wait_for(
        "all 3 nodes see collection wide",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    // Insert ROWS rows in batches so each INSERT statement stays modest.
    const BATCH: usize = 500;
    let mut i = 0;
    while i < ROWS {
        let mut sql = String::from("INSERT INTO wide (id, n) VALUES ");
        let end = (i + BATCH).min(ROWS);
        for j in i..end {
            if j > i {
                sql.push(',');
            }
            // Zero-padded keys keep them distinct and uniform width.
            sql.push_str(&format!("('k{j:06}', 'v{j:06}')"));
        }
        cluster.nodes[0]
            .client
            .simple_query(&sql)
            .await
            .unwrap_or_else(|e| panic!("insert batch [{i}, {end}): {e}"));
        i = end;
    }

    // Wait until every node sees all rows applied. Each node has the vShard's
    // data Raft-replicated to its local store and scans it for this read, so the
    // count converges on every node once replication completes.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all {ROWS} rows"),
            Duration::from_secs(30),
            Duration::from_millis(100),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, "SELECT id FROM wide"))
                        >= ROWS
                })
            },
        )
        .await;
    }

    // From EVERY node: the full result spanning all stream chunks — before the
    // fix the result was silently truncated to the first 1000-row chunk because
    // the streamed chunk arrays were raw-concatenated (only the first array was
    // decoded) instead of merged into one array.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let got = count_rows(&node.client, "SELECT id FROM wide").await;
        assert_eq!(
            got, ROWS,
            "node {idx} SELECT must return all {ROWS} rows across stream chunks; got {got}"
        );
    }

    cluster.shutdown().await;
}
