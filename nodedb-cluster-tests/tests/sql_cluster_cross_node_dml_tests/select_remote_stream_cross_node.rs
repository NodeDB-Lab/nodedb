// SPDX-License-Identifier: BUSL-1.1

//! End-to-end cluster test for the L4 remote QUIC streaming path.
//!
//! A SELECT whose result exceeds `stream_chunk_size` is emitted by the Data
//! Plane as several `Partial` frames followed by a terminal frame. On a node
//! that does NOT lead the collection's vShard, the streamable scan is routed to
//! the owning node over the cluster QUIC transport as an `ExecuteStreamRequest`:
//! the owner streams `RPC_EXECUTE_STREAM_CHUNK` frames back, and the
//! coordinator's gather merges those remote chunks with any local streams via
//! the same `select_all`.
//!
//! This is the streaming sibling of `select_streaming_cross_node.rs` (which
//! exercises the one-shot remote-drain path). The collection is single-vShard-
//! homed, so all rows live on ONE owning vShard. The node that leads that vShard
//! answers from its local cores; the other two route the scan to it over QUIC
//! and so exercise the remote streaming transport. Asserting the full count
//! from every node covers both.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Run a simple query and return the number of data rows returned.
async fn count_rows(client: &tokio_postgres::Client, sql: &str) -> usize {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    msgs.into_iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// A cross-node streamable SELECT spanning multiple chunks must return EVERY
/// row from any coordinator node — the remote shard streams them over QUIC.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_remote_stream_returns_all_chunks() {
    // More than `stream_chunk_size` (1000) so the remote scan streams as
    // several chunk frames + a terminal frame — 2500 → 3 chunks.
    const ROWS: usize = 2_500;

    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION streamwide \
             (id TEXT PRIMARY KEY, n TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION streamwide");

    wait_for(
        "all 3 nodes see collection streamwide",
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
        let mut sql = String::from("INSERT INTO streamwide (id, n) VALUES ");
        let end = (i + BATCH).min(ROWS);
        for j in i..end {
            if j > i {
                sql.push(',');
            }
            sql.push_str(&format!("('k{j:06}', 'v{j:06}')"));
        }
        cluster.nodes[0]
            .client
            .simple_query(&sql)
            .await
            .unwrap_or_else(|e| panic!("insert batch [{i}, {end}): {e}"));
        i = end;
    }

    // Wait until every node sees all rows applied (Raft replication).
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all {ROWS} rows"),
            Duration::from_secs(30),
            Duration::from_millis(100),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, "SELECT id FROM streamwide"))
                        >= ROWS
                })
            },
        )
        .await;
    }

    // From EVERY node: the full result spanning all stream chunks. The two
    // non-owning nodes pull the chunks across the QUIC streaming transport.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let got = count_rows(&node.client, "SELECT id FROM streamwide").await;
        assert_eq!(
            got, ROWS,
            "node {idx} streamed SELECT must return all {ROWS} rows across chunks; got {got}"
        );
    }

    cluster.shutdown().await;
}
