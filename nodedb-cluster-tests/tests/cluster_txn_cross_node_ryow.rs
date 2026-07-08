// SPDX-License-Identifier: BUSL-1.1

//! Cross-node in-transaction read-your-own-writes.
//!
//! An explicit transaction's staged writes live in per-core overlays keyed by
//! the session's transaction id. When the coordinator is NOT the leader for
//! the collection's data group, both the staged write (`MetaOp::StageWrite`)
//! and every in-block read cross the gateway to a remote node — so the
//! transaction id must survive the `ExecuteRequest` wire hop, or the remote
//! executor cannot key the overlay (staged writes are rejected with
//! "StageWrite dispatched without a txn_id") and in-block reads cannot see
//! the transaction's own staged rows.
//!
//! Steps, from EVERY node of a 3-node cluster (at most one of the three is
//! the collection's data-group leader, so at least two exercise the
//! cross-node hop):
//! 1. BEGIN on a dedicated connection (session state is per-connection).
//! 2. INSERT one row — a stageable point write, staged to the target shard's
//!    overlay at statement time.
//! 3. Point SELECT of that row — must see the staged row (RYOW via the
//!    overlay merge on the shard that staged it).
//! 4. Full-collection SELECT — must also see it (the gather path threads the
//!    same transaction id).
//! 5. ROLLBACK — drops the overlay on every staged shard.
//! 6. Both SELECTs again — the row must be gone (nothing was committed).
//!
//! Committed rows are never touched: the collection stays empty throughout,
//! so any row visible outside the transaction block is a bug on its own.
//!
//! File name contains "cluster" via the cluster-tests crate so nextest
//! applies the cluster test-group serialization.

use std::time::Duration;

mod common;

use crate::common::cluster_harness::{TestCluster, wait_for};

fn count_rows(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// Render every returned row's columns — failure diagnostics only.
fn dump_rows(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Vec<String> {
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or("<null>").to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn in_txn_staged_row_is_visible_and_rolls_back_from_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let coll = "txn_xnode_ryow";

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {coll} (id TEXT PRIMARY KEY, v TEXT)"
        ))
        .await
        .expect("CREATE COLLECTION");

    wait_for(
        "all 3 nodes see the collection",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    for (i, node) in cluster.nodes.iter().enumerate() {
        let id = format!("row-from-node-{i}");

        node.client
            .simple_query("BEGIN")
            .await
            .unwrap_or_else(|e| panic!("node {i}: BEGIN failed: {e}"));

        // Stageable point write. On a non-leader coordinator this ships a
        // StageWrite to the remote data-group leader — the transaction id
        // must ride the wire with it.
        node.client
            .simple_query(&format!(
                "INSERT INTO {coll} (id, v) VALUES ('{id}', 'staged')"
            ))
            .await
            .unwrap_or_else(|e| {
                panic!("node {i}: in-transaction INSERT (staged write) failed: {e}")
            });

        // RYOW via point lookup (overlay merge on the owning shard).
        let point = node
            .client
            .simple_query(&format!("SELECT * FROM {coll} WHERE id = '{id}'"))
            .await
            .unwrap_or_else(|e| panic!("node {i}: in-transaction point SELECT failed: {e}"));
        assert_eq!(
            count_rows(&point),
            1,
            "node {i}: point read inside the transaction must see its own staged row"
        );

        // RYOW via the scan/gather path.
        let scan = node
            .client
            .simple_query(&format!("SELECT * FROM {coll}"))
            .await
            .unwrap_or_else(|e| panic!("node {i}: in-transaction scan SELECT failed: {e}"));
        assert_eq!(
            count_rows(&scan),
            1,
            "node {i}: scan inside the transaction must see exactly its own staged row; got {:?}",
            dump_rows(&scan)
        );

        node.client
            .simple_query("ROLLBACK")
            .await
            .unwrap_or_else(|e| panic!("node {i}: ROLLBACK failed: {e}"));

        // The overlay is dropped on every staged shard; nothing was committed.
        let after = node
            .client
            .simple_query(&format!("SELECT * FROM {coll} WHERE id = '{id}'"))
            .await
            .unwrap_or_else(|e| panic!("node {i}: post-ROLLBACK SELECT failed: {e}"));
        assert_eq!(
            count_rows(&after),
            0,
            "node {i}: the staged row must vanish on ROLLBACK"
        );
    }

    cluster.shutdown().await;
}
