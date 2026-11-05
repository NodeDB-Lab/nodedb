// SPDX-License-Identifier: BUSL-1.1
//! Cluster-wide stored-byte totals.
//!
//! Each node samples the bytes of the vShards it leads and asks every peer
//! for its share. Every replica stores the same bytes, and only each group's
//! leader counts them. So every node publishes one copy of the data, and the
//! same total on each node. One node's bytes over every vShard is that copy,
//! because each of the three nodes holds a full replica.

use crate::common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for_report};

use nodedb::control::metrics::storage::query_local_storage_bytes;
use nodedb_types::DatabaseId;
use nodedb_types::id::VShardId;

/// Sample period every node runs in this test, in milliseconds.
const SAMPLE_INTERVAL_MS: &str = "300";
const SAMPLE_PERIOD: Duration = Duration::from_millis(300);

const COLL: &str = "storage_totals_docs";
const ROWS: usize = 200;
const BODY_BYTES: usize = 1_000;

/// The cluster total of the default database on every node, or why not.
fn totals(cluster: &TestCluster) -> Result<Vec<u64>, String> {
    cluster
        .nodes
        .iter()
        .map(|node| {
            let store = &node.shared.cluster_storage_totals;
            if store.has_total() {
                Ok(store.database_bytes(DatabaseId::DEFAULT))
            } else {
                Err(format!("node {} has no complete total yet", node.node_id))
            }
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn every_node_reports_one_copy_of_the_stored_bytes() {
    // Every node reads the sample period at spawn.
    unsafe {
        std::env::set_var("NODEDB_STORAGE_SAMPLE_INTERVAL_MS", SAMPLE_INTERVAL_MS);
    }
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("CREATE COLLECTION");

    // The bytes the default database holds before the rows land.
    wait_for_report(
        "every node publishes a first complete total",
        Duration::from_secs(30),
        Duration::from_millis(100),
        || totals(&cluster).map(|_| ()),
    )
    .await;
    let baseline = totals(&cluster)
        .expect("complete totals")
        .into_iter()
        .max()
        .unwrap_or(0);

    let body = "x".repeat(BODY_BYTES);
    for i in 0..ROWS {
        cluster.nodes[0]
            .exec(&format!(
                "INSERT INTO {COLL} (id, body) VALUES ('row-{i:04}', '{body}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert row {i}: {e}"));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let payload = (ROWS * BODY_BYTES) as u64;
    let check = || -> Result<(), String> {
        let totals: Vec<u64> = totals(&cluster)?
            .into_iter()
            .map(|total| total.saturating_sub(baseline))
            .collect();
        let (low, high) = (
            totals.iter().copied().min().unwrap_or(0),
            totals.iter().copied().max().unwrap_or(0),
        );
        if low < payload {
            return Err(format!(
                "totals {totals:?} above the baseline hold less than the {payload} bytes written"
            ));
        }
        // Rows still settle between rounds. One percent covers that.
        if high - low > high / 100 {
            return Err(format!("the nodes disagree: {totals:?}"));
        }
        Ok(())
    };
    wait_for_report(
        "every node publishes the same cluster total",
        Duration::from_secs(30),
        Duration::from_millis(100),
        &check,
    )
    .await;

    // Two more periods: the totals hold steady across rounds.
    tokio::time::sleep(SAMPLE_PERIOD * 2).await;
    wait_for_report(
        "the cluster total holds across rounds",
        Duration::from_secs(10),
        Duration::from_millis(100),
        &check,
    )
    .await;

    // One replica's bytes: node 0 read over every vShard, led or not.
    let one_copy: u64 = query_local_storage_bytes(
        &cluster.nodes[0].shared,
        (0..VShardId::COUNT).collect(),
        std::time::Instant::now() + Duration::from_secs(10),
    )
    .await
    .expect("read one replica's bytes")
    .into_iter()
    .filter(|((_, database, _), _)| *database == DatabaseId::DEFAULT)
    .map(|(_, bytes)| bytes)
    .sum();
    let totals = totals(&cluster).expect("complete totals");
    for (node, total) in cluster.nodes.iter().zip(&totals) {
        assert!(
            total.abs_diff(one_copy) <= one_copy / 100,
            "node {} reports {total} bytes, one replica holds {one_copy}: \
             the cluster total counts one copy",
            node.node_id,
        );
    }

    cluster.shutdown().await;
}
