// SPDX-License-Identifier: BUSL-1.1

//! A cluster array aggregate reads one pinned system-time cut.
//!
//! `ARRAY_AGG ... AS OF SYSTEM TIME` pins to one ordinal under the
//! statement's read cut. The pinned scope, with its cut marker, rides each
//! shard request, and every serving node waits for the marker before it
//! reads. A grouped aggregate run on any node while a writer appends cells
//! across every shard reads a prefix of the writer's history, and a past
//! cut reads the same groups on every node and every run.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, TestClusterNode};

/// Cells the writer appends, one per `x`. Tiles of extent 4 spread them over
/// 16 tiles and many vShards.
const CELLS: i64 = 64;
/// Cells written before the past cut.
const BEFORE_CUT: i64 = 16;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

/// A cut one day past the wall clock.
fn future_ms() -> i64 {
    now_ms() + 86_400_000
}

async fn write_cell(node: &TestClusterNode, x: i64) {
    let sql = format!("INSERT INTO ARRAY bac_cut COORDS ({x}) VALUES ({x})");
    node.exec(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// The `x` groups a grouped count reads through `node` at `cut`. Every
/// group counts one cell.
async fn groups_at(node: &TestClusterNode, cut: i64) -> BTreeSet<i64> {
    let sql =
        format!("SELECT * FROM ARRAY_AGG('bac_cut', 'v', 'count', 'x') AS OF SYSTEM TIME {cut}");
    let messages = node
        .client
        .simple_query(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let group = row.try_get("group").ok().flatten()?;
                let count: f64 = row
                    .get("result")
                    .and_then(|r| r.parse().ok())
                    .unwrap_or_else(|| panic!("{sql}: group {group} has no count"));
                assert_eq!(count, 1.0, "{sql}: group {group} counts one cell");
                Some(group.parse().expect("group"))
            }
            _ => None,
        })
        .collect()
}

/// Check that `groups` is a prefix `0..n` of the writer's history.
fn assert_prefix(groups: &BTreeSet<i64>, stage: &str) {
    let expected: BTreeSet<i64> = (0..groups.len() as i64).collect();
    assert_eq!(
        groups, &expected,
        "{stage}: the aggregate read a hole in the writer's history, so its shards read \
         different cuts"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_cluster_array_aggregate_reads_one_pinned_cut() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("3-node cluster spawn");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE ARRAY bac_cut DIMS (x INT64 [0..{}]) ATTRS (v INT64) TILE_EXTENTS (4)",
            CELLS - 1
        ))
        .await
        .expect("CREATE ARRAY bac_cut");

    for x in 0..BEFORE_CUT {
        write_cell(&cluster.nodes[0], x).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let past_cut = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let done = AtomicBool::new(false);
    let writer = async {
        for x in BEFORE_CUT..CELLS {
            write_cell(&cluster.nodes[0], x).await;
        }
        done.store(true, Ordering::SeqCst);
    };
    let reader = async {
        let mut reads = 0usize;
        while !done.load(Ordering::SeqCst) {
            let node = &cluster.nodes[reads % cluster.nodes.len()];
            let groups = groups_at(node, future_ms()).await;
            assert_prefix(
                &groups,
                &format!("future cut through node {}", node.node_id),
            );
            reads += 1;
        }
        reads
    };
    let ((), reads) = tokio::join!(writer, reader);
    assert!(reads > 0, "the aggregate ran while the writer wrote");

    let before: BTreeSet<i64> = (0..BEFORE_CUT).collect();
    let all: BTreeSet<i64> = (0..CELLS).collect();
    for node in &cluster.nodes {
        assert_eq!(
            groups_at(node, future_ms()).await,
            all,
            "node {}: every cell is under a future cut after the writer ends",
            node.node_id
        );
        for run in 0..2 {
            assert_eq!(
                groups_at(node, past_cut).await,
                before,
                "node {}, run {run}: a past cut reads the cells written before it",
                node.node_id
            );
        }
    }

    cluster.shutdown().await;
}
