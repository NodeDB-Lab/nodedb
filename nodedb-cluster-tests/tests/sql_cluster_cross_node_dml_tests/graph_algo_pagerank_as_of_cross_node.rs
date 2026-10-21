// SPDX-License-Identifier: BUSL-1.1

//! Historical distributed PageRank: `GRAPH ALGO PAGERANK ... AS OF SYSTEM TIME`
//! runs the BSP supersteps across every shard at one pinned cut.
//!
//! A ring spread across all three nodes is written, a timestamp taken, then
//! one ring edge is deleted and an edge to a new node added. A run at the
//! timestamp answers the ring from every coordinator: exactly the ring's
//! nodes, uniform ranks summing to 1.0. A current run answers the changed
//! graph.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::common::cluster_harness::{TestCluster, wait_for};

const N: usize = 24;

async fn pagerank_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<(String, f64)> {
    let msgs = client
        .simple_query(sql)
        .await
        .expect("simple_query PAGERANK");
    let mut out = Vec::new();
    for m in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            let node = r.get("node_id").unwrap_or("").to_string();
            let rank: f64 = r.get("rank").unwrap_or("0").parse().unwrap_or(0.0);
            out.push((node, rank));
        }
    }
    out
}

/// Wait until `sql` run from every node names exactly `expected`.
async fn wait_every_node_sees(cluster: &TestCluster, sql: &str, expected: &HashSet<String>) {
    for idx in 0..cluster.nodes.len() {
        wait_for(
            &format!("node {idx} sees {} pagerank nodes", expected.len()),
            Duration::from_secs(30),
            Duration::from_millis(200),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = pagerank_rows(&cluster.nodes[idx].client, sql).await;
                        let names: HashSet<String> = rows.into_iter().map(|(n, _)| n).collect();
                        &names == expected
                    })
                })
            },
        )
        .await;
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn historical_pagerank_runs_bsp_at_one_cut_cross_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION gpr_asof")
        .await
        .expect("CREATE COLLECTION gpr_asof");
    wait_for(
        "all 3 nodes see gpr_asof",
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

    let client = &cluster.nodes[0].client;
    for i in 0..N {
        let src = format!("n_{i}");
        let dst = format!("n_{}", (i + 1) % N);
        client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gpr_asof' FROM '{src}' TO '{dst}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {e}"));
    }
    let ring: HashSet<String> = (0..N).map(|i| format!("n_{i}")).collect();
    wait_every_node_sees(&cluster, "GRAPH ALGO PAGERANK ON 'gpr_asof'", &ring).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let cut = now_ms();
    tokio::time::sleep(Duration::from_millis(100)).await;

    client
        .simple_query("GRAPH DELETE EDGE IN 'gpr_asof' FROM 'n_0' TO 'n_1' TYPE 'K'")
        .await
        .expect("delete ring edge");
    client
        .simple_query("GRAPH INSERT EDGE IN 'gpr_asof' FROM 'n_0' TO 'extra' TYPE 'K'")
        .await
        .expect("insert edge to a new node");
    let mut changed = ring.clone();
    changed.insert("extra".to_string());
    wait_every_node_sees(&cluster, "GRAPH ALGO PAGERANK ON 'gpr_asof'", &changed).await;

    let historical = format!("GRAPH ALGO PAGERANK ON 'gpr_asof' AS OF SYSTEM TIME {cut}");
    for idx in 0..cluster.nodes.len() {
        let rows = pagerank_rows(&cluster.nodes[idx].client, &historical).await;
        let names: HashSet<String> = rows.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(
            names, ring,
            "node {idx}: a run at the cut covers exactly the ring; got {rows:?}"
        );
        assert_eq!(rows.len(), N, "node {idx}: one row per node; got {rows:?}");
        let sum: f64 = rows.iter().map(|(_, r)| *r).sum();
        assert!(
            (sum - 1.0).abs() < 1e-3,
            "node {idx}: historical ranks sum to ~1.0; got {sum}"
        );
        let uniform = 1.0 / N as f64;
        for (node, rank) in &rows {
            assert!(
                (rank - uniform).abs() < 1e-3,
                "node {idx}: the ring is uniform at the cut; {node} has {rank}"
            );
        }
    }

    cluster.shutdown().await;
}
