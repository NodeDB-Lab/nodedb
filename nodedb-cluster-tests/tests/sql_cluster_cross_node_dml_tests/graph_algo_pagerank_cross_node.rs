// SPDX-License-Identifier: BUSL-1.1

//! Cross-node distributed PageRank correctness (F1d-4 Phase B).
//!
//! Graph edges are Raft-homed on `from_key(src)` and each Data-Plane core's CSR
//! is PARTITIONED — it holds only the edges whose source is homed to its vShard.
//! A `GRAPH ALGO PAGERANK` coordinated from any node therefore CANNOT be
//! computed on the coordinator's local partitions alone: most nodes' out-edges
//! live on other shards/nodes, and PageRank's teleport / dangling terms need the
//! GLOBAL node count, while every rank push from a node to a neighbor on another
//! shard is a cross-shard contribution.
//!
//! Phase B wires the Control-Plane BSP coordinator: a count phase sums every
//! shard's owned vertex count into `global_n`, then a superstep loop dispatches
//! one `GraphOp::BspSuperstep` per shard, routes each shard's cross-shard
//! contributions to the owning shard for the next superstep, and assembles the
//! per-shard ranks into the same result shape as single-node PageRank.
//!
//! The acceptance invariant is mass conservation: PageRank ranks SUM to ≈ 1.0.
//! That only holds if (a) `global_n` is the true cluster-wide node count (count
//! phase crossed all shards), (b) every cross-shard contribution was routed to
//! the owning shard (no mass dropped at a boundary), and (c) teleport mass uses
//! the global `n`. A partial / boundary-dropping implementation would sum to
//! less than 1.0. We force a genuine cross-shard graph by spreading many
//! distinct node names across vShards spanning all three nodes.

use std::collections::HashSet;
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// All `(node_id, rank)` rows from a `GRAPH ALGO PAGERANK` result (columns
/// `node_id` text, `rank` float).
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

/// Distributed PageRank from ANY coordinator must (i) sum to ≈ 1.0 — proving the
/// count phase + cross-shard contribution routing + global teleport are correct
/// across shard boundaries — and (ii) return exactly one row per distinct node.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn pagerank_sums_to_one_cross_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION gpr_xnode")
        .await
        .expect("CREATE COLLECTION gpr_xnode");

    wait_for(
        "all 3 nodes see gpr_xnode",
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

    // Build a single directed RING over N distinct node names:
    //   n_0 -> n_1 -> ... -> n_{N-1} -> n_0
    // The distinct `n_i` names hash to vShards spanning all three nodes, so the
    // ring's edges (and the rank pushes along them) cross shard boundaries
    // repeatedly — exactly the case the BSP coordinator must stitch together. A
    // ring is also strongly connected with uniform out-degree 1, so the exact
    // PageRank is uniform (1/N each) and the ranks MUST sum to 1.0.
    const N: usize = 24;
    for i in 0..N {
        let src = format!("n_{i}");
        let dst = format!("n_{}", (i + 1) % N);
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gpr_xnode' FROM '{src}' TO '{dst}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {e}"));
    }

    let expected_nodes: HashSet<String> = (0..N).map(|i| format!("n_{i}")).collect();

    const PAGERANK_SQL: &str = "GRAPH ALGO PAGERANK ON 'gpr_xnode'";

    // Wait until EVERY node sees all N nodes in the PageRank result — edges
    // applied + replicated AND each node's routing view converged so a
    // non-owning coordinator runs the count phase and superstep loop across all
    // owning shards.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all {N} pagerank nodes"),
            Duration::from_secs(30),
            Duration::from_millis(200),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = pagerank_rows(&node.client, PAGERANK_SQL).await;
                        let names: HashSet<String> = rows.into_iter().map(|(n, _)| n).collect();
                        names == expected_nodes
                    })
                })
            },
        )
        .await;
    }

    // From every node: ranks sum to ≈ 1.0 and exactly one row per distinct node.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = pagerank_rows(&node.client, PAGERANK_SQL).await;

        // (ii) Row count == distinct node count (no dup, no drop).
        let names: HashSet<String> = rows.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(
            names.len(),
            N,
            "node {idx} PAGERANK must return one row per distinct node; got {rows:?}"
        );
        assert_eq!(
            rows.len(),
            names.len(),
            "node {idx} PAGERANK returned duplicate node rows: {rows:?}"
        );
        assert_eq!(
            names, expected_nodes,
            "node {idx} PAGERANK must cover exactly the inserted nodes; got {rows:?}"
        );

        // (i) Mass conservation across shards: ranks sum to ≈ 1.0.
        let sum: f64 = rows.iter().map(|(_, r)| *r).sum();
        assert!(
            (sum - 1.0).abs() < 1e-3,
            "node {idx} cross-shard PageRank ranks must sum to ~1.0 (mass conserved); got {sum}"
        );
    }

    cluster.shutdown().await;
}
