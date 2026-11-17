// SPDX-License-Identifier: BUSL-1.1

//! Cross-node distributed PageRank mass conservation with dangling nodes.
//!
//! A dangling node (out-degree 0) cannot scatter its rank to any neighbor, so its
//! mass must be redistributed via the teleport base: `(1−d)/n + d*dangling_sum/n`,
//! where `dangling_sum` is the GLOBAL sum of rank held by ALL dangling nodes.
//!
//! The bug this test guards against: `dangling_sum` was computed PER-SHARD and
//! redistributed only to the owning shard's nodes. If dangling nodes reside on
//! different shards/nodes than their "mass consumers", the redistribution only
//! covers a fraction of the graph — causing the ranks to sum to < 1.0 even at
//! convergence. The fix aggregates all shards' local dangling sums at the
//! coordinator and passes the GLOBAL total into each shard's next superstep.
//!
//! Topology: hub + spokes-with-dead-ends.
//!   - `root` → `mid_i` for i in 0..M
//!   - `mid_i` → `dst_i` for i in 0..M
//!   - `dst_i` has NO outgoing edges (dangling)
//!
//! With M=12 this gives 1 + 2*M = 25 nodes, of which M=12 are dangling. The
//! distinct node names hash to vShards spanning all three cluster nodes, so
//! dangling mass is spread across nodes and the bug manifests clearly.
//!
//! Acceptance invariants checked from EVERY coordinator node:
//! (a) Completeness — exactly the expected 1+2M distinct nodes.
//! (b) Mass conservation — ranks sum to ≈ 1.0 (within 1e-3).
//!
//! Without the fix, invariant (b) fails because the per-shard dangling
//! redistribution discards `(global_N - local_N) / global_N` of dangling mass on
//! each shard.

use std::collections::HashSet;
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// All `(node_id, rank)` rows from a `GRAPH ALGO PAGERANK` result.
async fn pagerank_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<(String, f64)> {
    let msgs = client
        .simple_query(sql)
        .await
        .expect("simple_query PAGERANK dangling");
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

/// Distributed PageRank with dangling nodes spread across all cluster shards must
/// conserve mass: ranks sum to ≈ 1.0, not < 1.0.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn pagerank_dangling_nodes_mass_conserved_cross_node() {
    // 1 core per node is enough — the dangling nodes span 3 NODES/shards, which
    // is the exact scenario that exposes the per-shard dangling-sum loss.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION gpd_xnode")
        .await
        .expect("CREATE COLLECTION gpd_xnode");

    wait_for(
        "all 3 nodes see gpd_xnode",
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

    // Hub + spokes-with-dead-ends topology:
    //   root → mid_0, mid_1, …, mid_{M-1}
    //   mid_i → dst_i                        (dst_i has out-degree 0: dangling)
    //
    // Total nodes: 1 + 2*M. Dangling nodes: M.
    // The distinct names hash to vShards spanning all three nodes.
    const M: usize = 12;
    let total_nodes = 1 + 2 * M; // 25

    // root → mid_i edges
    for i in 0..M {
        let mid = format!("mid_{i}");
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gpd_xnode' FROM 'root' TO '{mid}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert root -> {mid}: {e}"));
    }

    // mid_i → dst_i edges (dst_i will be dangling — no outgoing edges added)
    for i in 0..M {
        let mid = format!("mid_{i}");
        let dst = format!("dst_{i}");
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gpd_xnode' FROM '{mid}' TO '{dst}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {mid} -> {dst}: {e}"));
    }

    // Build the expected node set: root + M mid_i + M dst_i.
    let mut expected_nodes: HashSet<String> = HashSet::new();
    expected_nodes.insert("root".to_string());
    for i in 0..M {
        expected_nodes.insert(format!("mid_{i}"));
        expected_nodes.insert(format!("dst_{i}"));
    }
    assert_eq!(expected_nodes.len(), total_nodes);

    // High ITERATIONS budget so the algorithm converges (mirrors the convergence
    // note in graph_multicore_cross_node.rs — the default 20 iterations may not
    // be sufficient for a hub-and-spoke topology with dangling nodes).
    const PAGERANK_SQL: &str = "GRAPH ALGO PAGERANK ON 'gpd_xnode' ITERATIONS 100";

    // Wait until EVERY node sees all expected nodes in the PageRank result —
    // edges applied + replicated AND routing view converged so a non-owning
    // coordinator runs the BSP loop across all owning shards.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all {total_nodes} pagerank nodes (dangling test)"),
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

    // From every coordinator node: assert completeness AND mass conservation.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = pagerank_rows(&node.client, PAGERANK_SQL).await;

        // (a) Completeness — exactly the expected distinct node set, no dups.
        let names: HashSet<String> = rows.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(
            names.len(),
            total_nodes,
            "node {idx} PAGERANK (dangling) must return one row per distinct node; got {rows:?}"
        );
        assert_eq!(
            rows.len(),
            names.len(),
            "node {idx} PAGERANK (dangling) returned duplicate node rows: {rows:?}"
        );
        assert_eq!(
            names, expected_nodes,
            "node {idx} PAGERANK (dangling) must cover exactly the inserted nodes; got {rows:?}"
        );

        // (b) Mass conservation — the fix ensures dangling mass redistributes
        // GLOBALLY (using the cluster-wide dangling sum), so ranks sum to ≈ 1.0.
        // Without the fix, only each shard's LOCAL dangling mass was redistributed,
        // causing the sum to be < 1.0 (roughly (local_N / global_N) of dangling
        // mass was lost per shard).
        let sum: f64 = rows.iter().map(|(_, r)| *r).sum();
        assert!(
            (sum - 1.0).abs() < 1e-3,
            "node {idx} cross-shard PageRank with dangling nodes must sum to ~1.0 \
             (mass conserved via global dangling redistribution); got {sum}"
        );
    }

    cluster.shutdown().await;
}
