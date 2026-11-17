// SPDX-License-Identifier: BUSL-1.1

//! Cross-node distributed WCC correctness (single-round contraction).
//!
//! Graph edges are Raft-homed on `from_key(src)` and each Data-Plane core's CSR
//! is PARTITIONED — it holds only the edges whose source is homed to its vShard.
//! A `GRAPH ALGO WCC` coordinated from any node therefore CANNOT be computed on
//! the coordinator's local partitions alone: most nodes' out-edges live on other
//! shards/nodes, and connected components that span shard boundaries must be
//! stitched together by the coordinator.
//!
//! The WCC coordinator dispatches ONE `GraphOp::WccSuperstep` per owner node:
//! each shard contracts its OWNED nodes into local components (union-find) and
//! reports `(name, local_root_name)` labels plus `(owned_name, ghost_name)`
//! boundary edges. The coordinator builds ONE global union-find over node names
//! — unioning each label and each boundary edge — and assigns dense component
//! ids ordered by each component's minimum node name.
//!
//! The acceptance invariant: a graph with TWO components must yield exactly two
//! distinct component ids, with all nodes in each component sharing one id. We
//! force a genuine cross-shard component A (a 12-node chain whose distinct names
//! hash to vShards spanning all three nodes) plus a separate small component B.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// All `(node_id, component_id)` rows from a `GRAPH ALGO WCC` result (columns
/// `node_id` text, `component_id` int).
async fn wcc_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<(String, i64)> {
    let msgs = client.simple_query(sql).await.expect("simple_query WCC");
    let mut out = Vec::new();
    for m in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            let node = r.get("node_id").unwrap_or("").to_string();
            let comp: i64 = r.get("component_id").unwrap_or("0").parse().unwrap_or(0);
            out.push((node, comp));
        }
    }
    out
}

/// Distributed WCC from ANY coordinator must split a two-component graph into
/// exactly two components, proving cross-shard component stitching is correct:
/// (i) exactly 15 rows (12 in A + 3 in B);
/// (ii) all A nodes share ONE component id;
/// (iii) all B nodes share ONE component id;
/// (iv) the two ids differ.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn wcc_two_components_cross_node() {
    // 2 cores/node: WCC connectivity must be stitched across BOTH cores of each
    // node AND across nodes — exercising the per-core fan-out and the coordinator
    // global union-find over cross-core + cross-node boundary edges.
    let cluster = TestCluster::spawn_three_with_cores(2)
        .await
        .expect("3-node 2-core cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION gwcc_xnode")
        .await
        .expect("CREATE COLLECTION gwcc_xnode");

    wait_for(
        "all 3 nodes see gwcc_xnode",
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

    // Component A: chain a_0 -> a_1 -> ... -> a_11 (12 nodes). The distinct a_i
    // names hash to vShards spanning all three nodes, so the chain's edges cross
    // shard boundaries repeatedly — exactly the case the WCC coordinator must
    // stitch together.
    const A_LEN: usize = 12;
    for i in 0..A_LEN - 1 {
        let src = format!("a_{i}");
        let dst = format!("a_{}", i + 1);
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gwcc_xnode' FROM '{src}' TO '{dst}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {e}"));
    }

    // Component B: separate chain z_0 -> z_1 -> z_2 (3 nodes).
    for i in 0..2 {
        let src = format!("z_{i}");
        let dst = format!("z_{}", i + 1);
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'gwcc_xnode' FROM '{src}' TO '{dst}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {e}"));
    }

    let a_nodes: HashSet<String> = (0..A_LEN).map(|i| format!("a_{i}")).collect();
    let b_nodes: HashSet<String> = (0..3).map(|i| format!("z_{i}")).collect();
    let all_nodes: HashSet<String> = a_nodes.union(&b_nodes).cloned().collect();

    const WCC_SQL: &str = "GRAPH ALGO WCC ON 'gwcc_xnode'";

    // Wait until EVERY node sees all 15 nodes in the WCC result — edges applied +
    // replicated AND each node's routing view converged so a non-owning
    // coordinator runs the contraction round across all owning shards.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all 15 wcc nodes"),
            Duration::from_secs(30),
            Duration::from_millis(200),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = wcc_rows(&node.client, WCC_SQL).await;
                        let names: HashSet<String> = rows.into_iter().map(|(n, _)| n).collect();
                        names == all_nodes
                    })
                })
            },
        )
        .await;
    }

    // From every node: exactly two components, correctly partitioned.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = wcc_rows(&node.client, WCC_SQL).await;
        let map: HashMap<String, i64> = rows.iter().cloned().collect();

        // (i) Exactly 15 rows, one per distinct node.
        assert_eq!(
            rows.len(),
            15,
            "node {idx} WCC must return exactly 15 rows; got {rows:?}"
        );
        let names: HashSet<String> = rows.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(
            names, all_nodes,
            "node {idx} WCC must cover exactly the inserted nodes; got {rows:?}"
        );

        // (ii) All A nodes share ONE component id.
        let a_ids: HashSet<i64> = a_nodes.iter().map(|n| map[n]).collect();
        assert_eq!(
            a_ids.len(),
            1,
            "node {idx}: all A nodes must share one component id; got {a_ids:?}"
        );

        // (iii) All B nodes share ONE component id.
        let b_ids: HashSet<i64> = b_nodes.iter().map(|n| map[n]).collect();
        assert_eq!(
            b_ids.len(),
            1,
            "node {idx}: all B nodes must share one component id; got {b_ids:?}"
        );

        // (iv) The two component ids differ.
        let a_id = *a_ids.iter().next().expect("A id");
        let b_id = *b_ids.iter().next().expect("B id");
        assert_ne!(
            a_id, b_id,
            "node {idx}: components A and B must have distinct ids"
        );
    }

    cluster.shutdown().await;
}
