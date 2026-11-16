// SPDX-License-Identifier: BUSL-1.1

//! Cross-node graph traversal correctness.
//!
//! Fills a coverage gap: there were no cluster tests for `GRAPH TRAVERSE` /
//! neighbor lookups. Graph edges are keyed by source node (`from_key(src)`) so
//! they distribute across vShards/nodes, and each core's CSR is PARTITIONED
//! (it holds only its owned nodes' edges, not the full graph). A traversal
//! coordinated from a node that does not own the start node must therefore
//! expand that frontier node at its OWNING shard, not just on local cores —
//! otherwise it silently returns a partial reachable set. This test confirms a
//! traversal returns the COMPLETE, DISTINCT reachable set from ANY coordinator
//! node — no duplication, no missing nodes.
//!
//! `GRAPH TRAVERSE` returns a single row whose `result` column is a JSON object
//! `{"nodes":[{"id","depth"}...],"edges":[...]}`; we parse it and assert on the
//! set of reached node ids.

use std::collections::HashSet;
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Run a `GRAPH TRAVERSE`/`GRAPH NEIGHBORS`-style query that yields a single
/// `result` JSON row, and return the parsed JSON value.
async fn query_graph_json(client: &tokio_postgres::Client, sql: &str) -> serde_json::Value {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    let row = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("graph query returned no result row");
    let raw = row.get("result").expect("result column present");
    serde_json::from_str(raw).expect("result column is valid JSON")
}

/// The set of reached node ids from a `GRAPH TRAVERSE` result JSON.
fn traversed_node_ids(v: &serde_json::Value) -> Vec<String> {
    v.get("nodes")
        .and_then(|n| n.as_array())
        .expect("traverse result has a nodes array")
        .iter()
        .map(|n| {
            n.get("id")
                .and_then(|id| id.as_str())
                .expect("node has string id")
                .to_string()
        })
        .collect()
}

/// A `GRAPH TRAVERSE` from any coordinator node must return the COMPLETE,
/// DISTINCT reachable set — not duplicated by the vShards/cores it fans out to,
/// and not truncated to the coordinator's local data.
///
/// Each hop partitions the incoming frontier by `from_key(node)` owner
/// (resolved against live Raft leadership) and expands each frontier node at
/// its owning node via a typed `NeighborsMulti` dispatch, so a traversal
/// coordinated from a non-owning node still reaches the full set.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn graph_traverse_returns_complete_distinct_set_from_any_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION graph_xnode")
        .await
        .expect("CREATE COLLECTION graph_xnode");

    wait_for(
        "all 3 nodes see graph_xnode",
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

    // root -> leaf_0..leaf_9 (10 one-hop neighbors). Edges are keyed by source
    // node; distinct destination names spread across vShards spanning nodes.
    const NEIGHBORS: usize = 10;
    for i in 0..NEIGHBORS {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'graph_xnode' FROM 'root' TO 'leaf_{i}' TYPE 'l'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert edge leaf_{i}: {e}"));
    }
    // A second hop off leaf_3, so a 2-hop traversal must cross to whatever
    // vShard owns leaf_3 (distinct from root's).
    for d in 0..2 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'graph_xnode' FROM 'leaf_3' TO 'deep_{d}' TYPE 'l'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert edge deep_{d}: {e}"));
    }

    // Wait until a 1-hop traversal reaches all 10 leaves (+ root) from EVERY
    // node — edges applied + replicated AND each node's routing view converged
    // so a non-owning coordinator correctly scatters the start frontier to its
    // owner. (The gateway path tolerates stale routing via NotLeader retry; the
    // graph path relies on a converged routing view here.)
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} reaches all {NEIGHBORS} root neighbors"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let v = query_graph_json(
                            &node.client,
                            "GRAPH TRAVERSE IN 'graph_xnode' FROM 'root' DEPTH 1 LABEL 'l' DIRECTION out",
                        )
                        .await;
                        traversed_node_ids(&v).len() > NEIGHBORS
                    })
                })
            },
        )
        .await;
    }

    // Expected reachable sets (ids), including the start node `root`.
    let expected_1hop: HashSet<String> = std::iter::once("root".to_string())
        .chain((0..NEIGHBORS).map(|i| format!("leaf_{i}")))
        .collect();
    let expected_2hop: HashSet<String> = expected_1hop
        .iter()
        .cloned()
        .chain((0..2).map(|d| format!("deep_{d}")))
        .collect();

    // From BOTH node 0 and node 1: complete + distinct (no dup, no drop).
    for (idx, node) in cluster.nodes.iter().take(2).enumerate() {
        let v1 = query_graph_json(
            &node.client,
            "GRAPH TRAVERSE IN 'graph_xnode' FROM 'root' DEPTH 1 LABEL 'l' DIRECTION out",
        )
        .await;
        let ids1 = traversed_node_ids(&v1);
        let set1: HashSet<String> = ids1.iter().cloned().collect();
        assert_eq!(
            ids1.len(),
            set1.len(),
            "node {idx} 1-hop traverse returned duplicate node ids: {ids1:?}"
        );
        assert_eq!(
            set1, expected_1hop,
            "node {idx} 1-hop traverse must reach exactly root + 10 leaves; got {ids1:?}"
        );

        let v2 = query_graph_json(
            &node.client,
            "GRAPH TRAVERSE IN 'graph_xnode' FROM 'root' DEPTH 2 LABEL 'l' DIRECTION out",
        )
        .await;
        let ids2 = traversed_node_ids(&v2);
        let set2: HashSet<String> = ids2.iter().cloned().collect();
        assert_eq!(
            ids2.len(),
            set2.len(),
            "node {idx} 2-hop traverse returned duplicate node ids: {ids2:?}"
        );
        assert_eq!(
            set2, expected_2hop,
            "node {idx} 2-hop traverse must reach root + 10 leaves + 2 deep; got {ids2:?}"
        );
    }

    cluster.shutdown().await;
}
