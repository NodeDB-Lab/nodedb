// SPDX-License-Identifier: BUSL-1.1

//! Cross-node REVERSE (in-direction) graph traversal after edge DELETE — the
//! end-to-end proof that edge DELETE dual-homes over the Calvin cross-shard
//! write loop, symmetrically with edge INSERT.
//!
//! A graph edge `src -> dst` is stored dual-homed: the forward EDGES entry +
//! CSR adjacency on `from_key(src)` (the source's vShard), and the
//! REVERSE_EDGES entry on `from_key(dst)` (the destination's vShard). A REVERSE
//! traversal starts at the DESTINATION and scatters to `from_key(dst)`, so it
//! only finds an edge if the reverse copy lives on the destination's shard.
//!
//! Edge INSERT already dual-homes cross-shard edges through Calvin (proven by
//! `graph_traverse_reverse_cross_node`). Edge DELETE must do the same: a
//! single-homed delete writes the forward tombstone on `from_key(src)` but
//! leaves the REVERSE_EDGES entry on `from_key(dst)` live — so a reverse
//! traversal from the hub keeps finding the DELETED source. Dual-homing the
//! delete through Calvin applies the tombstone on BOTH `from_key(src)` and
//! `from_key(dst)`, so the reverse copy on the hub's shard is removed too.
//!
//! This test inserts many `src_i -> hub` edges (a meaningful fraction
//! cross-shard), deletes a subset, and asserts that a reverse traversal from
//! `hub` no longer reaches the deleted sources from ANY coordinator node while
//! still reaching the surviving ones — a direct regression guard against a
//! single-homed delete that orphans the reverse index.

use std::collections::HashSet;
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Run a `GRAPH TRAVERSE`-style query that yields a single `result` JSON row.
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

/// Many distinct sources point at one `hub`; after deleting half of the
/// `src_i -> hub` edges, a reverse (in-direction) traversal from `hub` must
/// reach EXACTLY the surviving sources from ANY coordinator node — never a
/// deleted one.
///
/// With enough distinct source names, several `src_i -> hub` edges are
/// cross-shard (`from_key(src_i) != from_key(hub)`). Each such DELETE must be
/// dual-homed through Calvin so the reverse copy on `hub`'s home shard is
/// tombstoned; otherwise the in-traversal from `hub` still finds the deleted
/// source.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn graph_reverse_traverse_omits_deleted_sources_from_any_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION graph_del_xnode")
        .await
        .expect("CREATE COLLECTION graph_del_xnode");

    wait_for(
        "all 3 nodes see graph_del_xnode",
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

    // src_0..src_11 -> hub (12 in-edges into one hub). Distinct source names
    // spread across vShards spanning all three nodes, so a meaningful fraction
    // of these edges are cross-shard and must dual-home via Calvin.
    const SOURCES: usize = 12;
    for i in 0..SOURCES {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'graph_del_xnode' FROM 'src_{i}' TO 'hub' TYPE 'l'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert edge src_{i} -> hub: {e}"));
    }

    // Confirm all 12 in-edges are reverse-reachable from `hub` on every node
    // before deleting — establishes the dual-homed baseline so the deletion
    // assertion below isolates the DELETE path.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} reverse-reaches all {SOURCES} sources of hub"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let v = query_graph_json(
                            &node.client,
                            "GRAPH TRAVERSE IN 'graph_del_xnode' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in",
                        )
                        .await;
                        traversed_node_ids(&v).len() > SOURCES
                    })
                })
            },
        )
        .await;
    }

    // Delete the first half of the edges (src_0..src_5 -> hub). A
    // cross-shard delete that single-homes on from_key(src) would orphan the
    // reverse copy on hub's shard, leaving the deleted source reverse-reachable.
    const DELETED: usize = SOURCES / 2;
    for i in 0..DELETED {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH DELETE EDGE IN 'graph_del_xnode' FROM 'src_{i}' TO 'hub' TYPE 'l'"
            ))
            .await
            .unwrap_or_else(|e| panic!("delete edge src_{i} -> hub: {e}"));
    }

    // Expected surviving reverse-reachable set (ids), including the start node
    // `hub`: only src_6..src_11 remain.
    let expected: HashSet<String> = std::iter::once("hub".to_string())
        .chain((DELETED..SOURCES).map(|i| format!("src_{i}")))
        .collect();

    // Wait until every node's reverse traversal from `hub` converges to exactly
    // the surviving set — tombstones applied + dual-homed + replicated AND each
    // node's routing view converged.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!(
                "node {idx} reverse set converges to {} survivors",
                expected.len()
            ),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let v = query_graph_json(
                            &node.client,
                            "GRAPH TRAVERSE IN 'graph_del_xnode' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in",
                        )
                        .await;
                        let set: HashSet<String> = traversed_node_ids(&v).into_iter().collect();
                        set == expected
                    })
                })
            },
        )
        .await;
    }

    // From every node: exactly the survivors, no deleted source, no dup, no drop.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let v = query_graph_json(
            &node.client,
            "GRAPH TRAVERSE IN 'graph_del_xnode' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in",
        )
        .await;
        let ids = traversed_node_ids(&v);
        let set: HashSet<String> = ids.iter().cloned().collect();
        assert_eq!(
            ids.len(),
            set.len(),
            "node {idx} reverse traverse returned duplicate node ids: {ids:?}"
        );
        assert_eq!(
            set, expected,
            "node {idx} reverse traverse must reach exactly hub + survivors src_{DELETED}..src_{SOURCES} \
             (no deleted source src_0..src_{DELETED}); got {ids:?}"
        );
    }

    cluster.shutdown().await;
}
