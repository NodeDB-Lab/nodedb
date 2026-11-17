// SPDX-License-Identifier: BUSL-1.1

//! Cross-node REVERSE traversal over IMPLICIT graph edges — the end-to-end
//! proof that a document carrying `_from`/`_to` dual-homes its edge across
//! shards exactly like an explicit `GRAPH INSERT EDGE`.
//!
//! A schemaless document with the reserved `_from`/`_to`/`_type` fields is
//! mirrored as a graph edge. That extraction must not run on the data plane
//! homing the edge by the DOCUMENT's vShard — that would land a cross-shard
//! implicit edge (`from_key(_from) != from_key(_to)`) only on the document's
//! shard, missing it from a reverse traversal starting at the destination.
//! Extraction runs on the control plane: each endpoint's home vShard +
//! canonical surrogate is resolved and the edge routes through the same
//! single-home/Calvin dual-home path as an explicit edge. This test inserts
//! many `src_i -> hub` edges as
//! plain documents (a meaningful fraction cross-shard) and asserts a reverse
//! (in-direction) traversal from `hub` reaches EVERY source from ANY node —
//! which only holds if the implicit edge's reverse copy landed on `hub`'s home
//! shard.

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

/// Many distinct sources point at one `hub` via IMPLICIT edges (documents with
/// `_from`/`_to`); a reverse (in-direction) traversal from `hub` must reach
/// EVERY source from ANY coordinator node.
///
/// With enough distinct source names, several `src_i -> hub` edges are
/// cross-shard (`from_key(src_i) != from_key(hub)`). Each such implicit edge is
/// dual-homed atomically through Calvin so its reverse copy lands on `hub`'s
/// home shard; the in-traversal from `hub` then finds the full source set.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn implicit_edge_reverse_traverse_reaches_all_sources_from_any_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION graph_impl_xnode WITH (engine='document_schemaless')",
        )
        .await
        .expect("CREATE COLLECTION graph_impl_xnode");

    wait_for(
        "all 3 nodes see graph_impl_xnode",
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

    // src_0..src_11 -> hub as IMPLICIT edges: each is a plain document carrying
    // `_from`/`_to`/`_type`. Distinct source names spread across vShards
    // spanning all three nodes, so a meaningful fraction of these edges are
    // cross-shard (src home != hub home) and must dual-home via Calvin. A
    // cross-shard implicit edge that did not dual-home would be reverse-invisible
    // from hub.
    const SOURCES: usize = 12;
    for i in 0..SOURCES {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO graph_impl_xnode {{ id: 'edge_{i}', _from: 'src_{i}', _to: 'hub', _type: 'l' }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert implicit edge src_{i} -> hub: {e}"));
    }

    // Wait until a 1-hop REVERSE traversal from `hub` reaches all 12 sources
    // from EVERY node — implicit edges extracted on the control plane, dual-homed
    // + replicated, AND each node's routing view converged.
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
                            "GRAPH TRAVERSE IN 'graph_impl_xnode' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in",
                        )
                        .await;
                        // hub + all sources => SOURCES + 1 distinct ids.
                        traversed_node_ids(&v).len() > SOURCES
                    })
                })
            },
        )
        .await;
    }

    // Expected reverse-reachable set (ids), including the start node `hub`.
    let expected: HashSet<String> = std::iter::once("hub".to_string())
        .chain((0..SOURCES).map(|i| format!("src_{i}")))
        .collect();

    // From every node: complete + distinct (no dup, no drop).
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let v = query_graph_json(
            &node.client,
            "GRAPH TRAVERSE IN 'graph_impl_xnode' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in",
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
            "node {idx} reverse traverse over implicit edges must reach exactly hub + {SOURCES} sources; got {ids:?}"
        );
    }

    cluster.shutdown().await;
}
