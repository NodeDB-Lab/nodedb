// SPDX-License-Identifier: BUSL-1.1

//! Cross-node `EDGE WHERE` and edge properties on `GRAPH TRAVERSE` and
//! `GRAPH PATH`.
//!
//! Each frontier node expands at the node that owns its key vShard, and the
//! predicate runs on the core that stores the edge. A coordinator that
//! evaluated the predicate against its own partitions, or dropped it on the
//! remote `NeighborsMulti`, reaches a node the predicate rejects or misses
//! one it admits. Destination names are distinct, so the edges spread
//! across vShards spanning nodes.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

async fn result_json(client: &tokio_postgres::Client, sql: &str) -> serde_json::Value {
    let msgs = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
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

fn node_ids(v: &serde_json::Value) -> BTreeSet<String> {
    v["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .map(|n| n["id"].as_str().expect("node id").to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn edge_predicate_and_properties_hold_from_any_coordinator() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION ep_xnode")
        .await
        .expect("CREATE COLLECTION ep_xnode");
    wait_for(
        "all 3 nodes see ep_xnode",
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

    // root -> hot_i (score 9) and root -> cold_i (score 1); each hot_i ->
    // deep_i (score 9) and each cold_i -> lost_i (score 9).
    const FAN: usize = 6;
    let mut edges = Vec::new();
    for i in 0..FAN {
        edges.push(("root".to_string(), format!("hot_{i}"), 9));
        edges.push(("root".to_string(), format!("cold_{i}"), 1));
        edges.push((format!("hot_{i}"), format!("deep_{i}"), 9));
        edges.push((format!("cold_{i}"), format!("lost_{i}"), 9));
    }
    for (src, dst, score) in &edges {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'ep_xnode' FROM '{src}' TO '{dst}' TYPE 'L' \
                 PROPERTIES {{ score: {score} }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert edge {src}->{dst}: {e}"));
    }

    let unfiltered = "GRAPH TRAVERSE IN 'ep_xnode' FROM 'root' DEPTH 2 DIRECTION out";
    let total = 1 + 4 * FAN;
    for idx in 0..cluster.nodes.len() {
        wait_for(
            &format!("node {idx} reaches every seeded node"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        node_ids(&result_json(&cluster.nodes[idx].client, unfiltered).await).len()
                            == total
                    })
                })
            },
        )
        .await;
    }

    let expected: BTreeSet<String> = std::iter::once("root".to_string())
        .chain((0..FAN).flat_map(|i| [format!("hot_{i}"), format!("deep_{i}")]))
        .collect();
    for idx in 0..cluster.nodes.len() {
        let client = &cluster.nodes[idx].client;
        let got = result_json(
            client,
            "GRAPH TRAVERSE IN 'ep_xnode' FROM 'root' DEPTH 2 DIRECTION out EDGE WHERE score > 5",
        )
        .await;
        assert_eq!(node_ids(&got), expected, "node {idx}: {got}");
        for edge in got["edges"].as_array().expect("edges array") {
            assert_eq!(
                edge["properties"],
                serde_json::json!({ "score": 9 }),
                "node {idx}: every crossed edge carries score 9: {got}"
            );
        }

        // Backward expansion tests the stored edge `hot_0 -> deep_0`.
        let path = result_json(
            client,
            "GRAPH PATH IN 'ep_xnode' FROM 'root' TO 'deep_0' MAX_DEPTH 4 EDGE WHERE score > 5",
        )
        .await;
        assert_eq!(
            path,
            serde_json::json!(["root", "hot_0", "deep_0"]),
            "node {idx}"
        );
        let blocked = result_json(
            client,
            "GRAPH PATH IN 'ep_xnode' FROM 'root' TO 'lost_0' MAX_DEPTH 4 EDGE WHERE score > 5",
        )
        .await;
        assert_eq!(blocked, serde_json::json!([]), "node {idx}");
    }

    cluster.shutdown().await;
}
