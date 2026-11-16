// SPDX-License-Identifier: BUSL-1.1

//! Cross-node REVERSE (in-direction) graph traversal correctness — the
//! end-to-end proof of edge dual-homing (F1b-dualhome) over the Calvin
//! cross-shard write loop (Cv1).
//!
//! Graph edges are keyed by source node (`from_key(src)`), so a forward
//! traversal finds them at the source's owning shard. A REVERSE traversal,
//! however, starts at the DESTINATION and scatters to `from_key(dst)` — so it
//! only finds an edge if that edge was ALSO homed on the destination's vShard.
//!
//! Single-homing (F1a) writes a cross-shard edge only on `from_key(src)`, so a
//! reverse traversal from `dst` whose home differs silently misses it. Dual-home
//! (dh-2) writes the edge ATOMICALLY on BOTH `from_key(src)` and `from_key(dst)`
//! via a Calvin two-participant transaction whose submit-and-await is routed to
//! the sequencer-group leader (Cv1) — so the reverse edge lands on the
//! destination's home and the in-traversal finds it.
//!
//! This test is therefore a dual proof:
//!   1. cross-shard `GRAPH INSERT EDGE` actually COMPLETES (the Calvin write loop
//!      is operational from any coordinator — otherwise the insert times out at
//!      sequencer assignment); and
//!   2. a reverse traversal from a hub reaches the COMPLETE, DISTINCT set of its
//!      sources from ANY coordinator node.

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

/// Many distinct sources all point to one `hub`; a reverse (in-direction)
/// traversal from `hub` must reach EVERY source from ANY coordinator node.
///
/// With enough distinct source names, several `src_i -> hub` edges are
/// cross-shard (`from_key(src_i) != from_key(hub)`). Each such edge is
/// dual-homed atomically through Calvin so its reverse copy lands on `hub`'s
/// home shard; the in-traversal from `hub` then finds the full source set.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn graph_reverse_traverse_reaches_all_sources_from_any_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION graph_rev_xnode")
        .await
        .expect("CREATE COLLECTION graph_rev_xnode");

    wait_for(
        "all 3 nodes see graph_rev_xnode",
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
    // spread across vShards spanning all three nodes, so a meaningful fraction of
    // these edges are cross-shard (src home != hub home) and must dual-home via
    // Calvin. A cross-shard insert that does not complete would error here.
    const SOURCES: usize = 12;
    for i in 0..SOURCES {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN 'graph_rev_xnode' FROM 'src_{i}' TO 'hub' TYPE 'l'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert edge src_{i} -> hub: {e}"));
    }

    // Wait until a 1-hop REVERSE traversal from `hub` reaches all 12 sources from
    // EVERY node — edges applied + dual-homed + replicated AND each node's routing
    // view converged so a non-owning coordinator scatters the start frontier to
    // hub's owner correctly.
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
                            "GRAPH TRAVERSE IN 'graph_rev_xnode' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in",
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
            "GRAPH TRAVERSE IN 'graph_rev_xnode' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in",
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
            "node {idx} reverse traverse must reach exactly hub + {SOURCES} sources; got {ids:?}"
        );
    }

    cluster.shutdown().await;
}
