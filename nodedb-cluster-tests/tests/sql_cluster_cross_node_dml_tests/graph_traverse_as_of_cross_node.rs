// SPDX-License-Identifier: BUSL-1.1

//! Historical `GRAPH TRAVERSE ... AS OF SYSTEM TIME` across nodes.
//!
//! Edges are keyed by source node, so the hops of a 2-hop walk expand on the
//! nodes that own each frontier node's vShard. A historical walk pins one
//! cut on the coordinator. Every owner waits until it installed every edge
//! version at or below that cut before it expands a hop, so the walk answers
//! the full graph at the cut from any coordinator:
//!
//! - right after the writes, before any wait for replication, and
//! - after later deletes and inserts changed the current graph.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::common::cluster_harness::{TestCluster, wait_for};

const COLL: &str = "gtrav_asof";
const MIDS: usize = 8;

/// The node ids a `GRAPH TRAVERSE` result row names.
async fn traverse_ids(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
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
        .unwrap_or_else(|| panic!("{sql}: no result row"));
    let raw = row.get("result").expect("result column present");
    let v: serde_json::Value = serde_json::from_str(raw).expect("result column is valid JSON");
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

/// Wait until a traverse from every node reaches exactly `expected`.
async fn wait_every_node_reaches(cluster: &TestCluster, sql: &str, expected: &HashSet<String>) {
    for idx in 0..cluster.nodes.len() {
        wait_for(
            &format!("node {idx} reaches {} nodes", expected.len()),
            Duration::from_secs(30),
            Duration::from_millis(100),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let ids = traverse_ids(&cluster.nodes[idx].client, sql).await;
                        &ids.into_iter().collect::<HashSet<_>>() == expected
                    })
                })
            },
        )
        .await;
    }
}

/// Assert a traverse from every node returns exactly `expected`, each id once.
async fn assert_every_node_reaches(cluster: &TestCluster, sql: &str, expected: &HashSet<String>) {
    for idx in 0..cluster.nodes.len() {
        let ids = traverse_ids(&cluster.nodes[idx].client, sql).await;
        let set: HashSet<String> = ids.iter().cloned().collect();
        assert_eq!(
            ids.len(),
            set.len(),
            "node {idx}: duplicate node ids in {sql}: {ids:?}"
        );
        assert_eq!(&set, expected, "node {idx}: {sql} returned {ids:?}");
    }
}

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

fn ids(names: impl IntoIterator<Item = String>) -> HashSet<String> {
    names.into_iter().collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn historical_traverse_reads_the_full_cut_from_any_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("CREATE COLLECTION");
    wait_for(
        "all 3 nodes see the collection",
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

    // Warm-up: every node's routing view converges, so a coordinator sends
    // each frontier node to its owner.
    for i in 0..10 {
        exec(
            client,
            &format!("GRAPH INSERT EDGE IN '{COLL}' FROM 'warm' TO 'w_{i}' TYPE 'l'"),
        )
        .await;
    }
    let warm = ids(std::iter::once("warm".to_string()).chain((0..10).map(|i| format!("w_{i}"))));
    wait_every_node_reaches(
        &cluster,
        &format!("GRAPH TRAVERSE IN '{COLL}' FROM 'warm' DEPTH 1 LABEL 'l' DIRECTION out"),
        &warm,
    )
    .await;

    // root -> m_i -> l_i_0, l_i_1. Each m_i expands on its own owner.
    for i in 0..MIDS {
        exec(
            client,
            &format!("GRAPH INSERT EDGE IN '{COLL}' FROM 'root' TO 'm_{i}' TYPE 'l'"),
        )
        .await;
        for j in 0..2 {
            exec(
                client,
                &format!("GRAPH INSERT EDGE IN '{COLL}' FROM 'm_{i}' TO 'l_{i}_{j}' TYPE 'l'"),
            )
            .await;
        }
    }
    let at_cut = ids(std::iter::once("root".to_string())
        .chain((0..MIDS).map(|i| format!("m_{i}")))
        .chain((0..MIDS).flat_map(|i| (0..2).map(move |j| format!("l_{i}_{j}")))));

    // The cut follows every acknowledged write. No wait for replication: a
    // remote owner that has not installed the writes yet waits for the cut
    // before it expands its hop.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cut = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let historical = format!(
        "GRAPH TRAVERSE IN '{COLL}' FROM 'root' DEPTH 2 LABEL 'l' DIRECTION out \
         AS OF SYSTEM TIME {cut}"
    );
    assert_every_node_reaches(&cluster, &historical, &at_cut).await;

    // Change the current graph: cut one hop-1 edge and one hop-2 edge, add a
    // new hop-1 node.
    exec(
        client,
        &format!("GRAPH DELETE EDGE IN '{COLL}' FROM 'root' TO 'm_0' TYPE 'l'"),
    )
    .await;
    exec(
        client,
        &format!("GRAPH DELETE EDGE IN '{COLL}' FROM 'm_1' TO 'l_1_0' TYPE 'l'"),
    )
    .await;
    exec(
        client,
        &format!("GRAPH INSERT EDGE IN '{COLL}' FROM 'root' TO 'late' TYPE 'l'"),
    )
    .await;
    let mut current = at_cut.clone();
    for gone in ["m_0", "l_0_0", "l_0_1", "l_1_0"] {
        current.remove(gone);
    }
    current.insert("late".to_string());
    wait_every_node_reaches(
        &cluster,
        &format!("GRAPH TRAVERSE IN '{COLL}' FROM 'root' DEPTH 2 LABEL 'l' DIRECTION out"),
        &current,
    )
    .await;

    // The walk at the cut still reads the graph before the changes.
    assert_every_node_reaches(&cluster, &historical, &at_cut).await;

    cluster.shutdown().await;
}
