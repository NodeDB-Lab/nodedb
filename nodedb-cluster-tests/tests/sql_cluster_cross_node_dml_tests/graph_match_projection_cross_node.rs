// SPDX-License-Identifier: BUSL-1.1

//! A `MATCH` property projection across the nodes of a cluster.
//!
//! The pattern scatters to every owner and the coordinator merges the rows.
//! The coordinator drops a row another shard also returned, and it must keep
//! two matches that project to equal rows. Every node answers one row per
//! edge for `RETURN a.id`, and `LIMIT` bounds the merged answer.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

const COLL: &str = "gproj_cross";
const EDGES: usize = 8;

async fn row_count(client: &tokio_postgres::Client, sql: &str) -> usize {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

async fn wait_every_node_counts(cluster: &TestCluster, sql: &str, expected: usize) {
    for idx in 0..cluster.nodes.len() {
        wait_for(
            &format!("node {idx} answers {expected} rows of {sql}"),
            Duration::from_secs(30),
            Duration::from_millis(100),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        row_count(&cluster.nodes[idx].client, sql).await == expected
                    })
                })
            },
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_property_projection_answers_one_row_per_match_from_every_node() {
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
    for i in 0..EDGES {
        client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN '{COLL}' FROM 'src_{i}' TO 'dst_{i}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert edge {i}: {e}"));
    }

    let whole = format!("MATCH (a)-[:K]->(b) IN '{COLL}' RETURN a, b");
    wait_every_node_counts(&cluster, &whole, EDGES).await;

    // The nodes hold no documents, so every `a.id` reads NULL and every
    // projected row is equal: one row per edge still.
    let projected = format!("MATCH (a)-[:K]->(b) IN '{COLL}' RETURN a.id");
    wait_every_node_counts(&cluster, &projected, EDGES).await;

    let limited = format!("MATCH (a)-[:K]->(b) IN '{COLL}' RETURN a.id LIMIT 3");
    wait_every_node_counts(&cluster, &limited, 3).await;

    cluster.shutdown().await;
}
