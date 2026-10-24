// SPDX-License-Identifier: BUSL-1.1

//! Historical label-filtered `MATCH ... AS OF SYSTEM TIME` across nodes.
//!
//! A label write runs as a Calvin transaction. Its redo record carries the
//! transaction's ordinal, so every replica writes the node's label version at
//! that one ordinal. A historical read pins one cut, and every owner waits
//! until it installed every version at or below it. So a label-filtered MATCH
//! at the cut answers the labels at the cut, the same from every node, after
//! later label changes too.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::common::cluster_harness::{TestCluster, wait_for};

const COLL: &str = "glabel_asof";
const SOURCES: usize = 8;

type Pairs = HashSet<(String, String)>;

/// The `(a, b)` rows of a label-filtered `MATCH (a:L)-[:K]->(b)`.
async fn pairs(client: &tokio_postgres::Client, sql: &str) -> Vec<(String, String)> {
    let msgs = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some((
                r.get("a").unwrap_or("").to_string(),
                r.get("b").unwrap_or("").to_string(),
            )),
            _ => None,
        })
        .collect()
}

fn match_sql(label: &str, as_of: Option<i64>) -> String {
    let cut = as_of.map_or(String::new(), |ms| format!(" AS OF SYSTEM TIME {ms}"));
    format!("MATCH (a:{label})-[:K]->(b) IN '{COLL}'{cut} RETURN a, b")
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

fn source_pairs(sources: impl IntoIterator<Item = usize>) -> Pairs {
    sources
        .into_iter()
        .map(|i| (format!("src_{i}"), format!("dst_{i}")))
        .collect()
}

/// Wait until `sql` from every node returns exactly `expected`.
async fn wait_every_node_reads(cluster: &TestCluster, sql: &str, expected: &Pairs) {
    for idx in 0..cluster.nodes.len() {
        wait_for(
            &format!("node {idx} reads {} rows of {sql}", expected.len()),
            Duration::from_secs(30),
            Duration::from_millis(100),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let rows = pairs(&cluster.nodes[idx].client, sql).await;
                        &rows.into_iter().collect::<Pairs>() == expected
                    })
                })
            },
        )
        .await;
    }
}

/// Assert `sql` from every node returns exactly `expected`, each row once.
async fn assert_every_node_reads(cluster: &TestCluster, sql: &str, expected: &Pairs) {
    for idx in 0..cluster.nodes.len() {
        let rows = pairs(&cluster.nodes[idx].client, sql).await;
        let set: Pairs = rows.iter().cloned().collect();
        assert_eq!(
            rows.len(),
            set.len(),
            "node {idx}: duplicate rows in {sql}: {rows:?}"
        );
        assert_eq!(&set, expected, "node {idx}: {sql} returned {rows:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn historical_label_match_reads_the_cut_from_every_node() {
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

    // Distinct source names spread across vShards on all three nodes, so the
    // label versions live on every node.
    let client = &cluster.nodes[0].client;
    for i in 0..SOURCES {
        exec(
            client,
            &format!("GRAPH INSERT EDGE IN '{COLL}' FROM 'src_{i}' TO 'dst_{i}' TYPE 'K'"),
        )
        .await;
        exec(client, &format!("GRAPH LABEL 'src_{i}' AS 'Hot'")).await;
    }
    let all = source_pairs(0..SOURCES);
    wait_every_node_reads(&cluster, &match_sql("Hot", None), &all).await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let cut = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Move every even source from `Hot` to `Cold`.
    let even: Vec<usize> = (0..SOURCES).filter(|i| i % 2 == 0).collect();
    for i in &even {
        exec(client, &format!("GRAPH UNLABEL 'src_{i}' AS 'Hot'")).await;
        exec(client, &format!("GRAPH LABEL 'src_{i}' AS 'Cold'")).await;
    }
    let odd = source_pairs((0..SOURCES).filter(|i| i % 2 == 1));
    let cold = source_pairs(even.iter().copied());
    wait_every_node_reads(&cluster, &match_sql("Hot", None), &odd).await;
    wait_every_node_reads(&cluster, &match_sql("Cold", None), &cold).await;

    // At the cut every source was `Hot` and none was `Cold`, on every node.
    assert_every_node_reads(&cluster, &match_sql("Hot", Some(cut)), &all).await;
    assert_every_node_reads(&cluster, &match_sql("Cold", Some(cut)), &Pairs::new()).await;

    cluster.shutdown().await;
}
