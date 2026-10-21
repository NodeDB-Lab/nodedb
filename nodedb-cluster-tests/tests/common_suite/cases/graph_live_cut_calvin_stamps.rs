// SPDX-License-Identifier: BUSL-1.1

//! Every edge write takes a Calvin ordinal, so a live WCC run reads the same
//! graph on every node.
//!
//! A live distributed graph run reads at its Calvin cut marker's cut, which
//! covers only Calvin-stamped edge versions. An edge write stamped from a
//! node's own clock would sit above that cut on some replica and below it on
//! another. Edges are written here through every SQL path: autocommit put,
//! autocommit delete, and a session transaction's COMMIT. Right after the
//! last acknowledgement, each node runs a live WCC and must see every
//! acknowledged edge, grouped the same way. Each replica then stores every
//! edge version under the same system time: the replicas applied one
//! replicated ordinal, not three local clocks.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nodedb_types::TenantId;

use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

const COLL: &str = "glcut";
const TENANT: u64 = 1;

/// `node_id -> component_id` of a live WCC run through `node`.
async fn components(node: &TestClusterNode) -> BTreeMap<String, String> {
    let sql = format!("GRAPH ALGO WCC ON '{COLL}'");
    let messages = node
        .client
        .simple_query(&sql)
        .await
        .unwrap_or_else(|e| panic!("node {}: {sql}: {e}", node.node_id));
    messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get("node_id").unwrap_or("").to_string(),
                row.get("component_id").unwrap_or("").to_string(),
            )),
            _ => None,
        })
        .collect()
}

/// The components of `assignment` as sets of graph nodes, ignoring ids.
fn partition(assignment: &BTreeMap<String, String>) -> BTreeSet<BTreeSet<String>> {
    let mut by_component: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    for (node, component) in assignment {
        by_component
            .entry(component.as_str())
            .or_default()
            .insert(node.clone());
    }
    by_component.into_values().collect()
}

/// Every edge version key of `COLL` the cores of `node` store.
async fn stored_edge_keys(node: &TestClusterNode) -> BTreeSet<String> {
    let prefix = format!("{COLL}\x00");
    node.stored_tenant_snapshots(TenantId::new(TENANT))
        .await
        .into_iter()
        .flat_map(|snap| snap.edges.into_iter().map(|(key, _)| key))
        .filter(|key| key.starts_with(&prefix))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_live_wcc_reads_every_edge_write_path_alike_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("CREATE COLLECTION");
    wait_for(
        "every node sees the collection",
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

    let writer = &cluster.nodes[1];
    // Autocommit puts, and a put the same session deletes again.
    for (src, dst) in [("a0", "a1"), ("a1", "a2"), ("a2", "a3")] {
        writer
            .exec(&format!(
                "GRAPH INSERT EDGE IN '{COLL}' FROM '{src}' TO '{dst}' TYPE 'K'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {src} -> {dst}: {e}"));
    }
    writer
        .exec(&format!(
            "GRAPH DELETE EDGE IN '{COLL}' FROM 'a2' TO 'a3' TYPE 'K'"
        ))
        .await
        .expect("delete a2 -> a3");
    // A session transaction's COMMIT.
    writer
        .client
        .simple_query(&format!(
            "BEGIN; \
             GRAPH INSERT EDGE IN '{COLL}' FROM 'b0' TO 'b1' TYPE 'K'; \
             GRAPH INSERT EDGE IN '{COLL}' FROM 'b1' TO 'b2' TYPE 'K'; \
             COMMIT"
        ))
        .await
        .expect("COMMIT the edge transaction");

    // No wait: the run's cut marker is placed after every acknowledgement.
    let first = components(&cluster.nodes[0]).await;
    let chain_a: BTreeSet<String> = ["a0", "a1", "a2"].iter().map(|s| s.to_string()).collect();
    let chain_b: BTreeSet<String> = ["b0", "b1", "b2"].iter().map(|s| s.to_string()).collect();
    let groups = partition(&first);
    assert!(
        groups.contains(&chain_a),
        "the autocommit chain is one component: {groups:?}"
    );
    assert!(
        groups.contains(&chain_b),
        "the committed chain is one component: {groups:?}"
    );
    for node in &cluster.nodes[1..] {
        assert_eq!(
            partition(&components(node).await),
            groups,
            "node {} reads the same graph",
            node.node_id
        );
    }

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let stored = stored_edge_keys(&cluster.nodes[0]).await;
    assert!(
        stored.len() >= 5,
        "every put leaves a version: {}",
        stored.len()
    );
    for node in &cluster.nodes[1..] {
        assert_eq!(
            stored_edge_keys(node).await,
            stored,
            "node {} stores every edge version under the same system time",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
