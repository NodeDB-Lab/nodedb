// SPDX-License-Identifier: BUSL-1.1

//! Cluster BACKUP / RESTORE of graph edges when nodes outnumber the
//! replication factor.
//!
//! An edge lives on the key vShard of each endpoint, `from_key(src)` and
//! `from_key(dst)`, never on its collection's vShard. With 3 nodes and RF 2 the
//! leader of the collection's group does not hold every edge. The backup must
//! still capture each edge once, and the restore must write it to both endpoint
//! homes, so a traversal reaches it from either endpoint on any node.
//!
//! No one node holds every endpoint's PK→surrogate bind either: each endpoint
//! is bound on the group of `from_key(endpoint)`. The backup captures each
//! bind from the source node of its home, so every restored endpoint keeps its
//! source surrogate.
//!
//! The restore target is a fresh cluster with the same RF: it has no live data
//! and a zero tenant write HLC, so no restore guard fires.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use nodedb::types::{DatabaseId, TenantId};
use nodedb_types::CollectionKey;

use crate::common::cluster_harness::shared_steps::{db_detail, drain_backup, push_restore};
use crate::common::cluster_harness::{TestCluster, wait_for, wait_for_async};

const TENANT: u64 = 1;
const RF: usize = 2;
const COLLECTION: &str = "graph_br";
const FAN: usize = 12;

/// The node ids a 1-hop traversal from `start` reaches, including `start`, or
/// `None` while the query fails (the collection not yet visible on the node).
async fn reached(
    client: &tokio_postgres::Client,
    start: &str,
    direction: &str,
) -> Option<HashSet<String>> {
    let sql = format!(
        "GRAPH TRAVERSE IN '{COLLECTION}' FROM '{start}' DEPTH 1 LABEL 'l' DIRECTION {direction}"
    );
    let msgs = client.simple_query(&sql).await.ok()?;
    let raw = msgs.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get("result").map(str::to_string),
        _ => None,
    })?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let ids = value
        .get("nodes")?
        .as_array()?
        .iter()
        .filter_map(|n| n.get("id").and_then(|id| id.as_str()).map(str::to_string))
        .collect();
    Some(ids)
}

/// The surrogate every node of `cluster` that binds `endpoint` binds it to.
/// `None` while no node binds it. Two nodes disagreeing fails the test.
fn cluster_bind(cluster: &TestCluster, endpoint: &str) -> Option<u32> {
    let mut seen: Option<u32> = None;
    for node in &cluster.nodes {
        let bound = node
            .shared
            .surrogate_assigner
            .lookup_bound(
                CollectionKey::from_bare(DatabaseId::DEFAULT, COLLECTION),
                TenantId::new(TENANT),
                endpoint.as_bytes(),
            )
            .expect("surrogate lookup")
            .map(|s| s.as_u32());
        if let Some(s) = bound {
            assert!(
                seen.is_none_or(|prev| prev == s),
                "node {} binds {endpoint} to {s}, another node to {seen:?}",
                node.node_id
            );
            seen = Some(s);
        }
    }
    seen
}

fn set(ids: impl IntoIterator<Item = String>) -> HashSet<String> {
    ids.into_iter().collect()
}

fn sources() -> Vec<String> {
    (0..FAN).map(|i| format!("src_{i}")).collect()
}

fn sinks() -> Vec<String> {
    (0..FAN).map(|i| format!("dst_{i}")).collect()
}

/// Every traversal the test checks: `(start, direction, expected reach)`.
fn expectations() -> Vec<(String, &'static str, HashSet<String>)> {
    let hub = || "hub".to_string();
    let mut out = vec![
        (hub(), "in", set(std::iter::once(hub()).chain(sources()))),
        (hub(), "out", set(std::iter::once(hub()).chain(sinks()))),
    ];
    for src in sources() {
        out.push((src.clone(), "out", set([src, hub()])));
    }
    for dst in sinks() {
        out.push((dst.clone(), "in", set([dst, hub()])));
    }
    out
}

/// Wait until node `idx` answers every traversal in `expectations()` exactly.
async fn wait_all_traversals(cluster: &TestCluster, idx: usize, phase: &str) {
    let checks = expectations();
    let checks = &checks;
    wait_for_async(
        &format!("{phase}: node {idx} reaches every edge from both endpoints"),
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async move {
            for (start, direction, want) in checks {
                let got = reached(&cluster.nodes[idx].client, start, direction).await;
                if got.as_ref() != Some(want) {
                    return false;
                }
            }
            true
        },
    )
    .await;
}

/// Edges into and out of one hub survive BACKUP on a 3-node RF-2 cluster and
/// RESTORE into a fresh 3-node RF-2 cluster: every edge is traversable from
/// both of its endpoints on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn restored_edges_are_traversable_from_both_endpoints_when_nodes_outnumber_rf() {
    let source = TestCluster::spawn_three_with_replication_factor(RF)
        .await
        .expect("source cluster");
    source
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("CREATE COLLECTION");
    wait_for(
        "every source node sees the collection",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            source
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    for src in sources() {
        source.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN '{COLLECTION}' FROM '{src}' TO 'hub' TYPE 'l'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {src} -> hub: {}", db_detail(&e)));
    }
    for dst in sinks() {
        source.nodes[0]
            .client
            .simple_query(&format!(
                "GRAPH INSERT EDGE IN '{COLLECTION}' FROM 'hub' TO '{dst}' TYPE 'l'"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert hub -> {dst}: {}", db_detail(&e)));
    }
    for idx in 0..source.nodes.len() {
        wait_all_traversals(&source, idx, "source").await;
    }
    let endpoints: Vec<String> = std::iter::once("hub".to_string())
        .chain(sources())
        .chain(sinks())
        .collect();
    let source_binds: BTreeMap<String, u32> = endpoints
        .iter()
        .map(|ep| {
            let s = cluster_bind(&source, ep)
                .unwrap_or_else(|| panic!("some source node binds endpoint {ep}"));
            (ep.clone(), s)
        })
        .collect();

    let bytes = drain_backup(&source.nodes[0].client, TENANT).await;
    assert!(!bytes.is_empty(), "backup must produce bytes");
    source.shutdown().await;

    let target = TestCluster::spawn_three_with_replication_factor(RF)
        .await
        .expect("target cluster");
    push_restore(&target.nodes[0].client, TENANT, bytes).await;
    target
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    for idx in 0..target.nodes.len() {
        wait_all_traversals(&target, idx, "target").await;
    }
    for (idx, node) in target.nodes.iter().enumerate() {
        for (start, direction, want) in expectations() {
            let got = reached(&node.client, &start, direction).await;
            assert_eq!(
                got.as_ref(),
                Some(&want),
                "node {idx}: {direction}-traversal from {start} after restore"
            );
        }
    }
    for (endpoint, surrogate) in &source_binds {
        assert_eq!(
            cluster_bind(&target, endpoint),
            Some(*surrogate),
            "restored endpoint {endpoint} keeps its source surrogate"
        );
    }

    target.shutdown().await;
}
