// SPDX-License-Identifier: BUSL-1.1

//! A RESTORE whose only data is a node label raises the restore mark.
//!
//! Node labels belong to no collection: a node's labels home on
//! `VShardId::from_key(node)`. The backup here holds an empty collection and
//! one labeled node, so the restore re-issues exactly one batch: the label.
//! Every replica of the node's home group raises the tenant's restore mark
//! for that batch, so a later RESTORE's staleness guard sees the write. After
//! every node restarts, each replica still holds the mark and the label.

use std::time::Duration;

use nodedb::control::state::tenant_marks::MarkSite;
use nodedb_types::TenantId;

use crate::common::cluster_harness::shared_steps::{drain_backup, group_of_key, push_restore};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

const TENANT: u64 = 1;
const NODE: &str = "rlm_carol";
const LABEL: &str = "Admin";

/// Whether `node` holds a restore mark of `TENANT` in `group`.
fn holds_restore_mark(node: &TestClusterNode, group: u64) -> bool {
    node.shared
        .tenant_marks
        .get_all(group, TENANT)
        .iter()
        .any(|mark| mark.site == MarkSite::Restore && mark.restore_id != 0)
}

/// Whether `node`'s own cores hold `NODE` labeled `LABEL`.
async fn holds_label(node: &TestClusterNode) -> bool {
    node.stored_tenant_snapshots(TenantId::new(TENANT))
        .await
        .iter()
        .any(|snap| {
            snap.node_labels
                .iter()
                .any(|(_, _, labeled, labels)| labeled == NODE && labels.iter().any(|l| l == LABEL))
        })
}

async fn assert_restored(cluster: &TestCluster, group: u64, stage: &str) {
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    for node in &cluster.nodes {
        assert!(
            holds_restore_mark(node, group),
            "{stage}, node {}: the label batch raised the restore mark in group {group}",
            node.node_id
        );
        assert!(
            holds_label(node).await,
            "{stage}, node {}: the restored label is stored",
            node.node_id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_labels_only_restore_raises_the_restore_mark() {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    source
        .exec_ddl_on_any_leader("CREATE COLLECTION rlm_people WITH (engine='document_schemaless')")
        .await
        .expect("CREATE COLLECTION");
    source.nodes[0]
        .exec(&format!("GRAPH LABEL '{NODE}' AS '{LABEL}'"))
        .await
        .expect("GRAPH LABEL");
    source
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let backup = drain_backup(&source.nodes[0].client, TENANT).await;
    source.shutdown().await;

    let target = TestCluster::spawn_three().await.expect("target cluster");
    let group = group_of_key(&target.nodes[0], NODE);
    for node in &target.nodes {
        assert!(
            !holds_restore_mark(node, group),
            "node {}: a fresh cluster holds no restore mark",
            node.node_id
        );
    }
    push_restore(&target.nodes[0].client, TENANT, backup).await;
    wait_for(
        "every replica raises the restore mark",
        Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            target
                .nodes
                .iter()
                .all(|node| holds_restore_mark(node, group))
        },
    )
    .await;
    assert_restored(&target, group, "after the restore").await;

    let target = target.restart_all().await.expect("restart every node");
    assert_restored(&target, group, "after a restart").await;

    target.shutdown().await;
}
