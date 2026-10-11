// SPDX-License-Identifier: BUSL-1.1

//! A committed transaction's message reaches its topic exactly once across a
//! leader kill.
//!
//! A SYNC trigger body publishes one message per inserted row. The message
//! rides the insert's redo record, so every replica of the source
//! collection's group holds it once the insert commits. Only the node that
//! holds that group's leader lease delivers, from a replicated cursor.
//!
//! - `a_publish_committed_before_a_leader_kill_is_delivered_once` parks the
//!   leader's delivery before it sends anything, then kills the leader. The
//!   new lease holder delivers every message from the cursor.
//! - `a_publish_sent_before_a_leader_kill_is_not_appended_twice` holds the
//!   leader's delivery until every message is committed, lets it send them
//!   all in one pass, parks it before it commits the cursor, then kills it. The new lease holder
//!   sends the messages again from the old cursor, and the topic appends none of them twice.

#![cfg(feature = "failpoints")]

use crate::common;
use common::cluster_harness::shared_steps::{kill_node, leader_index_of};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use nodedb::event::cdc::CdcOffset;
use nodedb_test_support::fail_point::{FailAction, FailGuard};
use nodedb_types::DatabaseId;

const TOPIC: &str = "committed_failover_feed";
const SRC: &str = "committed_failover_src";
const TENANT: u64 = 1;
const ROWS: [&str; 3] = ["r1", "r2", "r3"];

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// The payloads `node`'s topic log holds, with how many times each appears.
fn topic_payloads(node: &TestClusterNode) -> BTreeMap<String, usize> {
    let messages = node
        .shared
        .credentials
        .catalog()
        .load_ep_topic_messages(DatabaseId::DEFAULT, TENANT, TOPIC)
        .unwrap_or_else(|e| panic!("node {}: read topic: {e}", node.node_id));
    let mut payloads = BTreeMap::new();
    for message in messages {
        let row = ROWS
            .iter()
            .find(|row| message.payload.contains(*row))
            .unwrap_or_else(|| panic!("unexpected topic message {:?}", message.payload));
        *payloads.entry((*row).to_owned()).or_insert(0) += 1;
    }
    payloads
}

/// Every row's message, once.
fn every_message_once() -> BTreeMap<String, usize> {
    ROWS.iter().map(|row| ((*row).to_owned(), 1)).collect()
}

/// How many committed messages `node` holds for delivery.
fn held(node: &TestClusterNode) -> usize {
    let Some(ledgers) = node.shared.sink_ledgers.get() else {
        return 0;
    };
    let partitions = ledgers.publishes.partitions().unwrap_or_default();
    partitions
        .into_iter()
        .map(|partition| {
            ledgers
                .publishes
                .held_after(partition, CdcOffset::ZERO, 1_000)
                .map(|held| held.len())
                .unwrap_or(0)
        })
        .sum()
}

/// A three-node cluster with the topic, the source collection, and its SYNC
/// publishing trigger on every node.
async fn cluster_with_publishing_trigger() -> TestCluster {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    wait_for("every data group has a leader", CONVERGE, STEP, || {
        cluster.nodes[0]
            .all_group_leaders()
            .iter()
            .all(|(_, leader)| *leader != 0)
    })
    .await;
    for ddl in [
        format!("CREATE TOPIC {TOPIC}"),
        format!(
            "CREATE COLLECTION {SRC} (id TEXT PRIMARY KEY, v BIGINT) \
             WITH (engine='document_strict')"
        ),
        format!(
            "CREATE SYNC TRIGGER {SRC}_pub AFTER INSERT ON {SRC} FOR EACH ROW \
             BEGIN PUBLISH TO {TOPIC} NEW.id; END"
        ),
    ] {
        cluster
            .exec_ddl_on_any_leader(&ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    wait_for("every node registers the topic", CONVERGE, STEP, || {
        cluster.nodes.iter().all(|node| {
            node.shared
                .ep_topic_registry
                .get(DatabaseId::DEFAULT, TENANT, TOPIC)
                .is_some()
        })
    })
    .await;
    cluster
}

/// Insert every row through `node`: each commits one message.
async fn insert_rows(node: &TestClusterNode) {
    for (n, row) in ROWS.iter().enumerate() {
        node.exec(&format!("INSERT INTO {SRC} (id, v) VALUES ('{row}', {n})"))
            .await
            .unwrap_or_else(|e| panic!("insert {row}: {e}"));
    }
}

/// Wait until `marker` exists: the gate's task reached it.
async fn wait_parked(marker: &Path, what: &str) {
    wait_for(what, CONVERGE, Duration::from_millis(20), || {
        marker.exists()
    })
    .await;
}

/// The survivors' topic logs converge on every message once, and the
/// delivery cursor passes every message on every survivor.
async fn survivors_hold_every_message_once(cluster: &TestCluster) {
    wait_for(
        "every survivor's topic holds every message",
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .all(|node| topic_payloads(node).len() == ROWS.len())
        },
    )
    .await;
    wait_for(
        "the delivery cursor passes every message on every survivor",
        CONVERGE,
        STEP,
        || cluster.nodes.iter().all(|node| held(node) == 0),
    )
    .await;
    // A delivery pass runs every 100ms. Several passes later the topic still
    // holds each message once.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    for node in &cluster.nodes {
        assert_eq!(
            topic_payloads(node),
            every_message_once(),
            "node {} holds a message twice or misses one",
            node.node_id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_publish_committed_before_a_leader_kill_is_delivered_once() {
    let mut cluster = cluster_with_publishing_trigger().await;
    let leader = leader_index_of(&cluster, SRC);
    let leader_id = cluster.nodes[leader].node_id;
    let writer = (leader + 1) % cluster.nodes.len();

    let gate_dir = tempfile::tempdir().expect("gate directory");
    // Never created: the leader stays parked until it dies.
    let release = gate_dir.path().join("release");
    let parked = gate_dir.path().join("release.parked");
    let _gate = FailGuard::for_node(
        leader_id,
        "publish::before_delivery",
        FailAction::WaitForFile(release),
    );
    wait_parked(&parked, "the leader's delivery parks").await;

    insert_rows(&cluster.nodes[writer]).await;
    wait_for(
        "every replica holds every committed message",
        CONVERGE,
        STEP,
        || cluster.nodes.iter().all(|node| held(node) == ROWS.len()),
    )
    .await;
    for node in &cluster.nodes {
        assert!(
            topic_payloads(node).is_empty(),
            "node {}: nothing is delivered while the lease holder is parked",
            node.node_id
        );
    }

    kill_node(&mut cluster, leader).await;
    survivors_hold_every_message_once(&cluster).await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_publish_sent_before_a_leader_kill_is_not_appended_twice() {
    let mut cluster = cluster_with_publishing_trigger().await;
    let leader = leader_index_of(&cluster, SRC);
    let leader_id = cluster.nodes[leader].node_id;
    let writer = (leader + 1) % cluster.nodes.len();

    // Hold the leader's delivery until every message is committed, so one
    // pass sends them all.
    let hold_dir = tempfile::tempdir().expect("gate directory");
    let hold_release = hold_dir.path().join("release");
    let hold_parked = hold_dir.path().join("release.parked");
    let _hold = FailGuard::for_node(
        leader_id,
        "publish::before_delivery",
        FailAction::WaitForFile(hold_release.clone()),
    );
    wait_parked(&hold_parked, "the leader's delivery parks").await;
    insert_rows(&cluster.nodes[writer]).await;
    wait_for(
        "every replica holds every committed message",
        CONVERGE,
        STEP,
        || cluster.nodes.iter().all(|node| held(node) == ROWS.len()),
    )
    .await;

    // Then let it send, and park it before its cursor commit. The release
    // file of the second gate is never created: the leader dies parked.
    let commit_dir = tempfile::tempdir().expect("gate directory");
    let commit_parked = commit_dir.path().join("release.parked");
    let _commit = FailGuard::for_node(
        leader_id,
        "publish::before_cursor_commit",
        FailAction::WaitForFile(commit_dir.path().join("release")),
    );
    std::fs::write(&hold_release, b"release").expect("release the delivery");
    wait_parked(&commit_parked, "the leader parks before its cursor commit").await;
    wait_for(
        "every node's topic holds every message the leader sent",
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .all(|node| topic_payloads(node) == every_message_once())
        },
    )
    .await;
    for node in &cluster.nodes {
        assert!(
            held(node) > 0,
            "node {}: the cursor never passed the sent messages",
            node.node_id
        );
    }

    kill_node(&mut cluster, leader).await;
    survivors_hold_every_message_once(&cluster).await;
    cluster.shutdown().await;
}
