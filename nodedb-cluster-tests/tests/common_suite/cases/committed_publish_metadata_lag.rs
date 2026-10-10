// SPDX-License-Identifier: BUSL-1.1

//! A committed transaction's message waits for a lagging catalog and is
//! never dropped for it.
//!
//! Node B leads the source collection's data group, so it holds the lease
//! that delivers the collection's committed messages. B's metadata apply is
//! held. The topic is created, and a SYNC trigger body publishes to it from
//! an insert on node A, while B's catalog does not know the topic yet. B's
//! Calvin scheduler holds the transaction until B's catalog reaches the
//! metadata floor A planned it at, so the insert waits for B. B delivers
//! nothing and drops nothing while held. Once released, B's catalog reaches
//! the topic, the insert commits, and B delivers the message exactly once.

#![cfg(feature = "failpoints")]

use crate::common;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use nodedb::control::cluster::metadata_applier::METADATA_APPLY_HOLD_POINT;
use nodedb::event::cdc::CdcOffset;
use nodedb_test_support::fail_point::{FailAction, FailGuard};
use nodedb_types::DatabaseId;

const TOPIC: &str = "metadata_lag_feed";
const SRC: &str = "metadata_lag_src";
const TENANT: u64 = 1;

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// How many messages `node`'s topic log holds.
fn topic_messages(node: &TestClusterNode) -> usize {
    node.shared
        .credentials
        .catalog()
        .load_ep_topic_messages(DatabaseId::DEFAULT, TENANT, TOPIC)
        .map(|messages| messages.len())
        .unwrap_or(0)
}

/// How many committed messages `node` holds for delivery.
fn held(node: &TestClusterNode) -> usize {
    let Some(ledgers) = node.shared.sink_ledgers.get() else {
        return 0;
    };
    ledgers
        .publishes
        .partitions()
        .unwrap_or_default()
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

/// Committed messages `node` counted as dropped for a missing topic.
fn dropped(node: &TestClusterNode) -> u64 {
    node.shared.system_metrics.as_ref().map_or(0, |metrics| {
        metrics.committed_publishes_dropped.load(Ordering::Relaxed)
    })
}

fn knows_topic(node: &TestClusterNode) -> bool {
    node.shared
        .ep_topic_registry
        .get(DatabaseId::DEFAULT, TENANT, TOPIC)
        .is_some()
}

/// Run `sql` on `node`, retrying while the cluster settles.
async fn exec_on(node: &TestClusterNode, sql: &str) {
    let deadline = Instant::now() + CONVERGE;
    loop {
        match node.exec(sql).await {
            Ok(()) => return,
            Err(error) if Instant::now() < deadline => {
                tracing::debug!(%error, sql, "statement not accepted yet; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("{sql}: {error}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_lagging_lease_holder_waits_for_the_topic_and_delivers_once() {
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

    // B leads the source group and delivers its committed messages. A is
    // another node, where the statements under test run.
    let probe = &cluster.nodes[0];
    let group = probe
        .group_id_for_collection(SRC)
        .unwrap_or_else(|| panic!("no group mapping for {SRC}"));
    let b_id = probe
        .all_group_leaders()
        .into_iter()
        .find_map(|(id, leader)| (id == group).then_some(leader))
        .unwrap_or(0);
    let b = cluster
        .nodes
        .iter()
        .position(|node| node.node_id == b_id)
        .unwrap_or_else(|| panic!("no live node leads {SRC}'s group {group}"));
    let a = (b + 1) % cluster.nodes.len();

    // Hold B's metadata apply, then create the topic through A.
    let gate_dir = tempfile::tempdir().expect("gate directory");
    let release = gate_dir.path().join("release");
    let parked = gate_dir.path().join("release.parked");
    let _hold = FailGuard::for_node(
        b_id,
        METADATA_APPLY_HOLD_POINT,
        FailAction::WaitForFile(release.clone()),
    );
    exec_on(&cluster.nodes[a], &format!("CREATE TOPIC {TOPIC}")).await;
    wait_for(
        "every node but B knows the topic, and B's metadata apply is held",
        CONVERGE,
        STEP,
        || {
            parked.exists()
                && cluster
                    .nodes
                    .iter()
                    .enumerate()
                    .all(|(i, node)| knows_topic(node) == (i != b))
        },
    )
    .await;

    // A's trigger body finds the topic. The transaction carries A's metadata
    // floor, which covers the topic, so B's Calvin scheduler holds it until
    // B's catalog reaches that floor: the insert completes only once B is
    // released.
    let insert = format!("INSERT INTO {SRC} (id, v) VALUES ('r1', 1)");
    let (inserted, ()) = tokio::join!(cluster.nodes[a].exec(&insert), async {
        // Several delivery passes later, B still delivered and dropped nothing.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!knows_topic(&cluster.nodes[b]), "B's catalog still lags");
        for node in &cluster.nodes {
            assert_eq!(
                topic_messages(node),
                0,
                "node {}: nothing is delivered while the lease holder lags",
                node.node_id
            );
        }
        assert_eq!(
            dropped(&cluster.nodes[b]),
            0,
            "B drops nothing while it lags"
        );

        // Release B. Its catalog reaches the topic, B runs the held
        // transaction, and B delivers the message.
        std::fs::write(&release, b"release").expect("release B's metadata apply");
    });
    inserted.unwrap_or_else(|e| panic!("{insert}: {e}"));
    wait_for(
        "every node's topic holds the message and every ledger drains",
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .all(|node| topic_messages(node) == 1 && held(node) == 0)
        },
    )
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    for node in &cluster.nodes {
        assert_eq!(
            topic_messages(node),
            1,
            "node {} holds the message a number of times other than once",
            node.node_id
        );
        assert_eq!(
            dropped(node),
            0,
            "node {} dropped the message",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
