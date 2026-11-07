// SPDX-License-Identifier: BUSL-1.1

//! The count of a two-slice Calvin request whose coordinator hosts only one
//! participant.
//!
//! With a replication factor of 1, each data group lives on one node. The
//! session runs on the sequencer leader, which coordinates every Calvin
//! transaction. One collection lives on that node and the other on another
//! node. So the coordinator's sidecar holds one slice's answer, and only the
//! completion acks hold both. The statement's count is their sum.
//!
//! File name contains "calvin" so nextest applies the cluster test-group
//! serialization.

use std::time::Duration;

use crate::common;
use common::cluster_harness::shared_steps::{fail_stopped, sequencer_admitted};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use nodedb_test_support::native_harness::send_sql;
use nodedb_types::protocol::NativeResponse;

use super::ts_native_ingest::{assert_native_ok, native_session};

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(50);
/// Candidate collection names tried before giving up.
const MAX_TRIES: u32 = 512;

/// The node that alone replicates the data group of `collection`, once
/// placement converged to one replica.
async fn owner_of(cluster: &TestCluster, collection: &str) -> u64 {
    let group_id = cluster.nodes[0]
        .group_id_for_collection(collection)
        .unwrap_or_else(|| panic!("the data group of {collection}"));
    wait_for(
        &format!("exactly one node replicates group {group_id}"),
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .filter(|node| node.replicates_data_group(group_id))
                .count()
                == 1
        },
    )
    .await;
    cluster
        .nodes
        .iter()
        .find(|node| node.replicates_data_group(group_id))
        .map(|node| node.node_id)
        .unwrap_or_else(|| panic!("the replica of group {group_id}"))
}

/// A collection name `{prefix}_{i}` whose owner matches `on_coordinator`.
async fn collection_placed(
    cluster: &TestCluster,
    coordinator: u64,
    prefix: &str,
    on_coordinator: bool,
) -> String {
    for i in 0..MAX_TRIES {
        let name = format!("{prefix}_{i}");
        if (owner_of(cluster, &name).await == coordinator) == on_coordinator {
            return name;
        }
    }
    panic!("no collection under {prefix} fits in {MAX_TRIES} tries");
}

async fn create(cluster: &TestCluster, name: &str) {
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

async fn seed(coordinator: &TestClusterNode, name: &str) {
    coordinator
        .client
        .simple_query(&format!(
            "INSERT INTO {name} (id, v) VALUES ('r1', 'a'), ('r2', 'b'), ('r3', 'c')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {name}: {e}"));
}

fn update(collection: &str, id: &str) -> String {
    format!("UPDATE {collection} SET v = 'x' WHERE id = '{id}'")
}

fn assert_count(response: &NativeResponse, expected: u64, what: &str) {
    assert_native_ok(response, what);
    assert_eq!(response.rows_affected, Some(expected), "{what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_coordinator_of_one_slice_answers_the_count_of_both() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("spawn 3-node cluster");
    wait_for(
        "every node sees one sequencer leader",
        CONVERGE,
        STEP,
        || {
            let leader = cluster.nodes[0].sequencer_leader();
            leader != 0 && cluster.nodes.iter().all(|n| n.sequencer_leader() == leader)
        },
    )
    .await;
    let leader_id = cluster.nodes[0].sequencer_leader();
    let coordinator = cluster
        .nodes
        .iter()
        .find(|node| node.node_id == leader_id)
        .expect("the sequencer leader is a cluster node");

    let local = collection_placed(&cluster, leader_id, "partial_local", true).await;
    let remote = collection_placed(&cluster, leader_id, "partial_remote", false).await;
    create(&cluster, &local).await;
    create(&cluster, &remote).await;
    wait_for("every node sees both collections", CONVERGE, STEP, || {
        cluster
            .nodes
            .iter()
            .all(|n| n.cached_collection_count() >= 2)
    })
    .await;
    seed(coordinator, &local).await;
    seed(coordinator, &remote).await;

    let mut session = native_session(coordinator).await;
    let admitted_before = sequencer_admitted(coordinator);

    assert_count(
        &send_sql(
            &mut session,
            2,
            &format!("{}; {}", update(&local, "r1"), update(&remote, "r1")),
        )
        .await,
        2,
        "two real writes",
    );
    assert_count(
        &send_sql(
            &mut session,
            3,
            &format!("{}; {}", update(&local, "r2"), update(&remote, "absent")),
        )
        .await,
        1,
        "a remote write that matches nothing",
    );
    assert_count(
        &send_sql(
            &mut session,
            4,
            &format!("{}; {}", update(&local, "absent"), update(&remote, "r2")),
        )
        .await,
        1,
        "a local write that matches nothing",
    );
    assert!(
        sequencer_admitted(coordinator) > admitted_before,
        "the requests commit through Calvin"
    );

    for node in &cluster.nodes {
        assert!(
            !fail_stopped(node),
            "node {} fail-stopped a core",
            node.node_id
        );
    }
    cluster.shutdown().await;
}
