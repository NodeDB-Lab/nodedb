// SPDX-License-Identifier: BUSL-1.1

//! Cross-node routed Calvin-submit test (Cv1).
//!
//! Reproduces and fixes the sequencer-leader-routing gap: a cross-shard Calvin
//! write submitted on a NON-sequencer-leader coordinator must complete (not time
//! out), because the submit is routed to the sequencer-group leader — the only
//! node whose sequencer service assigns the transaction and whose completion
//! registry receives the replicated completion ack.
//!
//! Steps:
//! 1. Bring up the standard 3-node cluster (`TestCluster::spawn_three`).
//! 2. Create two collections whose names hash to DISTINCT vShards, cluster-wide.
//! 3. Determine the sequencer-group leader and pick a coordinator node that is
//!    NOT the leader.
//! 4. From that non-leader coordinator, run a cross-shard write through Calvin:
//!    `BEGIN; INSERT a; INSERT b; COMMIT` under `cross_shard_txn = 'strict'`.
//!    The COMMIT's multi-shard path builds a `TxClass` and routes the
//!    submit-and-await to the sequencer leader via `submit_calvin_routed`.
//! 5. Assert the transaction SUCCEEDS (completes, not times out).
//! 6. Read both rows back to prove the write actually committed.
//!
//! File name contains "cluster" via the cluster-tests crate so nextest applies
//! the cluster test group serialization.

use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;

mod common;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Find two collection names whose vShard ids differ.
fn two_distinct_vshard_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("calvin_routed_{i}");
        let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
        if let Some((ref fname, fv)) = first {
            if fv != vshard {
                return (fname.clone(), name);
            }
        } else {
            first = Some((name, vshard));
        }
    }
    panic!("could not find two distinct-vshard collections in 512 tries");
}

/// Observed sequencer-group leader id from a node's local Raft status, or `0` if
/// no leader is known yet.
fn sequencer_leader(node: &common::cluster_harness::TestClusterNode) -> u64 {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return 0;
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0)
}

// This exercises a cross-shard *document* write inside an explicit
// `BEGIN; … ; COMMIT` block from a NON-sequencer-leader coordinator. The COMMIT
// flush routes its submit-and-await to the sequencer-group leader via
// `dispatch_tasks_to_calvin` → `submit_calvin_routed` — the same routed
// primitive the autocommit cross-shard path uses — so it completes instead of
// timing out at the completion phase on a non-leader coordinator. This is the
// H1 acceptance test for interactive cross-shard COMMIT under leader routing.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_shard_write_from_non_sequencer_leader_completes() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let (col_a, col_b) = two_distinct_vshard_collections();

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {col_a} (id TEXT PRIMARY KEY, v TEXT)"
        ))
        .await
        .expect("CREATE COLLECTION col_a");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {col_b} (id TEXT PRIMARY KEY, v TEXT)"
        ))
        .await
        .expect("CREATE COLLECTION col_b");

    wait_for(
        "all 3 nodes see both collections",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 2)
        },
    )
    .await;

    // Wait for a stable sequencer-group leader to be visible cluster-wide.
    wait_for(
        "sequencer-group leader elected and visible on every node",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|n| sequencer_leader(n) != 0)
                && cluster
                    .nodes
                    .iter()
                    .all(|n| sequencer_leader(n) == sequencer_leader(&cluster.nodes[0]))
        },
    )
    .await;

    let leader = sequencer_leader(&cluster.nodes[0]);
    assert_ne!(leader, 0, "sequencer leader must be elected");

    // Pick a coordinator node that is NOT the sequencer leader — this is where
    // the original silent-loss bug bit: a submit on a non-leader's local inbox is
    // drained and discarded, so the caller times out at the assignment phase.
    let coordinator = cluster
        .nodes
        .iter()
        .find(|n| n.shared.node_id != leader)
        .expect("a non-sequencer-leader coordinator must exist in a 3-node cluster");

    assert_ne!(
        coordinator.shared.node_id, leader,
        "coordinator must not be the sequencer leader for this test to be meaningful"
    );

    // Enable strict cross-shard mode on the coordinator's session so COMMIT's
    // multi-shard path runs through Calvin.
    coordinator
        .client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    // The cross-shard write: two single-row INSERTs into collections on DISTINCT
    // vShards in one multi-statement transaction. On COMMIT the buffered tasks
    // span two vShards → MultiShard → routed Calvin submit-and-await. Before the
    // fix this would TIME OUT on a non-leader coordinator; with routing it
    // completes.
    let txn_sql = format!(
        "BEGIN; \
         INSERT INTO {col_a} (id, v) VALUES ('k1', 'hello'); \
         INSERT INTO {col_b} (id, v) VALUES ('k2', 'world'); \
         COMMIT"
    );
    coordinator.client.simple_query(&txn_sql).await.expect(
        "cross-shard Calvin write from a non-leader coordinator must complete, not time out",
    );

    // Prove the write committed: read both rows back from the coordinator.
    let count_rows = |msgs: &[tokio_postgres::SimpleQueryMessage]| -> usize {
        msgs.iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count()
    };

    let rows_a = coordinator
        .client
        .simple_query(&format!("SELECT * FROM {col_a}"))
        .await
        .expect("SELECT col_a");
    assert_eq!(
        count_rows(&rows_a),
        1,
        "col_a must have 1 row after the routed Calvin commit"
    );

    let rows_b = coordinator
        .client
        .simple_query(&format!("SELECT * FROM {col_b}"))
        .await
        .expect("SELECT col_b");
    assert_eq!(
        count_rows(&rows_b),
        1,
        "col_b must have 1 row after the routed Calvin commit"
    );

    cluster.shutdown().await;
}
