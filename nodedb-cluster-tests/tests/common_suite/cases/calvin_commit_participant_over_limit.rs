// SPDX-License-Identifier: BUSL-1.1

//! The COMMIT of a block whose Calvin group committed answers OK, whatever
//! the participants report for the block's `RETURNING` statements.
//!
//! Each statement of a block answers when it runs: its count, or its
//! `RETURNING` rows. The COMMIT returns no rows. It reads only the
//! timeseries install counts of the group's completion acks.
//!
//! - With a replication factor of 1 and a small `max_query_result_bytes`,
//!   the client's session runs on the sequencer leader and its block writes
//!   a collection hosted on another node. One `INSERT ... RETURNING *`
//!   inserts a row larger than the limit, so the participant's completion
//!   ack reports `OverLimit` for it. The COMMIT answers OK and the rows are
//!   stored, in strict and in best-effort mode.
//! - A strict block runs `INSERT ... RETURNING` on two vShards. Both
//!   participants report rows. The COMMIT answers OK and both rows are
//!   stored.
//!
//! A COMMIT that answered an error here would tell the client to retry a
//! block that applied, and the retry would apply it twice.
//!
//! File name contains "calvin" so nextest applies the cluster test-group
//! serialization.

use std::time::Duration;

use crate::common;
use common::cluster_harness::{
    TestCluster, TestClusterNode, is_no_serving_leader, wait_for, wait_for_async,
};

use super::calvin_multishard_fixture::data_rows;
use super::vshard_names::distinct_vshard_collections;

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(50);
/// Candidate collection names tried before giving up.
const MAX_TRIES: u32 = 512;
/// The result limit every node runs. Far below the default.
const RESULT_LIMIT: u64 = 16 * 1024;
/// Bytes of padding in the large row. The row alone passes `RESULT_LIMIT`.
const ROW_PAD: usize = 20 * 1024;

fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn exec(node: &TestClusterNode, sql: &str) {
    node.client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {}", pg_detail(&e)));
}

/// The node that alone replicates the data group of `collection`, once
/// placement converged to one replica.
async fn owner_of<'a>(cluster: &'a TestCluster, collection: &str) -> &'a TestClusterNode {
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
        .unwrap_or_else(|| panic!("the replica of group {group_id}"))
}

/// A collection name `{prefix}_{i}` whose data group lives on a node other
/// than `coordinator`.
async fn collection_off(cluster: &TestCluster, coordinator: u64, prefix: &str) -> String {
    for i in 0..MAX_TRIES {
        let name = format!("{prefix}_{i}");
        if owner_of(cluster, &name).await.node_id != coordinator {
            return name;
        }
    }
    panic!("no collection under {prefix} lives off node {coordinator} in {MAX_TRIES} tries");
}

/// Spawn the cluster and return it with the sequencer leader's node index.
async fn spawn_with_small_limit() -> (TestCluster, usize) {
    let cluster = TestCluster::spawn_three_with_result_limit(1, RESULT_LIMIT)
        .await
        .expect("spawn 3-node cluster with a small result limit");
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
        .position(|node| node.node_id == leader_id)
        .expect("the sequencer leader is a cluster node");
    (cluster, coordinator)
}

/// Create `names` as schemaless document collections and wait until every
/// node sees them.
async fn create_collections(cluster: &TestCluster, names: [&String; 2]) {
    for name in names {
        cluster
            .exec_ddl_on_any_leader(&format!(
                "CREATE COLLECTION {name} WITH (engine='document_schemaless')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    wait_for("every node sees both collections", CONVERGE, STEP, || {
        cluster
            .nodes
            .iter()
            .all(|n| n.cached_collection_count() >= 2)
    })
    .await;
}

/// Run `COMMIT` on `node`. It must answer OK.
async fn commit_answers_ok(node: &TestClusterNode) {
    if let Err(e) = node.client.simple_query("COMMIT").await {
        panic!(
            "the COMMIT of a group that applied answers OK, got {}",
            pg_detail(&e)
        );
    }
}

/// Wait until `SELECT id FROM <name>` returns one row on `node`, for each
/// of `names`. A projection of the id keeps the read under the small limit.
async fn one_row_each(node: &TestClusterNode, names: [&String; 2]) {
    for name in names {
        let sql = format!("SELECT id FROM {name}");
        let sql = sql.as_str();
        wait_for_async(
            &format!("`{sql}` returns 1 row on the coordinator"),
            CONVERGE,
            STEP,
            || async move {
                match node.client.simple_query(sql).await {
                    Ok(messages) => data_rows(&messages) == 1,
                    Err(e) if is_no_serving_leader(&e) => false,
                    Err(e) => panic!("`{sql}`: {}", pg_detail(&e)),
                }
            },
        )
        .await;
    }
}

/// Run one cross-shard block under `mode` and check the COMMIT answers OK
/// and the rows land.
async fn commit_with_over_limit_rows(mode: &str, prefix: &str) {
    let (cluster, coordinator) = spawn_with_small_limit().await;
    let coordinator = &cluster.nodes[coordinator];

    let returning = collection_off(&cluster, coordinator.node_id, &format!("{prefix}_ret")).await;
    let (_, other) = distinct_vshard_collections(&returning, &format!("{prefix}_other"));
    create_collections(&cluster, [&returning, &other]).await;

    exec(coordinator, &format!("SET cross_shard_txn = '{mode}'")).await;
    exec(coordinator, "BEGIN").await;
    let pad = "x".repeat(ROW_PAD);
    exec(
        coordinator,
        &format!("INSERT INTO {returning} (id, pad) VALUES ('big_0', '{pad}') RETURNING *"),
    )
    .await;
    exec(
        coordinator,
        &format!("INSERT INTO {other} (id, n) VALUES ('other_0', 1)"),
    )
    .await;
    commit_answers_ok(coordinator).await;
    one_row_each(coordinator, [&returning, &other]).await;

    // The row is over the limit: a read of all of it is refused.
    let whole = coordinator
        .client
        .simple_query(&format!("SELECT * FROM {returning}"))
        .await;
    match whole {
        Err(e) => assert!(
            pg_detail(&e).contains("max_query_result_bytes"),
            "a read past the limit names it, got {}",
            pg_detail(&e)
        ),
        Ok(_) => panic!("a read of a row past the result limit is refused"),
    }
    cluster.shutdown().await;
}

/// A strict block commits one Calvin batch. The ack of the remote
/// participant reports rows past the limit, and the COMMIT answers OK.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn strict_commit_answers_ok_when_a_participant_report_passes_the_result_limit() {
    commit_with_over_limit_rows("strict", "over_limit_strict").await;
}

/// A best-effort block commits one Calvin batch per vShard. The ack of the
/// remote group reports rows past the limit, and the COMMIT answers OK.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn best_effort_commit_answers_ok_when_a_group_report_passes_the_result_limit() {
    commit_with_over_limit_rows("best_effort_non_atomic", "over_limit_best_effort").await;
}

/// A strict block runs `INSERT ... RETURNING` on two vShards. Both
/// participants report rows, and the COMMIT answers OK.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn strict_commit_answers_ok_when_two_participants_report_rows() {
    let (cluster, coordinator) = spawn_with_small_limit().await;
    let coordinator = &cluster.nodes[coordinator];

    let (first, second) = distinct_vshard_collections("two_ret_a", "two_ret_b");
    create_collections(&cluster, [&first, &second]).await;

    exec(coordinator, "SET cross_shard_txn = 'strict'").await;
    exec(coordinator, "BEGIN").await;
    for (name, id) in [(&first, "a_0"), (&second, "b_0")] {
        exec(
            coordinator,
            &format!("INSERT INTO {name} (id, n) VALUES ('{id}', 1) RETURNING *"),
        )
        .await;
    }
    commit_answers_ok(coordinator).await;
    one_row_each(coordinator, [&first, &second]).await;
    cluster.shutdown().await;
}
