//! DDL replication correctness matrix.
//!
//! For every DDL variant that flows through the replicated metadata
//! path, this file tests:
//!
//! 1. Execute DDL on the leader → visible on every follower.
//! 2. Execute the inverse DDL → removal visible on every node.
//! 3. `IF NOT EXISTS` / `IF EXISTS` branches handled without error.
//!
//! Uses the 3-node `TestCluster` harness from `common/cluster_harness`.

mod common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};

// ── Collection ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ddl_create_drop_collection_replicates() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION ddl_test_coll")
        .await
        .expect("create");
    wait_for(
        "collection visible on all nodes",
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

    cluster
        .exec_ddl_on_any_leader("DROP COLLECTION ddl_test_coll")
        .await
        .expect("drop");
    wait_for(
        "collection removed on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() == 0)
        },
    )
    .await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ddl_create_collection_if_not_exists() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION ine_coll")
        .await
        .expect("first create");
    wait_for(
        "collection visible",
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
    // Second CREATE IF NOT EXISTS must succeed without error.
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION IF NOT EXISTS ine_coll")
        .await
        .expect("if not exists must not error");
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ddl_drop_collection_if_exists_missing() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    // DROP IF EXISTS on a nonexistent collection must succeed.
    cluster
        .exec_ddl_on_any_leader("DROP COLLECTION IF EXISTS no_such_coll")
        .await
        .expect("if exists on missing must not error");
    cluster.shutdown().await;
}

// ── Sequence ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ddl_create_drop_sequence_replicates() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE SEQUENCE ddl_test_seq START 1")
        .await
        .expect("create seq");
    wait_for(
        "sequence visible on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_sequence(1, "ddl_test_seq"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP SEQUENCE ddl_test_seq")
        .await
        .expect("drop seq");
    wait_for(
        "sequence removed on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.sequence_count(1) == 0),
    )
    .await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ddl_create_sequence_if_not_exists() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE SEQUENCE ine_seq START 1")
        .await
        .expect("first create");
    wait_for(
        "seq visible",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_sequence(1, "ine_seq")),
    )
    .await;
    cluster
        .exec_ddl_on_any_leader("CREATE SEQUENCE IF NOT EXISTS ine_seq START 1")
        .await
        .expect("if not exists must not error");
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ddl_drop_sequence_if_exists_missing() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader("DROP SEQUENCE IF EXISTS no_such_seq")
        .await
        .expect("if exists on missing must not error");
    cluster.shutdown().await;
}

// ── Trigger ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ddl_create_drop_trigger_replicates() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION trig_coll")
        .await
        .expect("create coll for trigger");
    wait_for(
        "coll visible",
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

    cluster
        .exec_ddl_on_any_leader(
            "CREATE TRIGGER ddl_test_trig AFTER INSERT ON trig_coll FOR EACH ROW BEGIN RETURN 1; END",
        )
        .await
        .expect("create trigger");
    wait_for(
        "trigger visible on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_trigger(1, "ddl_test_trig"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP TRIGGER ddl_test_trig ON trig_coll")
        .await
        .expect("drop trigger");
    wait_for(
        "trigger removed on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_trigger(1, "ddl_test_trig"))
        },
    )
    .await;
    cluster.shutdown().await;
}

// ── Schedule ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ddl_create_drop_schedule_replicates() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE SCHEDULE ddl_test_sched CRON '0 0 * * *' AS BEGIN RETURN 1; END",
        )
        .await
        .expect("create schedule");
    wait_for(
        "schedule visible on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_schedule(1, "ddl_test_sched"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP SCHEDULE ddl_test_sched")
        .await
        .expect("drop schedule");
    wait_for(
        "schedule removed on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_schedule(1, "ddl_test_sched"))
        },
    )
    .await;
    cluster.shutdown().await;
}
