// SPDX-License-Identifier: BUSL-1.1

//! DDL replication tests: collections, sequences, triggers, procedures, schedules, change streams.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};
use nodedb_types::DatabaseId;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn create_on_any_node_is_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    // Every node starts with an empty replicated cache.
    for node in &cluster.nodes {
        assert_eq!(node.cached_collection_count(), 0);
    }

    // CREATE proposed on whichever node is the metadata-group leader.
    let leader_idx = cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION users")
        .await
        .expect("create collection");
    eprintln!("CREATE accepted by node {}", leader_idx + 1);

    // Every node's replicated cache must see the new collection.
    wait_for(
        "all 3 nodes see the replicated collection",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() == 1)
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP COLLECTION users")
        .await
        .expect("drop collection");

    wait_for(
        "all 3 nodes no longer see the collection",
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
async fn sequence_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    for node in &cluster.nodes {
        assert_eq!(node.sequence_count(1), 0);
    }

    let leader_idx = cluster
        .exec_ddl_on_any_leader("CREATE SEQUENCE order_id START 100")
        .await
        .expect("create sequence");
    eprintln!("CREATE SEQUENCE accepted by node {}", leader_idx + 1);

    wait_for(
        "all 3 nodes see the replicated sequence in their in-memory registry",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_sequence(1, "order_id")),
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("ALTER SEQUENCE order_id RESTART WITH 500")
        .await
        .expect("alter sequence restart");

    // A restart marks the sequence not yet called, so its stored counter
    // sits one increment below 500; the next `nextval` still returns 500.
    wait_for(
        "all 3 nodes: next order_id nextval == 500",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.sequence_next_value(1, "order_id") == Some(500))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP SEQUENCE order_id")
        .await
        .expect("drop sequence");

    wait_for(
        "all 3 nodes remove the sequence from their registry",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_sequence(1, "order_id")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn trigger_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION audits")
        .await
        .expect("create collection");

    wait_for(
        "collection visible on every node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() == 1)
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader(
            "CREATE TRIGGER audit_ins AFTER INSERT ON audits FOR EACH ROW BEGIN RETURN 1; END",
        )
        .await
        .expect("create trigger");

    wait_for(
        "all 3 nodes see the replicated trigger in trigger_registry",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_trigger(1, "audit_ins")),
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP TRIGGER audit_ins")
        .await
        .expect("drop trigger");

    wait_for(
        "all 3 nodes unregister the trigger",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_trigger(1, "audit_ins")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn procedure_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE PROCEDURE noop_proc() BEGIN RETURN 1; END")
        .await
        .expect("create procedure");

    wait_for(
        "all 3 nodes see the procedure in local SystemCatalog redb",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_procedure(1, "noop_proc"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP PROCEDURE noop_proc")
        .await
        .expect("drop procedure");

    wait_for(
        "all 3 nodes no longer see the procedure",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_procedure(1, "noop_proc"))
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn schedule_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE SCHEDULE nightly_cleanup CRON '0 0 * * *' AS BEGIN RETURN 1; END",
        )
        .await
        .expect("create schedule");

    wait_for(
        "all 3 nodes see the schedule in schedule_registry",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_schedule(1, "nightly_cleanup"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP SCHEDULE nightly_cleanup")
        .await
        .expect("drop schedule");

    wait_for(
        "all 3 nodes no longer see the schedule",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_schedule(1, "nightly_cleanup"))
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn change_stream_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION events")
        .await
        .expect("create collection");

    wait_for(
        "collection visible on every node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() == 1)
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("CREATE CHANGE STREAM event_feed ON events")
        .await
        .expect("create change stream");

    wait_for(
        "all 3 nodes see the stream in stream_registry",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_change_stream(DatabaseId::DEFAULT, 1, "event_feed"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP CHANGE STREAM event_feed")
        .await
        .expect("drop change stream");

    wait_for(
        "all 3 nodes no longer see the stream",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_change_stream(DatabaseId::DEFAULT, 1, "event_feed"))
        },
    )
    .await;

    cluster.shutdown().await;
}
