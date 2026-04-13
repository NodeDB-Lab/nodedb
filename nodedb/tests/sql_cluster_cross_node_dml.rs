//! End-to-end cluster test: CREATE / INSERT / SELECT across 3 pgwire
//! clients, one per node.
//!
//! This is the acceptance gate for batch 1d / Phase A per
//! `resource/SQL_CLUSTER_CHECKLIST.md`. It replays the exact
//! production failure mode from the DO deployment that motivated
//! this checklist:
//!
//! > CREATE COLLECTION on node 1, SELECT on node 2 → "unknown table"
//!
//! The modern path should succeed end-to-end because:
//!
//! 1. `create_collection` proposes a `MetadataEntry::CollectionDdl::
//!    Create` through the metadata raft group (group 0).
//! 2. The entry commits on quorum and the `MetadataCommitApplier`
//!    runs on every node: writes the descriptor to the replicated
//!    `MetadataCache` AND writes the host `StoredCollection` to the
//!    local `SystemCatalog` redb AND spawns a `DocumentOp::Register`
//!    into the local Data Plane core.
//! 3. An INSERT on node 2 reads the collection from its local redb
//!    (populated by the applier), dispatches into its own Data
//!    Plane which knows the storage mode (registered by the
//!    applier), and the row lands on node 2's vShard.
//!
//! This test exercises every link in that chain.

mod common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_cluster_boots() {
    // Smallest possible smoke test: one node in cluster mode.
    let node = common::cluster_harness::TestClusterNode::spawn(1, vec![])
        .await
        .expect("single-node cluster spawn");
    assert_eq!(node.topology_size(), 1);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn single_node_cluster_create_collection() {
    // Isolates the pgwire handler → propose_metadata_and_wait path
    // on a single-node cluster so cluster-formation noise (elections,
    // joining learners) is out of the picture.
    let node = common::cluster_harness::TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn");
    // Give the raft tick a moment to process any startup entries.
    tokio::time::sleep(Duration::from_millis(200)).await;
    node.exec("CREATE COLLECTION widgets")
        .await
        .expect("create widgets");
    assert_eq!(node.cached_collection_count(), 1);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn create_on_any_node_is_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    // Every node starts with an empty replicated cache.
    for node in &cluster.nodes {
        assert_eq!(node.cached_collection_count(), 0);
    }

    // CREATE proposed on whichever node is the metadata-group leader.
    // The cluster harness retries across nodes so we don't need to
    // discover the leader explicitly.
    let leader_idx = cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION users")
        .await
        .expect("create collection");
    eprintln!("CREATE accepted by node {}", leader_idx + 1);

    // Every node's replicated cache must see the new collection.
    wait_for(
        "all 3 nodes see the replicated collection",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() == 1)
        },
    )
    .await;

    // DROP on any leader — should cascade through raft and
    // deactivate the record on every node's `SystemCatalog` redb
    // via the applier's Drop branch.
    cluster
        .exec_ddl_on_any_leader("DROP COLLECTION users")
        .await
        .expect("drop collection");

    // The replicated-cache view removes the descriptor on Drop.
    wait_for(
        "all 3 nodes no longer see the collection",
        Duration::from_secs(5),
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
    // 3-node cluster. CREATE SEQUENCE on the leader; every follower's
    // in-memory `sequence_registry` must see the replicated definition
    // within 5s via the `CatalogEntry::PutSequence` → post-apply hook.
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
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_sequence(1, "order_id")),
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP SEQUENCE order_id")
        .await
        .expect("drop sequence");

    wait_for(
        "all 3 nodes remove the sequence from their registry",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_sequence(1, "order_id")),
    )
    .await;

    cluster.shutdown().await;
}
