// SPDX-License-Identifier: BUSL-1.1

//! `PURGE TENANT` and `CONVERT COLLECTION` reach every replica through the
//! data group's log.
//!
//! Each test runs the statement on one node of a 3-node cluster, with every
//! data group on all three nodes. It then reads each node's own stored row
//! images from that node's tenant snapshot, so a replica's answer is its own
//! storage and never a peer's.

use std::collections::BTreeMap;
use std::time::Duration;

use nodedb::types::TenantDataSnapshot;
use nodedb_test_support::cluster_harness::snapshot_rows::current_document_rows;
use nodedb_types::{DatabaseId, TenantId};

use crate::common::cluster_harness::{TestCluster, TestClusterNode};

/// The tenant of the harness superuser, and of every row these tests write.
const TENANT: u64 = 1;
const REPLICAS: usize = 3;
const ROWS: usize = 5;
const CONVERGE: Duration = Duration::from_secs(20);

/// The stored row images of `collection` on `node`, keyed by row key.
async fn stored_rows(node: &TestClusterNode, collection: &str) -> BTreeMap<String, Vec<u8>> {
    let bytes = node.create_tenant_snapshot(TenantId::new(TENANT)).await;
    let snapshot: TenantDataSnapshot =
        zerompk::from_msgpack(&bytes).unwrap_or_else(|e| panic!("snapshot decodes: {e}"));
    current_document_rows(&snapshot)
        .into_iter()
        .filter(|(key, _)| key.split(':').nth(2) == Some(collection))
        .collect()
}

async fn replicated_cluster() -> TestCluster {
    let cluster = TestCluster::spawn_three_with_replication_factor(REPLICAS)
        .await
        .expect("3-node cluster");
    // A cluster that already holds the row refuses this, and the statements
    // under test only need the row to exist.
    let _ = cluster
        .exec_ddl_on_any_leader(&format!("CREATE TENANT harness_tenant ID {TENANT}"))
        .await;
    cluster
}

async fn create_with_rows(cluster: &TestCluster, collection: &str) {
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {collection} WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    for row in 0..ROWS {
        cluster.nodes[0]
            .exec(&format!(
                "INSERT INTO {collection} (id, name) VALUES ('r{row}', 'name{row}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert into {collection}: {e}"));
    }
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
}

/// The node index of a node that does not lead `collection`'s data group.
fn follower_of(cluster: &TestCluster, collection: &str) -> usize {
    let vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection)
        .vshard()
        .as_u32();
    let routing = cluster.nodes[0]
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let group = routing.group_for_vshard(vshard).expect("vShard group");
    let leader = routing.group_info(group).map_or(0, |info| info.leader);
    cluster
        .nodes
        .iter()
        .position(|node| node.node_id != leader)
        .expect("a node that does not lead the group")
}

/// A purge run on a node that does not lead the group empties every replica.
/// A write after the purge lands on every replica, and a restarted replica
/// replays no purged row back from its WAL.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_tenant_purge_empties_every_replica_and_survives_a_restart() {
    const FIRST: &str = "pcr_purge_a";
    const SECOND: &str = "pcr_purge_b";
    let mut cluster = replicated_cluster().await;
    create_with_rows(&cluster, FIRST).await;
    create_with_rows(&cluster, SECOND).await;
    for node in &cluster.nodes {
        assert_eq!(stored_rows(node, FIRST).await.len(), ROWS);
        assert_eq!(stored_rows(node, SECOND).await.len(), ROWS);
    }

    let coordinator = follower_of(&cluster, FIRST);
    cluster.nodes[coordinator]
        .exec(&format!("PURGE TENANT {TENANT} CONFIRM"))
        .await
        .expect("purge the tenant");
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    for node in &cluster.nodes {
        for collection in [FIRST, SECOND] {
            assert!(
                stored_rows(node, collection).await.is_empty(),
                "node {} must hold no row of {collection} after the purge",
                node.node_id
            );
        }
    }

    cluster.nodes[0]
        .exec(&format!(
            "INSERT INTO {FIRST} (id, name) VALUES ('after', 'purge')"
        ))
        .await
        .expect("insert after the purge");
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    let mut images = Vec::new();
    for node in &cluster.nodes {
        images.push(stored_rows(node, FIRST).await);
    }
    assert_eq!(images[0].len(), 1, "the later write lands on node 0");
    assert_eq!(images[0], images[1], "replicas 0 and 1 hold one image");
    assert_eq!(images[1], images[2], "replicas 1 and 2 hold one image");

    let stopped = cluster
        .stop_member(coordinator)
        .await
        .expect("stop a replica");
    cluster
        .restart_member(stopped)
        .await
        .expect("restart the replica");
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    let node = &cluster.nodes[coordinator];
    assert_eq!(
        stored_rows(node, FIRST).await.len(),
        1,
        "a restart replays no purged row back from the WAL"
    );
    assert!(stored_rows(node, SECOND).await.is_empty());

    cluster.shutdown().await;
}

/// A conversion run on one node re-encodes every replica's rows to one byte
/// image. Writes on either side of it land in the encoding their log position
/// names, and a restarted replica reads the converted rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_conversion_gives_every_replica_the_same_bytes_and_survives_a_restart() {
    const COLLECTION: &str = "pcr_convert";
    let mut cluster = replicated_cluster().await;
    create_with_rows(&cluster, COLLECTION).await;

    let before = stored_rows(&cluster.nodes[0], COLLECTION).await;
    assert_eq!(before.len(), ROWS);

    let coordinator = follower_of(&cluster, COLLECTION);
    cluster.nodes[coordinator]
        .exec(&format!(
            "CONVERT COLLECTION {COLLECTION} TO document_strict \
             (id TEXT PRIMARY KEY, name TEXT)"
        ))
        .await
        .expect("convert the collection");
    cluster.wait_for_full_apply_convergence(CONVERGE).await;

    let mut images = Vec::new();
    for node in &cluster.nodes {
        let image = stored_rows(node, COLLECTION).await;
        assert_eq!(image.len(), ROWS, "node {} keeps its rows", node.node_id);
        for (key, body) in &image {
            assert_ne!(
                before.get(key),
                Some(body),
                "node {} must hold row {key} in the target encoding",
                node.node_id
            );
        }
        images.push(image);
    }
    assert_eq!(images[0], images[1], "replicas 0 and 1 hold one image");
    assert_eq!(images[1], images[2], "replicas 1 and 2 hold one image");

    cluster.nodes[0]
        .exec(&format!(
            "INSERT INTO {COLLECTION} (id, name) VALUES ('after', 'convert')"
        ))
        .await
        .expect("insert after the conversion");
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    let mut images = Vec::new();
    for node in &cluster.nodes {
        images.push(stored_rows(node, COLLECTION).await);
    }
    assert_eq!(images[0].len(), ROWS + 1);
    assert_eq!(images[0], images[1], "replicas 0 and 1 hold one image");
    assert_eq!(images[1], images[2], "replicas 1 and 2 hold one image");

    let stopped = cluster
        .stop_member(coordinator)
        .await
        .expect("stop a replica");
    cluster
        .restart_member(stopped)
        .await
        .expect("restart the replica");
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    let node = &cluster.nodes[coordinator];
    assert_eq!(
        stored_rows(node, COLLECTION).await,
        images[0],
        "a restart keeps the converted image"
    );
    let messages = node
        .client
        .simple_query(&format!("SELECT name FROM {COLLECTION} WHERE id = 'r1'"))
        .await
        .expect("read through the restarted replica");
    assert!(
        messages.iter().any(|message| matches!(
            message,
            tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0) == Some("name1")
        )),
        "the restarted replica decodes a converted row: {messages:?}"
    );

    cluster.shutdown().await;
}
