//! Integration test: replicated metadata group commits + cache apply.
//!
//! The `nodedb-cluster` crate does not understand per-DDL-object
//! descriptor shapes — `CatalogDdl { payload }` is opaque here.
//! This test verifies the cluster-side plumbing
//! (raft commit + metadata applier dispatch + cache watermark)
//! using synthetic opaque payloads. End-to-end cross-node DDL
//! visibility (applier decoding + redb writeback + pgwire visibility)
//! is covered by `nodedb/tests/sql_cluster_cross_node_dml.rs`.

mod common;

use std::time::Duration;

use nodedb_cluster::MetadataEntry;

use common::{TestNode, wait_for};

fn opaque_catalog_entry(data: &[u8]) -> MetadataEntry {
    MetadataEntry::CatalogDdl {
        payload: data.to_vec(),
    }
}

async fn find_metadata_leader<'a>(nodes: &'a [&'a TestNode]) -> &'a TestNode {
    for _ in 0..100 {
        for n in nodes {
            if n.is_metadata_leader() {
                return n;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no metadata-group leader elected within 5s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_ddl_replicates_across_3_nodes() {
    let node1 = TestNode::spawn(1, vec![]).await.expect("node 1 bootstrap");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let seeds = vec![node1.listen_addr()];
    let node2 = TestNode::spawn(2, seeds.clone())
        .await
        .expect("node 2 join");
    let node3 = TestNode::spawn(3, seeds).await.expect("node 3 join");

    let nodes = [&node1, &node2, &node3];

    wait_for(
        "all 3 nodes topology == 3",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || nodes.iter().all(|n| n.topology_size() == 3),
    )
    .await;

    let leader = find_metadata_leader(&nodes).await;
    eprintln!("metadata leader: node {}", leader.node_id);

    // Propose 3 opaque CatalogDdl entries. The `CacheApplier`
    // counts them regardless of payload shape.
    let idx1 = leader
        .propose_metadata(&opaque_catalog_entry(b"entry-1"))
        .expect("propose 1");
    let idx2 = leader
        .propose_metadata(&opaque_catalog_entry(b"entry-2"))
        .expect("propose 2");
    let idx3 = leader
        .propose_metadata(&opaque_catalog_entry(b"entry-3"))
        .expect("propose 3");
    assert!(idx1 > 0 && idx2 > idx1 && idx3 > idx2);

    wait_for(
        "all 3 nodes see 3 committed CatalogDdl entries",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || nodes.iter().all(|n| n.catalog_entries_applied() >= 3),
    )
    .await;

    node3.shutdown().await;
    node2.shutdown().await;
    node1.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_ddl_single_node_applies_to_cache() {
    let node = TestNode::spawn(1, vec![])
        .await
        .expect("single-node bootstrap");

    wait_for(
        "node 1 is metadata leader",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || node.is_metadata_leader(),
    )
    .await;

    let idx = node
        .propose_metadata(&opaque_catalog_entry(b"single-node"))
        .expect("propose");
    assert!(idx > 0);

    wait_for(
        "cache applied_index bumps",
        Duration::from_secs(3),
        Duration::from_millis(25),
        || node.catalog_entries_applied() >= 1,
    )
    .await;

    node.shutdown().await;
}
