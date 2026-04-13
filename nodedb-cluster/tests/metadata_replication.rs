//! Integration test: replicated catalog via the metadata raft group.
//!
//! Proves the batch 1a foundation end-to-end on a real 3-node
//! in-process cluster:
//!
//! 1. 3 nodes form a cluster (node 1 bootstraps; 2 + 3 join over QUIC).
//! 2. We find whichever node has been elected leader of the metadata
//!    group (group 0).
//! 3. We propose `MetadataEntry::CollectionDdl::Create` through
//!    `RaftLoop::propose_to_metadata_group` (the same entry point
//!    the production `metadata_proposer` will use once pgwire DDL
//!    handlers are migrated).
//! 4. Every node's `MetadataCache` — driven by a `CacheApplier`
//!    installed on its raft loop — must observe the new descriptor
//!    within a short timeout.
//! 5. We propose an `Alter { AddColumn }` and assert the descriptor
//!    version bumps on every node.
//! 6. We propose a `Drop` and assert the descriptor is removed from
//!    every node's cache.
//!
//! This is the regression test for the DO deployment failure where
//! `CREATE COLLECTION` on node A was invisible to nodes B and C.

mod common;

use std::time::Duration;

use nodedb_cluster::{
    CollectionAction, CollectionAlter, CollectionDescriptor, ColumnDef, DescriptorHeader,
    DescriptorId, DescriptorKind, MetadataEntry,
};
use nodedb_types::Hlc;

use common::{TestNode, wait_for};

const TEST_TENANT: u32 = 1;
const COLL_NAME: &str = "users";

fn coll_id() -> DescriptorId {
    DescriptorId::new(TEST_TENANT, DescriptorKind::Collection, COLL_NAME)
}

fn make_descriptor(version: u64) -> CollectionDescriptor {
    CollectionDescriptor {
        header: DescriptorHeader::new_public(
            coll_id(),
            version,
            Hlc::new(version.wrapping_mul(1_000_000), 0),
        ),
        collection_type: "document_schemaless".into(),
        columns: vec![ColumnDef {
            name: "name".into(),
            data_type: "TEXT".into(),
            nullable: true,
            default: None,
        }],
        with_options: vec![],
        primary_key: None,
    }
}

fn create_entry() -> MetadataEntry {
    MetadataEntry::CollectionDdl {
        tenant_id: TEST_TENANT,
        action: CollectionAction::Create(Box::new(make_descriptor(1))),
    }
}

fn alter_entry() -> MetadataEntry {
    MetadataEntry::CollectionDdl {
        tenant_id: TEST_TENANT,
        action: CollectionAction::Alter {
            id: coll_id(),
            change: CollectionAlter::AddColumn(ColumnDef {
                name: "email".into(),
                data_type: "TEXT".into(),
                nullable: true,
                default: None,
            }),
        },
    }
}

fn drop_entry() -> MetadataEntry {
    MetadataEntry::CollectionDdl {
        tenant_id: TEST_TENANT,
        action: CollectionAction::Drop { id: coll_id() },
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
async fn collection_ddl_replicates_across_3_nodes() {
    // ── 1. Spawn a 3-node cluster ──────────────────────────────────
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

    // ── 2. Identify the metadata-group leader ──────────────────────
    let leader = find_metadata_leader(&nodes).await;
    eprintln!("metadata leader: node {}", leader.node_id);

    // ── 3. Propose CREATE ──────────────────────────────────────────
    let create_idx = leader
        .propose_metadata(&create_entry())
        .expect("propose create");
    assert!(create_idx > 0);

    wait_for(
        "all 3 nodes see collection at version 1",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            nodes
                .iter()
                .all(|n| n.collection_version(TEST_TENANT, COLL_NAME) == Some(1))
        },
    )
    .await;

    // ── 4. Propose ALTER AddColumn ─────────────────────────────────
    let alter_idx = leader
        .propose_metadata(&alter_entry())
        .expect("propose alter");
    assert!(alter_idx > create_idx);

    wait_for(
        "all 3 nodes see collection at version 2 after Alter",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            nodes
                .iter()
                .all(|n| n.collection_version(TEST_TENANT, COLL_NAME) == Some(2))
        },
    )
    .await;

    // ── 5. Propose DROP ────────────────────────────────────────────
    let drop_idx = leader
        .propose_metadata(&drop_entry())
        .expect("propose drop");
    assert!(drop_idx > alter_idx);

    wait_for(
        "all 3 nodes no longer see the collection",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || nodes.iter().all(|n| n.collection_count() == 0),
    )
    .await;

    node3.shutdown().await;
    node2.shutdown().await;
    node1.shutdown().await;
}

/// Single-node variant: propose + apply on the same node. No QUIC hop,
/// but still exercises the full raft-commit → metadata_applier pipe.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_ddl_single_node_applies_to_cache() {
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

    let idx = node.propose_metadata(&create_entry()).expect("propose");
    assert!(idx > 0);

    wait_for(
        "cache sees version 1",
        Duration::from_secs(3),
        Duration::from_millis(25),
        || node.collection_version(TEST_TENANT, COLL_NAME) == Some(1),
    )
    .await;

    node.shutdown().await;
}
