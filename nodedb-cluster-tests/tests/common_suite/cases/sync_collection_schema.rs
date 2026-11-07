// SPDX-License-Identifier: BUSL-1.1
//! A peer-announced `CollectionSchema` (opcode 0x13) materializes the
//! collection into the cluster catalog on EVERY node.
//!
//! ## What this guards
//!
//! When a sync peer announces a `CollectionSchemaSyncMsg`, the receiving
//! node must materialize the collection into the system catalog — create-only,
//! never clobbering an existing collection — and, via the shared post-apply
//! path, register the Data-Plane engine state on every node that applies the
//! Raft entry. The end result: the synced collection is catalog-visible and
//! queryable cluster-wide, carrying the correct engine type and bitemporal
//! flag.
//!
//! The test drives the sync WebSocket end-to-end: it connects to ONE node's
//! sync listener, announces a descriptor for each supported engine, then
//! asserts the collection appears with the right `collection_type` +
//! `bitemporal` on a DIFFERENT node — proving Raft propagation plus per-node
//! catalog materialization, not just a local write on the receiving node.

use crate::common;
use common::cluster_harness::{TestCluster, TestClusterNode};

use std::time::{Duration, Instant};

use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb::control::shutdown::{ShutdownBus, ShutdownWatch};
use nodedb_test_support::sync_client::{SchemaOutcome, SyncTestClient};
use nodedb_types::collection_config::{PartitionStrategy, PrimaryEngine};
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::sync::wire::CollectionDescriptor;
use nodedb_types::{CollectionType, DatabaseId, Hlc};

/// Trust-mode sync sessions authenticate as tenant 1 (see the sync
/// handshake), so announced descriptors must carry tenant 1.
const TENANT: u64 = 1;

/// A single strict-schema column set reused by strict + kv descriptors.
fn pk_schema() -> StrictSchema {
    StrictSchema {
        columns: vec![ColumnDef::required("id", ColumnType::Int64).with_primary_key()],
        version: 1,
        dropped_columns: Vec::new(),
        bitemporal: false,
    }
}

/// Build a descriptor for `name` with `collection_type` and `bitemporal`,
/// mirroring the shape a local `CREATE COLLECTION` would emit over sync.
fn descriptor(
    name: &str,
    collection_type: CollectionType,
    bitemporal: bool,
) -> CollectionDescriptor {
    CollectionDescriptor {
        tenant_id: TENANT,
        database_id: DatabaseId::DEFAULT,
        name: name.to_string(),
        partition_strategy: PartitionStrategy::default_for_collection_type(&collection_type),
        collection_type,
        bitemporal,
        crdt: false,
        fields: Vec::new(),
        primary: PrimaryEngine::Document,
        vector_primary: None,
        declared_primary_key: None,
        descriptor_version: 0,
    }
}

/// Poll `node`'s local catalog until the collection is visible, then return
/// its `(collection_type, bitemporal)`. Panics if it does not converge —
/// a bounded retry loop, not a single fixed sleep.
async fn await_collection(node: &TestClusterNode, name: &str) -> (CollectionType, bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let c = node.shared.credentials.catalog();
        let found = c
            .get_collection(DatabaseId::DEFAULT, TENANT, name)
            .ok()
            .flatten();
        if let Some(coll) = found {
            return (coll.collection_type, coll.bitemporal);
        }
        if Instant::now() >= deadline {
            panic!("collection '{name}' did not become catalog-visible on the follower within 30s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn announced_schema_materializes_on_every_node() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");

    // Start the sync listener on node 0; the client connects here, so node 0
    // is the RECEIVING node. Assertions run on node 1 (a different node) to
    // prove Raft propagation + per-node catalog materialization.
    let cfg = SyncListenerConfig {
        listen_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (shutdown_bus, _shutdown_handle) =
        ShutdownBus::new(std::sync::Arc::new(ShutdownWatch::new()));
    let state = start_sync_listener(
        cfg,
        Some(std::sync::Arc::clone(&cluster.nodes[0].shared)),
        shutdown_bus,
    )
    .await
    .expect("start sync listener on node 0");
    let addr = state.config.listen_addr;

    let mut client = SyncTestClient::connect(addr)
        .await
        .expect("sync handshake with node 0");

    // One descriptor per engine, plus a bitemporal document variant.
    let cases: Vec<(&str, CollectionType, bool)> = vec![
        ("sync_doc", CollectionType::document(), false),
        ("sync_strict", CollectionType::strict(pk_schema()), false),
        ("sync_kv", CollectionType::kv(pk_schema()), false),
        ("sync_columnar", CollectionType::columnar(), false),
        ("sync_ts", CollectionType::timeseries("ts", "1m"), false),
        ("sync_spatial", CollectionType::spatial("geom"), false),
        ("sync_bitemporal", CollectionType::document(), true),
    ];

    let hlc = Hlc::new(1, 0);
    for (name, ct, bitemporal) in &cases {
        let outcome = client
            .push_collection_schema(descriptor(name, ct.clone(), *bitemporal), hlc)
            .await
            .unwrap_or_else(|e| panic!("push CollectionSchema for '{name}': {e}"));
        match outcome {
            SchemaOutcome::Held(held) => assert_ne!(
                held.creation_hlc,
                Hlc::ZERO,
                "the answer for '{name}' must carry the collection's incarnation"
            ),
            SchemaOutcome::Reject(reject) => {
                panic!("announce for '{name}' refused: {reject:?}")
            }
        }
    }

    // Assert on node 1 — a node the client never talked to.
    let follower = &cluster.nodes[1];
    for (name, expected_ct, expected_bitemporal) in &cases {
        let (got_ct, got_bitemporal) = await_collection(follower, name).await;
        assert_eq!(
            &got_ct, expected_ct,
            "collection '{name}' materialized with the wrong engine type on the follower"
        );
        assert_eq!(
            got_bitemporal, *expected_bitemporal,
            "collection '{name}' materialized with the wrong bitemporal flag on the follower"
        );
    }

    cluster.shutdown().await;
}
