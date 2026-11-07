// SPDX-License-Identifier: BUSL-1.1
//! The EMIT side of collection-schema sync: a `CollectionSchema` frame
//! (opcode 0x13) is written to the peer STRICTLY BEFORE the first data frame
//! for a collection.
//!
//! ## What this guards
//!
//! The announce-precedes-data invariant. Before the server ships a shape
//! snapshot (or a delta) for a collection to a sync peer, it must first send
//! that collection's `CollectionDescriptor` so the peer can materialize the
//! collection with the correct engine before any rows arrive. The announce is
//! idempotent per session.
//!
//! The test drives the sync WebSocket end-to-end: it connects to node 0's
//! sync listener, materializes a document collection on the server (via a
//! client-side `CollectionSchema` announce, which does NOT mark the server
//! session's emit-announce set), then subscribes to a Document shape for that
//! collection and asserts the FIRST frame back is `CollectionSchema` and the
//! NEXT is `ShapeSnapshot`.

use crate::common;
use common::cluster_harness::{TestCluster, TestClusterNode};

use std::time::{Duration, Instant};

use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb::control::shutdown::{ShutdownBus, ShutdownWatch};
use nodedb_test_support::sync_client::{SchemaOutcome, SyncTestClient};
use nodedb_types::collection_config::{PartitionStrategy, PrimaryEngine};
use nodedb_types::sync::wire::{CollectionDescriptor, SyncMessageType};
use nodedb_types::{CollectionType, DatabaseId, Hlc};

/// Trust-mode sync sessions authenticate as tenant 1.
const TENANT: u64 = 1;

/// Build a plain-document descriptor for `name`, mirroring what a local
/// `CREATE COLLECTION` would emit over sync.
fn document_descriptor(name: &str) -> CollectionDescriptor {
    let collection_type = CollectionType::document();
    CollectionDescriptor {
        tenant_id: TENANT,
        database_id: DatabaseId::DEFAULT,
        name: name.to_string(),
        partition_strategy: PartitionStrategy::default_for_collection_type(&collection_type),
        collection_type,
        bitemporal: false,
        crdt: false,
        fields: Vec::new(),
        primary: PrimaryEngine::Document,
        vector_primary: None,
        declared_primary_key: None,
        descriptor_version: 0,
    }
}

/// Poll `node`'s local catalog until the collection is visible. Panics if it
/// does not converge — a bounded retry loop, not a single fixed sleep.
async fn await_collection(node: &TestClusterNode, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let c = node.shared.credentials.catalog();
        let found = c
            .get_collection(DatabaseId::DEFAULT, TENANT, name)
            .ok()
            .flatten();
        if found.is_some() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("collection '{name}' did not become catalog-visible within 30s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collection_schema_precedes_shape_snapshot() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");

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

    // Materialize the collection on the server. This is the client→server
    // announce (receive side). The push consumes the server's answer to it.
    // The answer does not mark the server session's emit-announce set, so
    // the subscription below still triggers an emit.
    let name = "emit_doc";
    let outcome = client
        .push_collection_schema(document_descriptor(name), Hlc::new(1, 0))
        .await
        .expect("push CollectionSchema to materialize collection");
    assert!(
        matches!(outcome, SchemaOutcome::Held(_)),
        "announce must answer the held collection, got {outcome:?}"
    );

    // Wait until the receiving node's catalog can resolve the collection —
    // the emit path resolves the descriptor from exactly this catalog.
    await_collection(&cluster.nodes[0], name).await;

    // Subscribe to a Document shape for the collection.
    client
        .subscribe_document_shape("emit_shape", name, TENANT as u32)
        .await
        .expect("send ShapeSubscribe");

    // The first frame back MUST be the collection-schema announce.
    let first = client
        .recv_next_frame()
        .await
        .expect("receive first frame after subscribe");
    assert_eq!(
        first.msg_type,
        SyncMessageType::CollectionSchema,
        "expected CollectionSchema announce before any shape data, got {:?}",
        first.msg_type
    );
    let announced: nodedb_types::sync::wire::CollectionSchemaSyncMsg =
        first.decode_body().expect("decode CollectionSchemaSyncMsg");
    assert_eq!(
        announced.descriptor.name, name,
        "announced descriptor is for the wrong collection"
    );

    // The next frame MUST be the shape snapshot — data strictly after schema.
    let second = client
        .recv_next_frame()
        .await
        .expect("receive second frame after subscribe");
    assert_eq!(
        second.msg_type,
        SyncMessageType::ShapeSnapshot,
        "expected ShapeSnapshot after the CollectionSchema announce, got {:?}",
        second.msg_type
    );

    cluster.shutdown().await;
}
