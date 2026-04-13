//! End-to-end cluster tests for descriptor lease drain.
//!
//! Covers the full drain lifecycle on a real 3-node cluster:
//!
//! - Drain state replicates from the proposer to every follower.
//! - New lease acquires at a drained version are rejected
//!   cluster-wide, including via the leader-forwarding path.
//! - DDL blocks until existing leases release, then proceeds.
//! - Drain times out cleanly and emits an explicit `DrainEnd`
//!   so the cluster can make progress on the next DDL.
//! - New acquires at a NEWER version than the drain are allowed.
//!
//! The tests use short lease durations + short drain timeouts to
//! keep the whole suite under ~5 seconds on a warm build.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};
use nodedb_cluster::{DescriptorId, DescriptorKind};

const TENANT: u32 = 1;
const WAIT_BUDGET: Duration = Duration::from_secs(3);
const POLL: Duration = Duration::from_millis(20);

fn coll_id(name: &str) -> DescriptorId {
    DescriptorId::new(TENANT, DescriptorKind::Collection, name.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drain_blocks_new_acquires_at_drained_version() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let leader = &cluster.nodes[0];
    let id = coll_id("orders");

    // Drive a drain start entry through raft so every node
    // observes the drain in its local tracker. We issue this via
    // `drain_for_ddl` but with `prior_version=1` and without any
    // lease held — the drain succeeds immediately (nothing to
    // wait for) but the drain-start entry still committed and
    // propagated.
    //
    // To keep the drain entry ACTIVE long enough to test the
    // rejection behavior, we manually propose a DrainStart with
    // a far-future expiry via a test-only harness helper, then
    // cancel it at the end of the test.
    let shared = Arc::clone(&leader.shared);
    let drain_id = id.clone();
    tokio::task::spawn_blocking(move || {
        let now_hlc = shared.hlc_clock.now();
        let expires_at = nodedb_types::Hlc::new(now_hlc.wall_ns.saturating_add(60_000_000_000), 0);
        let entry = nodedb_cluster::MetadataEntry::DescriptorDrainStart {
            descriptor_id: drain_id,
            up_to_version: 1,
            expires_at,
        };
        let raw = nodedb_cluster::encode_entry(&entry).expect("encode");
        let handle = shared.metadata_raft.get().expect("metadata raft handle");
        handle.propose(raw).expect("propose drain start");
    })
    .await
    .expect("join");

    // Wait for every node to observe the drain in its local tracker.
    wait_for(
        "all 3 nodes observe the drain entry",
        WAIT_BUDGET,
        POLL,
        || cluster.nodes.iter().all(|n| n.has_drain_for(&id, 1)),
    )
    .await;

    // Every node should now reject acquires at version 1 (the
    // drained version). Version 2 should still succeed.
    for node in &cluster.nodes {
        let err = node
            .acquire_lease(
                DescriptorKind::Collection,
                TENANT,
                "orders",
                1,
                Duration::from_secs(60),
            )
            .await
            .expect_err("acquire at drained version must fail");
        assert!(
            err.contains("drain in progress"),
            "unexpected error on node {}: {err}",
            node.node_id
        );
    }

    // Version 2 is OUTSIDE the drain range → acquires succeed.
    leader
        .acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "orders",
            2,
            Duration::from_secs(60),
        )
        .await
        .expect("acquire at version above drain succeeds");

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drain_clears_after_end_entry() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let leader = &cluster.nodes[0];
    let id = coll_id("cleared");

    // Propose DrainStart then DrainEnd. After both have applied
    // every node's tracker should be empty for this descriptor.
    let shared = Arc::clone(&leader.shared);
    let drain_id = id.clone();
    tokio::task::spawn_blocking(move || {
        let now_hlc = shared.hlc_clock.now();
        let expires_at = nodedb_types::Hlc::new(now_hlc.wall_ns.saturating_add(60_000_000_000), 0);
        let handle = shared.metadata_raft.get().expect("handle");
        let start = nodedb_cluster::MetadataEntry::DescriptorDrainStart {
            descriptor_id: drain_id.clone(),
            up_to_version: 5,
            expires_at,
        };
        handle
            .propose(nodedb_cluster::encode_entry(&start).unwrap())
            .expect("start");
        let end = nodedb_cluster::MetadataEntry::DescriptorDrainEnd {
            descriptor_id: drain_id,
        };
        handle
            .propose(nodedb_cluster::encode_entry(&end).unwrap())
            .expect("end");
    })
    .await
    .expect("join");

    wait_for("drain cleared on every node", WAIT_BUDGET, POLL, || {
        cluster.nodes.iter().all(|n| !n.has_drain_for(&id, 5))
    })
    .await;

    // Acquires at version 1 must now succeed.
    leader
        .acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "cleared",
            1,
            Duration::from_secs(60),
        )
        .await
        .expect("acquire after end");

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ddl_waits_for_existing_lease_to_release() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let leader = &cluster.nodes[0];

    // Bootstrap a real collection via pgwire so there's a
    // descriptor with prior version > 0 for drain to target.
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION drainable")
        .await
        .expect("create");

    // Wait for the collection to stamp version 1 on every node.
    wait_for(
        "collection stamped v1 on every node",
        WAIT_BUDGET,
        POLL,
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.collection_descriptor(TENANT, "drainable").map(|s| s.0) == Some(1))
        },
    )
    .await;

    // Acquire a lease on v1 — this is what drain must wait for.
    leader
        .acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "drainable",
            1,
            Duration::from_secs(60),
        )
        .await
        .expect("acquire v1 lease");

    // Kick off an ALTER directly via `propose_catalog_entry`
    // rather than pgwire so the test can run it in a
    // spawn_blocking task while the main task polls for drain
    // state. We build a fresh `PutCollection` with the same
    // content as the existing v1 record — the applier will see
    // a Put* for an existing descriptor, run drain for prior=1,
    // wait for our lease to release, then commit as v2.
    let shared = Arc::clone(&leader.shared);
    let existing = shared
        .credentials
        .catalog()
        .as_ref()
        .unwrap()
        .get_collection(TENANT, "drainable")
        .expect("read existing")
        .expect("exists");

    let alter_shared = Arc::clone(&leader.shared);
    let alter_handle = tokio::task::spawn_blocking(move || {
        let entry = nodedb::control::catalog_entry::CatalogEntry::PutCollection(Box::new(existing));
        nodedb::control::metadata_proposer::propose_catalog_entry_with_timeout(
            &alter_shared,
            &entry,
            Duration::from_secs(10),
        )
    });

    // Give the ALTER a chance to start draining. Poll until the
    // drain entry shows up in the tracker on the leader.
    wait_for(
        "leader observes drain-in-progress",
        WAIT_BUDGET,
        POLL,
        || leader.has_drain_for(&coll_id("drainable"), 1),
    )
    .await;

    // While drain is active, version 1 is still stamped (the
    // Put* hasn't committed yet). Sanity: wait a beat and
    // confirm the version has NOT bumped.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        leader
            .collection_descriptor(TENANT, "drainable")
            .map(|s| s.0),
        Some(1),
        "DDL must not have committed while drain is waiting"
    );

    // Release the lease. Drain should complete; the Put* should
    // commit; version should bump to 2 on every node.
    leader
        .release_leases(vec![coll_id("drainable")])
        .await
        .expect("release");

    let alter_result = alter_handle.await.expect("join");
    assert!(
        alter_result.is_ok(),
        "alter failed: {:?}",
        alter_result.err()
    );

    wait_for(
        "collection stamped v2 on every node",
        WAIT_BUDGET,
        POLL,
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.collection_descriptor(TENANT, "drainable").map(|s| s.0) == Some(2))
        },
    )
    .await;

    // Drain should have cleared on every node (implicit clear on
    // successful Put* apply).
    for node in &cluster.nodes {
        assert!(
            !node.has_drain_for(&coll_id("drainable"), 1),
            "node {} still has drain entry after DDL committed",
            node.node_id
        );
    }

    let _ = shared; // Keep the earlier binding alive to silence warnings.
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drain_timeout_clears_state() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let leader = &cluster.nodes[0];
    let id = coll_id("stuck");

    // Install a drain with far-future expiry, then call
    // drain_for_ddl directly with a 100ms wait and a lease in
    // the way so drain cannot complete. The drain_for_ddl call
    // times out, emits DrainEnd, and returns an error.
    //
    // Setup: acquire a long-lived lease at v1 so drain has
    // something to wait for that it cannot clear on its own.
    leader
        .acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "stuck",
            1,
            Duration::from_secs(60),
        )
        .await
        .expect("acquire");

    let shared = Arc::clone(&leader.shared);
    let drain_id = id.clone();
    let result = tokio::task::spawn_blocking(move || {
        nodedb::control::lease::drain_for_ddl(&shared, drain_id, 1, Duration::from_millis(200))
    })
    .await
    .expect("join");

    assert!(result.is_err(), "drain should have timed out");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("drain timed out"),
        "unexpected error: {err_msg}"
    );

    // After timeout, every node should see the drain entry
    // removed (drain_for_ddl proposed DrainEnd explicitly).
    wait_for(
        "drain cleared on every node after timeout",
        WAIT_BUDGET,
        POLL,
        || cluster.nodes.iter().all(|n| !n.has_drain_for(&id, 1)),
    )
    .await;

    // Sanity: new acquires at v1 on a DIFFERENT collection work
    // fine (drain was scoped to the stuck descriptor).
    leader
        .acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "unrelated",
            1,
            Duration::from_secs(60),
        )
        .await
        .expect("unrelated acquire");

    cluster.shutdown().await;
}
