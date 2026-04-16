//! End-to-end tests for planner-side descriptor lease integration
//! and shutdown lease release.
//!
//! Runs against a 3-node cluster via the standard `TestCluster`
//! harness. Because the pgwire handler is the integration
//! surface, tests drive SELECTs through `tokio_postgres` and
//! assert lease state changes in the leader's
//! `MetadataCache.leases`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};
use nodedb_cluster::{DescriptorId, DescriptorKind};

const TENANT: u32 = 1;
const WAIT_BUDGET: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(20);

fn coll_id(name: &str) -> DescriptorId {
    DescriptorId::new(TENANT, DescriptorKind::Collection, name.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn select_through_pgwire_acquires_lease() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION planner_orders")
        .await
        .expect("create");

    // Wait for the collection to be stamped at v1 on all nodes.
    wait_for("collection stamped v1", WAIT_BUDGET, POLL, || {
        cluster.nodes.iter().all(|n| {
            n.collection_descriptor(TENANT, "planner_orders")
                .map(|s| s.0)
                == Some(1)
        })
    })
    .await;

    // Run a SELECT through pgwire so the planner lands
    // `OriginCatalog::get_collection` → `acquire_descriptor_lease`.
    // The SELECT may return no rows; we only care about the
    // lease side effect.
    cluster.nodes[0]
        .exec("SELECT * FROM planner_orders")
        .await
        .expect("select");

    // Every node should observe a lease on (planner_orders, v1)
    // keyed by whichever node ran the plan (node 1).
    wait_for(
        "leader node holds a planner_orders lease",
        WAIT_BUDGET,
        POLL,
        || {
            cluster.nodes[0].has_lease(
                DescriptorKind::Collection,
                TENANT,
                "planner_orders",
                cluster.nodes[0].node_id,
                1,
            )
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drain_forces_plan_retry_surfacing_typed_error() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION retry_me")
        .await
        .expect("create");

    wait_for("collection stamped v1", WAIT_BUDGET, POLL, || {
        cluster
            .nodes
            .iter()
            .all(|n| n.collection_descriptor(TENANT, "retry_me").map(|s| s.0) == Some(1))
    })
    .await;

    // Install a drain entry with a far-future expiry targeting
    // v1. Every node's `lease_drain` will reject acquires at
    // v1 until we manually clear it.
    let leader = &cluster.nodes[0];
    let shared = Arc::clone(&leader.shared);
    let drain_id = coll_id("retry_me");
    let drain_id_clone = drain_id.clone();
    tokio::task::spawn_blocking(move || {
        let now_hlc = shared.hlc_clock.now();
        let expires_at = nodedb_types::Hlc::new(now_hlc.wall_ns.saturating_add(60_000_000_000), 0);
        let entry = nodedb_cluster::MetadataEntry::DescriptorDrainStart {
            descriptor_id: drain_id_clone,
            up_to_version: 1,
            expires_at,
        };
        let raw = nodedb_cluster::encode_entry(&entry).expect("encode");
        let handle = shared.metadata_raft.get().expect("handle");
        handle.propose(raw).expect("propose drain start");
    })
    .await
    .expect("join");

    wait_for("all nodes observe drain", WAIT_BUDGET, POLL, || {
        cluster.nodes.iter().all(|n| n.has_drain_for(&drain_id, 1))
    })
    .await;

    // A SELECT should fail fast: the pgwire retry helper makes
    // 3 attempts, each hits the drain check, each surfaces
    // `RetryableSchemaChanged`. After the retry budget the
    // error maps to a plan error visible to the client.
    let result = leader.exec("SELECT * FROM retry_me").await;
    assert!(result.is_err(), "SELECT must fail while drain is active");
    let err = result.unwrap_err();
    assert!(
        err.to_lowercase().contains("retryable schema change")
            || err.to_lowercase().contains("plan error"),
        "unexpected error variant: {err}"
    );

    // Clean up: propose DrainEnd so later tests on the same
    // cluster (not that there are any) aren't affected.
    let cleanup_shared = Arc::clone(&leader.shared);
    let cleanup_id = drain_id.clone();
    tokio::task::spawn_blocking(move || {
        let entry = nodedb_cluster::MetadataEntry::DescriptorDrainEnd {
            descriptor_id: cleanup_id,
        };
        let raw = nodedb_cluster::encode_entry(&entry).expect("encode");
        let handle = cleanup_shared.metadata_raft.get().expect("handle");
        handle.propose(raw).expect("propose drain end");
    })
    .await
    .expect("join");

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drain_cleared_mid_retry_succeeds() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION drained_soon")
        .await
        .expect("create");

    wait_for("collection stamped v1", WAIT_BUDGET, POLL, || {
        cluster
            .nodes
            .iter()
            .all(|n| n.collection_descriptor(TENANT, "drained_soon").map(|s| s.0) == Some(1))
    })
    .await;

    let leader = &cluster.nodes[0];
    let drain_id = coll_id("drained_soon");

    // Install drain.
    let shared = Arc::clone(&leader.shared);
    let dstart = drain_id.clone();
    tokio::task::spawn_blocking(move || {
        let now_hlc = shared.hlc_clock.now();
        let expires_at = nodedb_types::Hlc::new(now_hlc.wall_ns.saturating_add(60_000_000_000), 0);
        let entry = nodedb_cluster::MetadataEntry::DescriptorDrainStart {
            descriptor_id: dstart,
            up_to_version: 1,
            expires_at,
        };
        let raw = nodedb_cluster::encode_entry(&entry).expect("encode");
        let handle = shared.metadata_raft.get().expect("handle");
        handle.propose(raw).expect("propose drain start");
    })
    .await
    .expect("join");

    wait_for("drain observed on leader", WAIT_BUDGET, POLL, || {
        leader.has_drain_for(&drain_id, 1)
    })
    .await;

    // Clear the drain after a short delay — the pgwire retry
    // budget is ~350ms, so clearing at 100ms lands mid-retry.
    let cleanup_shared = Arc::clone(&leader.shared);
    let dend = drain_id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        tokio::task::spawn_blocking(move || {
            let entry = nodedb_cluster::MetadataEntry::DescriptorDrainEnd {
                descriptor_id: dend,
            };
            let raw = nodedb_cluster::encode_entry(&entry).expect("encode");
            let handle = cleanup_shared.metadata_raft.get().expect("handle");
            handle.propose(raw).expect("propose drain end");
        })
        .await
        .expect("join");
    });

    // SELECT should succeed once the drain clears mid-retry.
    leader
        .exec("SELECT * FROM drained_soon")
        .await
        .expect("select should succeed after drain clears");

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn shutdown_release_clears_local_leases() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION for_shutdown")
        .await
        .expect("create");

    wait_for("collection stamped", WAIT_BUDGET, POLL, || {
        cluster
            .nodes
            .iter()
            .all(|n| n.collection_descriptor(TENANT, "for_shutdown").is_some())
    })
    .await;

    let leader = &cluster.nodes[0];

    // Planner acquires a lease via SELECT.
    leader
        .exec("SELECT * FROM for_shutdown")
        .await
        .expect("select");
    wait_for("lease held on leader", WAIT_BUDGET, POLL, || {
        leader.has_lease(
            DescriptorKind::Collection,
            TENANT,
            "for_shutdown",
            leader.node_id,
            1,
        )
    })
    .await;

    // Run the shutdown release helper directly (without
    // actually shutting down the process).
    nodedb::control::lease::shutdown_release::release_all_local_leases(
        Arc::clone(&leader.shared),
        Duration::from_secs(2),
    )
    .await;

    // Every node should observe the lease gone.
    wait_for(
        "all nodes observe lease released",
        WAIT_BUDGET,
        POLL,
        || {
            cluster.nodes.iter().all(|n| {
                !n.has_lease(
                    DescriptorKind::Collection,
                    TENANT,
                    "for_shutdown",
                    leader.node_id,
                    1,
                )
            })
        },
    )
    .await;

    cluster.shutdown().await;
}
