// SPDX-License-Identifier: BUSL-1.1

//! A node that dies holding the DDL preparation lease stops blocking DDL once
//! the metadata leader knows it is dead, long before the lease's 60s
//! stuck-owner fallback. Its late entries apply nothing.

use crate::common;

use std::time::{Duration, Instant};

use common::cluster_harness::shared_steps::propose_and_apply;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};
use nodedb_cluster::MetadataEntry;

const POLL: Duration = Duration::from_millis(50);
/// SWIM declares the owner Dead within a probe round plus a 3-node
/// suspicion timeout. The leader then waits the dead-holder grace, and polls.
const RECLAIM_BUDGET: Duration = Duration::from_secs(45);
/// The stuck-owner fallback a dead owner must not have to wait out.
const STUCK_OWNER_LEASE: Duration = Duration::from_secs(60);
const DEAD_TOKEN: u64 = 0x00dd_1ea5;

fn owner_token(node: &TestClusterNode) -> Option<u64> {
    node.shared
        .metadata_ddl
        .owner
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .map(|owner| owner.token)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_dead_ddl_lease_owner_is_reclaimed_before_its_lease_runs_out() {
    let mut cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    // The owner is a node that does not lead the metadata group, so the
    // survivors keep their metadata leader and quorum.
    let metadata_leader = cluster.nodes[0].metadata_group_leader();
    let owner_idx = cluster
        .nodes
        .iter()
        .position(|n| n.node_id != metadata_leader)
        .expect("a node that does not lead the metadata group");
    let owner_id = cluster.nodes[owner_idx].node_id;

    // The owner is mid-prepare: it holds the lease and has reserved a
    // pending DDL under it.
    propose_and_apply(
        &cluster.nodes[owner_idx],
        &MetadataEntry::DdlPrepareAcquire {
            token: DEAD_TOKEN,
            node_id: owner_id,
        },
    )
    .await;
    propose_and_apply(
        &cluster.nodes[owner_idx],
        &MetadataEntry::DdlPendingPropose {
            token: DEAD_TOKEN,
            objects: Vec::new(),
            proposed_at: cluster.nodes[owner_idx].shared.hlc_clock.now(),
        },
    )
    .await;
    wait_for(
        "every node sees the owner's lease and pending record",
        Duration::from_secs(10),
        POLL,
        || {
            cluster.nodes.iter().all(|n| {
                owner_token(n) == Some(DEAD_TOKEN) && n.shared.pending_ddl.contains(DEAD_TOKEN)
            })
        },
    )
    .await;

    // A harness shutdown never releases the lease, so the owner dies with it.
    let owner = cluster.nodes.remove(owner_idx);
    owner.shutdown().await;
    let killed_at = Instant::now();

    wait_for(
        "the metadata leader reclaims the dead owner's lease",
        RECLAIM_BUDGET,
        POLL,
        || {
            cluster.nodes.iter().all(|n| {
                owner_token(n) != Some(DEAD_TOKEN) && !n.shared.pending_ddl.contains(DEAD_TOKEN)
            })
        },
    )
    .await;
    assert!(
        killed_at.elapsed() < STUCK_OWNER_LEASE,
        "the reclaim took {:?}: the dead owner waited out the stuck-owner fallback",
        killed_at.elapsed()
    );

    // A survivor's DDL proceeds.
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION reclaim_after_dead_owner")
        .await
        .expect("a survivor's DDL proceeds once the lease is reclaimed");
    let survivor = &cluster.nodes[0];

    // The dead owner's late entries, replayed through the log, apply nothing.
    propose_and_apply(
        survivor,
        &MetadataEntry::DdlPendingPropose {
            token: DEAD_TOKEN,
            objects: Vec::new(),
            proposed_at: survivor.shared.hlc_clock.now(),
        },
    )
    .await;
    propose_and_apply(
        survivor,
        &MetadataEntry::DdlPendingFinalize { token: DEAD_TOKEN },
    )
    .await;
    assert!(
        !survivor.shared.pending_ddl.contains(DEAD_TOKEN),
        "a reclaimed token reserves nothing"
    );
    assert_ne!(
        survivor
            .shared
            .metadata_ddl
            .applied_token
            .load(std::sync::atomic::Ordering::Acquire),
        DEAD_TOKEN,
        "a reclaimed token's finalize applies nothing"
    );

    cluster.shutdown().await;
}
