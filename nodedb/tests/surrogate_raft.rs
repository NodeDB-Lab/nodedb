//! 3-node cluster integration test: surrogate hwm replication and
//! monotonicity across a metadata-group leader failover.
//!
//! Background (from S5 design notes):
//!   - `SurrogateAlloc` (the hwm) is Raft-replicated via
//!     `MetadataEntry::SurrogateAlloc`.  Every node persists the hwm
//!     to `_system.surrogate_hwm` in its local redb catalog on apply.
//!   - `SurrogateBind` (individual pk↔surrogate mappings) is NOT
//!     Raft-replicated; each node re-derives bindings from the
//!     engine-write WAL replay path.
//!
//! What this test validates:
//!   1. After N inserts on the cluster, the hwm on ALL three nodes is
//!      non-zero and equal (hwm replication converges).
//!   2. After gracefully shutting down the metadata-group leader, the
//!      two surviving nodes elect a new metadata leader (quorum = 2).
//!   3. The surviving nodes' hwms equal the pre-failover hwm — no
//!      surrogate was reassigned and no hwm regressed.
//!   4. Additional inserts on the new leader succeed without
//!      unique-key violations (monotonic hwm allocation post-failover).
//!
//! Post-failover DML via pgwire may be transiently unavailable while the
//! data Raft group re-elects.  The test retries DML for a generous window
//! so transient "cluster in leader election" responses do not cause flakes.

mod common;
use common::cluster_harness::{TestCluster, wait::wait_for};

use std::time::Duration;

// ── helpers ────────────────────────────────────────────────────────────────

/// Read the persisted surrogate hwm from the given node's local redb catalog.
fn catalog_hwm(shared: &std::sync::Arc<nodedb::control::state::SharedState>) -> u32 {
    shared
        .credentials
        .catalog()
        .as_ref()
        .and_then(|c| c.get_surrogate_hwm().ok())
        .unwrap_or(0)
}

// ── test ───────────────────────────────────────────────────────────────────

/// Hwm replication and monotonicity survive a metadata-group leader failover.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn surrogate_hwm_survives_leader_failover() {
    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // Create a STRICT collection — each INSERT triggers surrogate allocation
    // via the SurrogateAssigner on the inserting node's Control Plane.
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION failover_sr TYPE DOCUMENT STRICT \
             (id TEXT PRIMARY KEY, val TEXT)",
        )
        .await
        .expect("CREATE COLLECTION failover_sr");

    // ── Step 1: identify the metadata-group leader ────────────────────────
    let leader_id = cluster.nodes[0].metadata_group_leader();
    assert_ne!(leader_id, 0, "no metadata-group leader elected");

    let leader_idx = cluster
        .nodes
        .iter()
        .position(|n| n.node_id == leader_id)
        .unwrap_or(0);

    // ── Step 2: insert rows, wait for hwm replication ─────────────────────
    // We retry each INSERT because the data vshard may still be electing
    // its leader in the first few hundred milliseconds after cluster formation.
    const ROWS_BEFORE: u32 = 3;
    for i in 0..ROWS_BEFORE {
        let pk = format!("pre_{i}");
        let sql = format!("INSERT INTO failover_sr (id, val) VALUES ('{pk}', 'v')");
        wait_for(
            &format!("INSERT {pk} accepted"),
            Duration::from_secs(15),
            Duration::from_millis(200),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(cluster.nodes[0].client.simple_query(&sql))
                })
                .is_ok()
            },
        )
        .await;
    }

    // Wait until all 3 nodes report a non-zero hwm.  The hwm is flushed to
    // Raft by the SurrogateAssigner on the inserting node's Control Plane
    // (at every FLUSH_OPS_THRESHOLD allocations and on shutdown).  The
    // inserting node may flush the hwm as part of the transaction commit; in
    // that case followers apply it through the metadata Raft apply path.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} catalog hwm is non-zero"),
            Duration::from_secs(10),
            Duration::from_millis(50),
            || catalog_hwm(&node.shared) > 0,
        )
        .await;
    }

    // Wait for all 3 nodes' hwm to converge to the same value before sampling.
    // With the cross-engine Raft proposer wired up, hwm advancement now flows
    // through the same propose+commit+apply path as other writes, so followers
    // catch up on Raft tick cadence rather than the prior best-effort path.
    wait_for(
        "all 3 nodes' hwm converged",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            let hwms: Vec<u32> = cluster
                .nodes
                .iter()
                .map(|n| catalog_hwm(&n.shared))
                .collect();
            let max = hwms.iter().copied().max().unwrap_or(0);
            max > 0 && hwms.iter().all(|&h| h == max)
        },
    )
    .await;

    let hwms_before: Vec<u32> = cluster
        .nodes
        .iter()
        .map(|n| catalog_hwm(&n.shared))
        .collect();
    let pre_failover_hwm = hwms_before.iter().copied().max().unwrap_or(0);
    assert!(
        pre_failover_hwm > 0,
        "pre-failover hwm must be non-zero (surrogates were allocated)"
    );
    for (idx, &hwm) in hwms_before.iter().enumerate() {
        assert_eq!(
            hwm, pre_failover_hwm,
            "node {idx} hwm {hwm} differs from expected {pre_failover_hwm} after wait_for"
        );
    }

    // ── Step 3: shut down the metadata leader ────────────────────────────
    // We remove the Raft metadata-group leader.  The two surviving nodes
    // form a quorum (2/3) and will elect a new metadata leader.
    let leader_node = cluster.nodes.remove(leader_idx);
    leader_node.shutdown().await;
    assert_eq!(cluster.nodes.len(), 2);

    // ── Step 4: wait for the two survivors to elect a new leader ─────────
    wait_for(
        "two surviving nodes elect a new metadata group leader",
        Duration::from_secs(20),
        Duration::from_millis(100),
        || {
            let leaders: Vec<u64> = cluster
                .nodes
                .iter()
                .map(|n| n.metadata_group_leader())
                .collect();
            // Both must agree on a stable non-zero leader, not the removed one.
            let first = leaders[0];
            first != 0 && first != leader_id && leaders.iter().all(|&l| l == first)
        },
    )
    .await;

    // ── Step 4b: wait for data group (group 1) to elect a leader ────────────
    // The data vshard Raft group (group 1) may need to re-elect after losing
    // its third member.  The metadata group leader wait above (step 4) is
    // not sufficient — the two Raft groups elect independently.
    wait_for(
        "two surviving nodes elect a new data group leader",
        Duration::from_secs(20),
        Duration::from_millis(100),
        || {
            let leaders: Vec<u64> = cluster
                .nodes
                .iter()
                .map(|n| n.data_group_leader())
                .collect();
            // Both surviving nodes must agree on a non-zero leader for group 1.
            // The data group leader may be any surviving node.
            let first = leaders[0];
            first != 0 && leaders.iter().all(|&l| l == first)
        },
    )
    .await;

    // ── Step 5: assert hwm agreement on surviving nodes ───────────────────
    // The hwm must survive failover: neither surviving node may have an hwm
    // lower than the pre-failover value, and the two survivors must agree.
    let hwm_survivor_0 = catalog_hwm(&cluster.nodes[0].shared);
    let hwm_survivor_1 = catalog_hwm(&cluster.nodes[1].shared);
    assert_eq!(
        hwm_survivor_0, hwm_survivor_1,
        "surviving nodes must agree on hwm after failover: \
         node 0 hwm = {hwm_survivor_0}, node 1 hwm = {hwm_survivor_1}"
    );
    assert_eq!(
        hwm_survivor_0, pre_failover_hwm,
        "surviving node 0 hwm must not regress after failover: \
         got {hwm_survivor_0}, expected {pre_failover_hwm}"
    );

    // ── Step 6: insert more rows on the new leader ────────────────────────
    // After failover, the new metadata leader resumes surrogate allocation
    // from the recovered hwm.  Inserts that succeed prove the data group
    // also recovered (it forms a quorum with the 2 surviving nodes).
    // We allow a generous retry window because the data vshard group may
    // need additional time to elect a new leader after the metadata leader
    // was removed.
    let new_leader_id = cluster.nodes[0].metadata_group_leader();
    let new_leader_idx = cluster
        .nodes
        .iter()
        .position(|n| n.node_id == new_leader_id)
        .unwrap_or(0);

    let mut post_failover_inserts = 0u32;
    for i in 0..3u32 {
        let pk = format!("post_{i}");
        let sql = format!("INSERT INTO failover_sr (id, val) VALUES ('{pk}', 'pv')");
        let label = format!("INSERT post-failover {pk} accepted (or data group not yet ready)");
        wait_for(
            &label,
            Duration::from_secs(25),
            Duration::from_millis(300),
            || {
                let result = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(cluster.nodes[new_leader_idx].client.simple_query(&sql))
                });
                result.is_ok()
            },
        )
        .await;
        post_failover_inserts += 1;
    }
    // If wait_for panicked (predicate never true), the test would already
    // have failed; reaching here means all 3 post-failover inserts succeeded.
    assert_eq!(post_failover_inserts, 3);

    // ── Step 7: hwm monotonicity after post-failover inserts ─────────────
    // The hwm on both surviving nodes must be >= the pre-failover value.
    // (It may equal pre_failover_hwm if the new allocations happen to fit
    // within the already-flushed hwm block, which is fine — the invariant
    // is that the hwm never decreases, not that it strictly increases per
    // insert.)
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let hwm = catalog_hwm(&node.shared);
        assert!(
            hwm >= pre_failover_hwm,
            "surviving node {idx} post-insert hwm {hwm} must be >= \
             pre-failover hwm {pre_failover_hwm}"
        );
    }

    // ── Shutdown ──────────────────────────────────────────────────────────
    let mut nodes = cluster.nodes;
    while let Some(node) = nodes.pop() {
        node.shutdown().await;
    }
}
