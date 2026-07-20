// SPDX-License-Identifier: BUSL-1.1
//! Asserts that `spawn_post_apply_async_side_effects` fires on
//! followers, not just on the metadata-group leader.
//!
//! The contract documented on `async_dispatch/mod.rs` is that every
//! node runs the async post-apply lane unconditionally (no
//! `is_leader()` gate). If someone reintroduces leader gating, the
//! follower's `_system.wal_tombstones` table will be empty after a
//! `DROP COLLECTION ... PURGE` even though the raft entry applied —
//! catching that silent regression is the whole point of this test.
//!
//! Coverage variants:
//!
//! - `PutCollection` — the create-side async dispatch registers the
//!   collection into every node's local Data Plane.
//! - `PurgeCollection` — the purge-side async dispatch persists a
//!   `(tenant_id, name, purge_lsn)` tombstone into every node's
//!   `_system.wal_tombstones` redb table (plus appends to the local
//!   WAL and dispatches `MetaOp::UnregisterCollection`). The
//!   tombstone presence is the tightest follower-observable effect.

mod common;

use std::time::Duration;

use common::cluster_harness::TestCluster;

/// Locate a node that is NOT the metadata-group leader. Returns its
/// index into `cluster.nodes`.
fn pick_follower_index(cluster: &TestCluster) -> usize {
    let leader_id = cluster
        .nodes
        .iter()
        .map(|n| n.metadata_group_leader())
        .find(|&id| id != 0)
        .expect("at least one node must report a non-zero leader id");
    cluster
        .nodes
        .iter()
        .enumerate()
        .find(|(_, n)| n.node_id != leader_id)
        .map(|(i, _)| i)
        .expect("at least one node must be a follower in a 3-node cluster")
}

/// Tombstone tuples `(tenant_id, collection, purge_lsn)` currently
/// persisted in this node's `_system.wal_tombstones`.
///
/// Tombstones are keyed by database as well as tenant; this test only
/// creates and purges collections in the default database, so entries from
/// any other database are filtered out rather than flattened away.
fn follower_tombstones(node: &common::cluster_harness::TestClusterNode) -> Vec<(u64, String, u64)> {
    let catalog = node.shared.credentials.catalog();
    catalog
        .load_wal_tombstones()
        .expect("load_wal_tombstones")
        .iter()
        .filter(|(database, _, _, _)| *database == nodedb_types::DatabaseId::DEFAULT.as_u64())
        .map(|(_, t, n, l)| (t, n.to_string(), l))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_dispatch_fires_on_follower_for_put_and_purge() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // ── PutCollection: create on the leader, verify every node's
    //    redb catalog has the row after applied-index convergence.
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION follower_dispatch_smoke")
        .await
        .expect("CREATE COLLECTION");

    for node in &cluster.nodes {
        let catalog = node.shared.credentials.catalog();
        let coll = catalog
            .get_collection(
                nodedb_types::DatabaseId::DEFAULT,
                1,
                "follower_dispatch_smoke",
            )
            .expect("get_collection")
            .expect("PutCollection apply must land on every node (leader + followers)");
        assert!(
            coll.is_active,
            "freshly-created collection must be active on node {}",
            node.node_id
        );
    }

    // ── PurgeCollection: drop+purge on the leader, then assert the
    //    follower's `_system.wal_tombstones` has the entry. If the
    //    async lane were gated to leader-only, this would be empty.
    cluster
        .exec_ddl_on_any_leader("DROP COLLECTION follower_dispatch_smoke PURGE")
        .await
        .expect("DROP COLLECTION PURGE");

    let follower_idx = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_idx];

    // purge_async persists the tombstone during the async task spawned
    // off the apply path; applied-index convergence only guarantees the
    // sync post-apply ran. Poll with a short ceiling to cover the async
    // task's fire-and-settle window.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut latest = Vec::new();
    while std::time::Instant::now() < deadline {
        latest = follower_tombstones(follower);
        if latest
            .iter()
            .any(|(_, n, _)| n == "follower_dispatch_smoke")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        latest.iter().any(|(tenant, name, lsn)| *tenant == 1
            && name == "follower_dispatch_smoke"
            && *lsn > 0),
        "follower (node_id={}) must observe the WAL tombstone for the purged collection. \
         If this fails, something reintroduced leader gating on the async post-apply \
         lane — `spawn_post_apply_async_side_effects` must run on every node. \
         Follower tombstones observed: {latest:?}",
        follower.node_id,
    );

    // Symmetric assertion on the leader: the same async lane must fire
    // there too, so both sides of the contract hold.
    let leader = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == n.metadata_group_leader())
        .expect("cluster must have a stable leader");
    let leader_tombstones = follower_tombstones(leader);
    assert!(
        leader_tombstones
            .iter()
            .any(|(tenant, name, _)| *tenant == 1 && name == "follower_dispatch_smoke"),
        "leader (node_id={}) must also persist the WAL tombstone. \
         Leader tombstones observed: {leader_tombstones:?}",
        leader.node_id,
    );
}
