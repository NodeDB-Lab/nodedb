// SPDX-License-Identifier: BUSL-1.1
//! 3-node cluster test for the collection hard-delete pipeline.
//!
//! Asserts that `DROP COLLECTION ... PURGE` issued on one node
//! causes every follower to:
//!   1. Remove the `StoredCollection` catalog row (post_apply sync +
//!      async `PurgeCollection` handler ran).
//!   2. Persist the WAL tombstone into `_system.wal_tombstones` (async
//!      post-apply dispatch reached the follower).
//!   3. Complete both of the above within a bounded time.
//!
//! Observable proof that the hard-delete reclaim reached every node
//! symmetrically — if someone reintroduces leader gating on either
//! the sync or async post-apply lane, one of these two assertions
//! fails within the poll window.

mod common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, TestClusterNode};

fn catalog_has_collection(node: &TestClusterNode, name: &str) -> bool {
    let cat = node.shared.credentials.catalog();
    match cat.get_collection(nodedb_types::DatabaseId::DEFAULT, 1, name) {
        Ok(Some(c)) => c.is_active,
        _ => false,
    }
}

fn has_tombstone(node: &TestClusterNode, name: &str) -> bool {
    let cat = node.shared.credentials.catalog();
    cat.load_wal_tombstones()
        .map(|set| {
            // Tombstones are keyed by database as well as tenant; this helper
            // only ever asks about collections in the default database.
            set.iter().any(|(database, tenant, n, lsn)| {
                database == nodedb_types::DatabaseId::DEFAULT.as_u64()
                    && tenant == 1
                    && n == name
                    && lsn > 0
            })
        })
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_purge_reclaims_on_every_follower() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // Create the collection on any leader, wait for fan-out.
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION cluster_hard_delete_smoke")
        .await
        .expect("CREATE COLLECTION");

    // Every node must see the collection as active.
    for node in &cluster.nodes {
        assert!(
            catalog_has_collection(node, "cluster_hard_delete_smoke"),
            "node {} missing freshly-created collection",
            node.node_id
        );
    }

    // Purge on any leader.
    cluster
        .exec_ddl_on_any_leader("DROP COLLECTION cluster_hard_delete_smoke PURGE")
        .await
        .expect("DROP COLLECTION PURGE");

    // Poll every node for the combined post-purge state:
    //   - the catalog row is gone (apply ran + purge applier removed it)
    //   - `_system.wal_tombstones` has the entry (async dispatch ran
    //     on the node, persisting the tombstone)
    // The async post-apply dispatch can run slightly after the sync
    // apply completes, so a short poll loop covers the fire-and-settle
    // window. 5s is generous — typical convergence is <200ms.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);

    for node in &cluster.nodes {
        let mut row_purged = false;
        let mut tombstone_present = false;
        while std::time::Instant::now() < deadline {
            row_purged = !catalog_has_collection(node, "cluster_hard_delete_smoke");
            tombstone_present = has_tombstone(node, "cluster_hard_delete_smoke");
            if row_purged && tombstone_present {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            row_purged,
            "node {} still has StoredCollection row after DROP PURGE — \
             apply/purge path did not run on this node",
            node.node_id
        );
        assert!(
            tombstone_present,
            "node {} missing _system.wal_tombstones entry for the purged \
             collection — async post-apply dispatch did not fan out to \
             this node (leader-gating regression?)",
            node.node_id
        );
    }

    // Purging a missing collection is idempotent — re-issuing the
    // PURGE must not return an error (the handler's absent-collection
    // branch short-circuits to success).
    cluster
        .exec_ddl_on_any_leader("DROP COLLECTION cluster_hard_delete_smoke PURGE")
        .await
        .expect("idempotent PURGE on already-purged collection must succeed");
}
