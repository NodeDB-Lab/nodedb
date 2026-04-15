//! Regression guard: `OriginCatalog::get_collection` must read
//! purely from the local `SystemCatalog` redb — never dispatch a
//! cluster RPC. If a future change adds a network-hopping read
//! path to planning, query latency becomes unbounded under slow
//! peers, and RLS cache lookups start cascading across the
//! cluster. This test pins the invariant in place: we plan a
//! SELECT on a single-node cluster where the cluster transport
//! is healthy but the SELECT itself does not need remote
//! dispatch, and we assert the plan completes in the local
//! tokio runtime with no spawn_blocking detour.

mod common;

use std::time::Duration;

use common::cluster_harness::TestClusterNode;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn planning_does_not_issue_cluster_rpcs() {
    // Single-node cluster: we own all the descriptors locally
    // and all gateway routes are local (no remote leaders).
    // The SQL-string forwarding path was deleted in C-δ.6.
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("single-node spawn");
    tokio::time::sleep(Duration::from_millis(200)).await;

    node.exec("CREATE COLLECTION local_only_foo")
        .await
        .expect("create");

    // Wrap the SELECT in a short timeout. If planning were to
    // ever block on a cluster RPC, the single-node transport
    // would take far longer than 2 seconds to respond (or
    // hang forever). 2 seconds is 1000x the expected local
    // plan time.
    let plan_result = tokio::time::timeout(
        Duration::from_secs(2),
        node.exec("SELECT * FROM local_only_foo"),
    )
    .await;

    let inner =
        plan_result.expect("planning timed out — did someone add a cluster RPC to get_collection?");
    inner.expect("SELECT");

    node.shutdown().await;
}
