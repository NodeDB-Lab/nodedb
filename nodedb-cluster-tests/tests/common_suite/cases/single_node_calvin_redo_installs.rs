// SPDX-License-Identifier: BUSL-1.1

//! A cross-shard, Calvin-committed write installs a stamped redo slice on each
//! participant vShard.
//!
//! The node-global `calvin.counters.redo_installs` counter shows it: the data
//! group's apply loop increments it once per committed slice whose stamped
//! redo installed.

use crate::common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_types::id::VShardId;

use common::cluster_harness::{TestClusterNode, wait_for};

/// Observed sequencer-group leader id from a node's local Raft status, or `0`
/// if no leader is known yet.
fn sequencer_leader(node: &TestClusterNode) -> u64 {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return 0;
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0)
}

/// Count of committed Calvin slices whose stamped redo installed.
fn redo_installs(node: &TestClusterNode) -> u64 {
    node.shared
        .calvin
        .counters
        .redo_installs
        .load(Ordering::Relaxed)
}

/// A `(src, dst)` pair of graph node keys whose home vShards differ, so an edge
/// between them is genuinely cross-shard. Deterministic: `VShardId::from_key` is
/// a pure function of the key bytes.
fn distinct_vshard_node_keys() -> (String, String) {
    let dst = "sncalvin_wv_hub".to_string();
    let vdst = VShardId::from_key(dst.as_bytes()).as_u32();
    for i in 0u32..4096 {
        let src = format!("sncalvin_wv_src_{i}");
        if VShardId::from_key(src.as_bytes()).as_u32() != vdst {
            return (src, dst);
        }
    }
    panic!("could not find a node key on a distinct vShard from the hub in 4096 tries");
}

/// A cross-shard (dual-home edge) Calvin-committed write increments the
/// node-global redo-install counter: each slice's stamped redo installs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_calvin_write_installs_its_redo() {
    // 4 Data-Plane cores so distinct vShards land on distinct cores — a genuine
    // cross-core transaction routed through the Calvin scheduler.
    let node = TestClusterNode::spawn_single_node_calvin(4)
        .await
        .expect("spawn standalone single-node-calvin server");

    // The lone sequencer voter self-elects; wait for it before submitting.
    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    node.client
        .simple_query("CREATE COLLECTION sncalvin_wv_graph")
        .await
        .expect("CREATE COLLECTION sncalvin_wv_graph");
    wait_for(
        "collection visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 1,
    )
    .await;

    let (src, dst) = distinct_vshard_node_keys();
    let installs_before = redo_installs(&node);

    // AUTOCOMMIT cross-shard edge insert: because the endpoints home to distinct
    // vShards, the edge dual-homes atomically through the Calvin scheduler
    // (`submit_calvin_routed`), not the single-home fast path. On commit each
    // participating vShard's data group installs its slice's stamped redo.
    node.client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN 'sncalvin_wv_graph' FROM '{src}' TO '{dst}' TYPE 'l'"
        ))
        .await
        .expect("cross-shard edge insert must commit via the single-node Calvin path");

    // Each participant's data-group apply loop raises the counter on its own
    // schedule, so wait for it to advance.
    wait_for(
        "calvin redo install fired for the cross-shard commit",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || redo_installs(&node) > installs_before,
    )
    .await;

    let installs_after = redo_installs(&node);
    assert!(
        installs_after > installs_before,
        "a cross-shard Calvin-committed write must install its redo \
         (before={installs_before}, after={installs_after})"
    );

    node.shutdown().await;
}
