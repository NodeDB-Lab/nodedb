// SPDX-License-Identifier: BUSL-1.1

//! A cross-shard, Calvin-committed write RECORDS its write-version in the
//! per-core write-version index — closing the gap where Calvin applies advanced
//! no version at all (fast-path writes and reads already do).
//!
//! The version index itself lives on the `!Send` Data-Plane core and its
//! readers are test-only, so this asserts the recording FIRED via the
//! node-global `calvin.counters.write_versions_recorded` counter, which the per-vShard
//! scheduler increments once per committed Calvin apply for which it dispatched
//! a write-version record op (at the CalvinApplied WAL LSN). The counter is the
//! standalone-observable proof that a cross-shard-committed write now advances
//! the version index; the end-to-end serializability regression (via read-set
//! validation) is covered separately once validation is enforcing.

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

/// Count of committed Calvin applies whose write versions were recorded into
/// the per-core write-version index.
fn write_versions_recorded(node: &TestClusterNode) -> u64 {
    node.shared
        .calvin
        .counters
        .write_versions_recorded
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
/// node-global write-version-recorded counter — i.e. its keys' versions are
/// recorded at the CalvinApplied WAL LSN instead of being silently dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_calvin_write_records_write_version() {
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
    let recorded_before = write_versions_recorded(&node);

    // AUTOCOMMIT cross-shard edge insert: because the endpoints home to distinct
    // vShards, the edge dual-homes atomically through the Calvin scheduler
    // (`submit_calvin_routed`), not the single-home fast path. On commit each
    // participating vShard's scheduler records its slice's write versions.
    node.client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN 'sncalvin_wv_graph' FROM '{src}' TO '{dst}' TYPE 'l'"
        ))
        .await
        .expect("cross-shard edge insert must commit via the single-node Calvin path");

    // Recording is dispatched fire-and-forget after the CalvinApplied WAL LSN is
    // obtained, so wait for the counter to advance.
    wait_for(
        "calvin write-version recording fired for the cross-shard commit",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || write_versions_recorded(&node) > recorded_before,
    )
    .await;

    let recorded_after = write_versions_recorded(&node);
    assert!(
        recorded_after > recorded_before,
        "a cross-shard Calvin-committed write must record its write version \
         (before={recorded_before}, after={recorded_after})"
    );

    node.shutdown().await;
}
