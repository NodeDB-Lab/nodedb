// SPDX-License-Identifier: BUSL-1.1

//! A cross-shard, Calvin-committed write goes through the stage/flush seam:
//! the scheduler STAGES the transaction (validate + buffer, no base mutation),
//! then — because the local commit vote is valid — FLUSHES the staged buffer to
//! base. The write is durable and visible only after the flush.
//!
//! The staged buffer + verdict-driven flush/drop live on the `!Send` Data-Plane
//! core, so this asserts the flush FIRED via the node-global
//! `calvin.counters.commits_flushed` counter (incremented once per staged apply the
//! per-vShard scheduler resolved to commit) plus the functional proof that the
//! flushed write is visible.

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

/// Count of staged Calvin transactions the schedulers resolved to commit by
/// flushing their commit-pending buffer to base.
fn commits_flushed(node: &TestClusterNode) -> u64 {
    node.shared
        .calvin
        .counters
        .commits_flushed
        .load(Ordering::Relaxed)
}

/// A `(src, dst)` pair of graph node keys whose home vShards differ, so an edge
/// between them is genuinely cross-shard.
fn distinct_vshard_node_keys() -> (String, String) {
    let dst = "sncalvin_2p_hub".to_string();
    let vdst = VShardId::from_key(dst.as_bytes()).as_u32();
    for i in 0u32..4096 {
        let src = format!("sncalvin_2p_src_{i}");
        if VShardId::from_key(src.as_bytes()).as_u32() != vdst {
            return (src, dst);
        }
    }
    panic!("could not find a node key on a distinct vShard from the hub in 4096 tries");
}

/// A cross-shard (dual-home edge) Calvin write with a valid (empty) read-set is
/// STAGED then FLUSHED: the node-global flushed counter advances and the edge
/// is visible (a reverse traversal from the destination reaches the source).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_calvin_write_flushes_and_is_visible() {
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
        .simple_query("CREATE COLLECTION sncalvin_2p_graph")
        .await
        .expect("CREATE COLLECTION sncalvin_2p_graph");
    wait_for(
        "collection visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 1,
    )
    .await;

    let (src, dst) = distinct_vshard_node_keys();
    let flushed_before = commits_flushed(&node);

    // AUTOCOMMIT cross-shard edge insert: the endpoints home to distinct vShards,
    // so the edge dual-homes atomically through the Calvin scheduler. Its
    // read-set is empty (autocommit) → the local vote is a commit → each
    // participating vShard STAGES then FLUSHES its slice.
    node.client
        .simple_query(&format!(
            "GRAPH INSERT EDGE IN 'sncalvin_2p_graph' FROM '{src}' TO '{dst}' TYPE 'l'"
        ))
        .await
        .expect("cross-shard edge insert must commit via the staged Calvin path");

    // The flush is dispatched after the stage response returns, so wait for the
    // node-global counter to advance.
    wait_for(
        "calvin commit flushed for the cross-shard write",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || commits_flushed(&node) > flushed_before,
    )
    .await;

    assert!(
        commits_flushed(&node) > flushed_before,
        "a cross-shard Calvin-committed write must flush its staged buffer \
         (before={flushed_before}, after={})",
        commits_flushed(&node)
    );

    // Functional proof the flush applied atomically to base: a reverse (IN)
    // traversal from the destination reaches the source, so the dual-homed edge
    // landed on the destination's home vShard.
    let msgs = node
        .client
        .simple_query(&format!(
            "GRAPH TRAVERSE IN 'sncalvin_2p_graph' FROM '{dst}' DEPTH 1 LABEL 'l' DIRECTION in"
        ))
        .await
        .expect("reverse traversal query");
    let row = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("traverse returned a result row");
    let raw = row.get("result").expect("result column present");
    assert!(
        raw.contains(&src),
        "reverse traversal from '{dst}' must reach flushed source '{src}'; got {raw}"
    );

    node.shutdown().await;
}
