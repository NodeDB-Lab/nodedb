// SPDX-License-Identifier: BUSL-1.1

//! Cross-node OLLP implicit-edge UPDATE lifecycle test.
//!
//! File name contains "cluster" via the cluster-tests crate so nextest applies
//! the cluster test-group serialization.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// IMPLICIT-edge relocation on a predicate UPDATE that moves an edge endpoint,
/// cross-shard.
///
/// Changing `_to` from `hub_old` to `hub_new` must atomically emit
/// `EdgeDelete(src_i→hub_old)` + `EdgePut(src_i→hub_new)` in the same Calvin
/// transaction, cross-shard-correctly. A fast-path `BulkUpdate` without the OLLP
/// edge-update bundle would mutate the document but leave old reverse copies
/// dangling on `hub_old`'s shard and never place new ones on `hub_new`'s shard.
///
/// Seeds `src_0..src_11 → hub_old`, confirms IN-direction traversal from `hub_old`
/// reaches every source, then runs a predicate UPDATE from a NON-leader coordinator
/// and asserts: (a) reverse traversal from `hub_old` finds nothing, (b) reverse
/// traversal from `hub_new` finds all sources. Both can only pass if the OLLP
/// edge-update bundle ran — genuine cross-node OLLP proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ollp_implicit_edge_update_moves_edge_cross_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let coll = "ollp_impl_edge_upd";

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
        ))
        .await
        .expect("CREATE COLLECTION");

    wait_for(
        "all 3 nodes see the collection",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    // Stable, cluster-wide-visible sequencer-group leader.
    wait_for(
        "sequencer-group leader elected and visible on every node",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|n| n.sequencer_leader() != 0)
                && cluster
                    .nodes
                    .iter()
                    .all(|n| n.sequencer_leader() == cluster.nodes[0].sequencer_leader())
        },
    )
    .await;

    let leader = cluster.nodes[0].sequencer_leader();
    assert_ne!(leader, 0, "sequencer leader must be elected");

    // src_0..src_11 -> hub_old as IMPLICIT edges. Distinct source names spread
    // across vShards so several edges are cross-shard.
    const SOURCES: usize = 12;
    for i in 0..SOURCES {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {coll} {{ id: 'edge_{i}', _from: 'src_{i}', _to: 'hub_old', _type: 'l', mark: 'move' }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert implicit edge src_{i} -> hub_old: {e}"));
    }

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // PRE-UPDATE: IN-direction traversal from `hub_old` must reach all sources on
    // every node — proves the cross-shard implicit edges dual-homed onto `hub_old`.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} reverse-reaches all {SOURCES} sources of hub_old"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                node
                    .traversed_node_ids(
                        "GRAPH TRAVERSE IN 'ollp_impl_edge_upd' FROM 'hub_old' DEPTH 1 LABEL 'l' DIRECTION in",
                    )
                    .len()
                    > SOURCES
            },
        )
        .await;
    }

    // Coordinate from a NON-sequencer-leader node (mirrors the delete tests).
    let coordinator = cluster
        .nodes
        .iter()
        .find(|n| n.shared.node_id != leader)
        .expect("a non-sequencer-leader coordinator must exist in a 3-node cluster");

    coordinator
        .client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    // Predicate UPDATE ⇒ BulkUpdate ⇒ OLLP dependent-read path. The implicit
    // edge of every matched document must be atomically relocated: old reverse
    // copy removed from hub_old's shard, new reverse copy written to hub_new's
    // shard, in the same Calvin transaction.
    coordinator
        .client
        .simple_query(&format!(
            "UPDATE {coll} SET _to = 'hub_new' WHERE mark = 'move'"
        ))
        .await
        .expect("OLLP implicit-edge predicate UPDATE from a non-leader coordinator must complete");

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // POST-UPDATE: old reverse copies cleaned up — `hub_old` should reach nothing.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} reverse traversal from hub_old finds no sources after update"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                let ids = node.traversed_node_ids(
                    "GRAPH TRAVERSE IN 'ollp_impl_edge_upd' FROM 'hub_old' DEPTH 1 LABEL 'l' DIRECTION in",
                );
                // Only the start node `hub_old` itself should remain (or nothing).
                ids.iter().all(|id| id == "hub_old")
            },
        )
        .await;
    }

    // POST-UPDATE: new reverse copies placed cross-shard — `hub_new` must reach
    // all sources. This is the genuine OLLP proof: a fast-path BulkUpdate never
    // emits cross-shard EdgePut ops so hub_new would be unreachable without OLLP.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} reverse traversal from hub_new reaches all {SOURCES} sources"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                let ids = node.traversed_node_ids(
                    "GRAPH TRAVERSE IN 'ollp_impl_edge_upd' FROM 'hub_new' DEPTH 1 LABEL 'l' DIRECTION in",
                );
                (0..SOURCES).all(|i| ids.iter().any(|id| id == &format!("src_{i}")))
            },
        )
        .await;
    }

    cluster.shutdown().await;
}
