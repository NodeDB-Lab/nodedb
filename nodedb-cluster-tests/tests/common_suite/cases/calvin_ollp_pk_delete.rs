// SPDX-License-Identifier: BUSL-1.1

//! IMPLICIT-edge cleanup on a PK-EQUALITY (`PointDelete`-shaped) OLLP delete,
//! cross-shard.
//!
//! `DELETE FROM coll WHERE id = 'x'` resolves the PK statically, so without the
//! edge-bearing routing gate it lowers to `DocumentOp::PointDelete` — NOT a
//! dependent predicate — and bypasses the OLLP edge-cleanup path entirely,
//! leaking the implicit edge. The gate routes such a delete on an edge-bearing
//! collection as a `BulkDelete` (with an `id = 'x'` filter) so it flows through
//! the same OLLP/Calvin dependent path as the predicate delete above.
//!
//! This seeds the same cross-shard `src_i -> hub` implicit edges, confirms the
//! reverse traversal from `hub` reaches all sources, then deletes EXACTLY ONE
//! document by PK (`id = 'edge_3'`) and asserts: that doc is gone, `src_3` is no
//! longer reverse-reachable from `hub` (its implicit edge was cleaned by the
//! OLLP txn), while every OTHER source remains reachable (their edges untouched).
//!
//! File name contains "cluster" via the cluster-tests crate so nextest applies
//! the cluster test-group serialization.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// IMPLICIT-edge cleanup on a PK-EQUALITY (`PointDelete`-shaped) OLLP delete,
/// cross-shard.
///
/// `DELETE FROM coll WHERE id = 'x'` resolves the PK statically, so without the
/// edge-bearing routing gate it lowers to `DocumentOp::PointDelete` — NOT a
/// dependent predicate — and bypasses the OLLP edge-cleanup path entirely,
/// leaking the implicit edge. The gate routes such a delete on an edge-bearing
/// collection as a `BulkDelete` (with an `id = 'x'` filter) so it flows through
/// the same OLLP/Calvin dependent path as the predicate delete above.
///
/// This seeds the same cross-shard `src_i -> hub` implicit edges, confirms the
/// reverse traversal from `hub` reaches all sources, then deletes EXACTLY ONE
/// document by PK (`id = 'edge_3'`) and asserts: that doc is gone, `src_3` is no
/// longer reverse-reachable from `hub` (its implicit edge was cleaned by the
/// OLLP txn), while every OTHER source remains reachable (their edges untouched).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ollp_implicit_edge_pk_delete_cleans_reverse_cross_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let coll = "ollp_impl_edge_pk_del";

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

    // src_0..src_11 -> hub as IMPLICIT edges (plain docs carrying _from/_to/_type).
    // Distinct source names spread across vShards so several edges are cross-shard.
    const SOURCES: usize = 12;
    for i in 0..SOURCES {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {coll} {{ id: 'edge_{i}', _from: 'src_{i}', _to: 'hub', _type: 'l' }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert implicit edge src_{i} -> hub: {e}"));
    }

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // PRE-DELETE: a 1-hop reverse traversal from `hub` must reach all sources on
    // every node — proves the cross-shard implicit edges dual-homed onto `hub`.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} reverse-reaches all {SOURCES} sources of hub"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                node
                    .traversed_node_ids("GRAPH TRAVERSE IN 'ollp_impl_edge_pk_del' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in")
                    .len()
                    > SOURCES
            },
        )
        .await;
    }

    // Coordinate the PK-equality DELETE from a NON-sequencer-leader node.
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

    // PK-equality DELETE. Without the edge-bearing routing gate this lowers to a
    // static `PointDelete` and leaks `src_3`'s implicit edge; with the gate it is
    // routed as a `BulkDelete` (id = 'edge_3' filter) through the OLLP path that
    // derives + drift-validates the `EdgeDelete`.
    coordinator
        .client
        .simple_query(&format!("DELETE FROM {coll} WHERE id = 'edge_3'"))
        .await
        .expect("OLLP implicit-edge PK delete from a non-leader coordinator must complete");

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // Exactly one document (`edge_3`) is gone; the other SOURCES-1 remain.
    let count_rows = |msgs: &[tokio_postgres::SimpleQueryMessage]| -> usize {
        msgs.iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count()
    };
    let remaining = coordinator
        .client
        .simple_query(&format!("SELECT * FROM {coll}"))
        .await
        .expect("SELECT all rows");
    assert_eq!(
        count_rows(&remaining),
        SOURCES - 1,
        "exactly the PK-targeted edge document must be deleted"
    );

    // POST-DELETE: the reverse traversal from `hub` must no longer reach `src_3`
    // (its implicit edge was cleaned by the OLLP txn) while EVERY other source
    // stays reachable. If the PK delete had taken the static `PointDelete` path,
    // `src_3`'s cross-shard reverse edge would dangle on hub's home shard and
    // `src_3` would still be reverse-reachable — failing this assertion.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} reverse traversal from hub drops only src_3 after PK delete"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                let ids = node
                    .traversed_node_ids("GRAPH TRAVERSE IN 'ollp_impl_edge_pk_del' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in");
                let src3_gone = !ids.iter().any(|id| id == "src_3");
                let others_present = (0..SOURCES)
                    .filter(|i| *i != 3)
                    .all(|i| ids.iter().any(|id| id == &format!("src_{i}")));
                src3_gone && others_present
            },
        )
        .await;
    }

    cluster.shutdown().await;
}
