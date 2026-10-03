// SPDX-License-Identifier: BUSL-1.1

//! Native-protocol implicit-edge cleanup on a predicate DELETE — cross-node.
//!
//! This test mirrors `calvin_ollp_cross_node::ollp_implicit_edge_delete_cleans_reverse_cross_node`
//! but issues the DELETE via the **native (MessagePack) protocol** (`NativeClient`)
//! instead of pgwire. It proves that the implicit-edge OLLP/Calvin routing gate
//! in `native/dispatch/edge_recon_gate.rs` fires correctly:
//!
//! Without the gate, a `DELETE FROM <coll> WHERE mark = 'del'` issued over the
//! native protocol would classify as `SingleShard` and dispatch directly to the
//! Data Plane — deleting the documents but emitting no edge-delete tasks. The
//! mirrored CSR edges would dangle on their home shards. The post-delete
//! reverse-traversal assertion (only the start node `hub` reachable) can ONLY
//! pass if the edge-cleanup OLLP transaction ran — making this an end-to-end
//! proof of the native gate.
//!
//! Steps:
//! 1. Spawn a 3-node cluster; create a schemaless collection.
//! 2. Insert `src_0..src_11 -> hub` as IMPLICIT edges via pgwire.
//! 3. Wait for full apply convergence and verify all 12 reverse-edges visible.
//! 4. Connect a `NativeClient` to a non-sequencer-leader coordinator node.
//! 5. Issue `DELETE FROM <coll> WHERE mark = 'del'` over the NATIVE protocol.
//! 6. Wait for convergence; assert docs gone + reverse traversal from `hub`
//!    finds only `hub` (no dangling edges).

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

const SOURCES: usize = 12;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_implicit_edge_delete_cleans_reverse_cross_node() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("3-node cluster spawn");

    let coll = "nat_impl_edge_del";

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

    // Wait for a stable sequencer-group leader visible cluster-wide.
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

    // Insert src_0..src_11 -> hub as IMPLICIT edges (plain docs carrying
    // `_from`/`_to`/`_type`). The `mark` field lets the predicate DELETE match
    // all of them without a PK. Distinct source names spread across vShards
    // so several edges are cross-shard (`from_key(src_i) != from_key(hub)`).
    for i in 0..SOURCES {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {coll} {{ id: 'edge_{i}', _from: 'src_{i}', _to: 'hub', \
                 _type: 'l', mark: 'del' }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert implicit edge src_{i} -> hub: {e}"));
    }

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // PRE-DELETE: 1-hop reverse traversal from `hub` must reach all sources on
    // every node (proves cross-shard implicit edges dual-homed onto `hub`).
    for idx in 0..cluster.nodes.len() {
        wait_for(
            &format!("node {idx} reverse-reaches all {SOURCES} sources of hub (pre-delete)"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                cluster.nodes[idx]
                    .traversed_node_ids("GRAPH TRAVERSE IN 'nat_impl_edge_del' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in")
                    .len()
                    > SOURCES
            },
        )
        .await;
    }

    // Pick a coordinator that is NOT the sequencer leader to exercise the
    // non-leader routed OLLP path (same choice as the pgwire mirror test).
    let coordinator_idx = cluster
        .nodes
        .iter()
        .position(|n| n.shared.node_id != leader)
        .expect("a non-sequencer-leader coordinator must exist in a 3-node cluster");
    let coordinator = &cluster.nodes[coordinator_idx];

    // Connect a NativeClient to the coordinator's native listener port,
    // authenticated as the harness's bootstrapped trust superuser (a bare
    // `NativeClient::connect` defaults to trust user `admin`, which this
    // harness never provisions).
    let native = coordinator.native_client();

    // Issue the predicate DELETE via the NATIVE protocol.
    // This exercises the `edge_recon_gate::try_edge_recon_dispatch` path.
    // Without the gate this classifies as SingleShard, deletes docs, but
    // leaves mirrored CSR edges dangling — the post-delete traversal assertion
    // below would then fail.
    native
        .query(&format!("DELETE FROM {coll} WHERE mark = 'del'"))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "native-protocol OLLP implicit-edge BulkDelete on coordinator {} must complete: {e}",
                coordinator.shared.node_id
            )
        });

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // All documents must be gone.
    let count_rows = |msgs: &[tokio_postgres::SimpleQueryMessage]| -> usize {
        msgs.iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count()
    };
    let remaining = coordinator
        .client
        .simple_query(&format!("SELECT * FROM {coll}"))
        .await
        .expect("SELECT remaining docs");
    assert_eq!(
        count_rows(&remaining),
        0,
        "all matched edge documents must be deleted by the native-protocol OLLP transaction"
    );

    // POST-DELETE: the reverse traversal from `hub` must no longer reach ANY
    // source on every node. This is the genuine OLLP proof: a fast-path native
    // dispatch would leave cross-shard reverse-edge copies dangling, making at
    // least some `src_i` still reachable. Only if `edge_recon_gate` fired and
    // the OLLP edge-delete bundle ran will all sources be absent.
    for idx in 0..cluster.nodes.len() {
        wait_for(
            &format!("node {idx} reverse traversal from hub finds no sources after native DELETE"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                let ids = cluster.nodes[idx]
                    .traversed_node_ids("GRAPH TRAVERSE IN 'nat_impl_edge_del' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in");
                // At most the start node `hub` remains. With every edge gone
                // `hub` is absent from the graph and the result is empty.
                ids.iter().all(|id| id == "hub")
            },
        )
        .await;
    }

    cluster.shutdown().await;
}
