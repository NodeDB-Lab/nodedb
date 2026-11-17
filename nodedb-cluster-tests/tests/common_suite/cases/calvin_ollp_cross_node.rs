// SPDX-License-Identifier: BUSL-1.1

//! Cross-node OLLP dependent-write test.
//!
//! Validates that a dependent (value-predicate ⇒ `BulkDelete`) cross-shard Calvin
//! write submitted on a NON-sequencer-leader coordinator completes. The
//! coordinator owns the OLLP retry loop (`run_dependent_with_retry`); its `submit`
//! step routes the inbox submit to the sequencer-group leader via
//! `submit_calvin_routed_assign` (the only node whose sequencer service assigns)
//! and awaits completion on its local registry (which receives the replicated
//! completion ack on every sequencer-group member), while still passing through
//! the coordinator's circuit-breaker / tenant-budget gate.
//!
//! Steps:
//! 1. Bring up the standard 3-node cluster (`TestCluster::spawn_three`).
//! 2. Create a collection and wait for convergence on every node plus a stable
//!    sequencer-group leader.
//! 3. INSERT 3 rows; wait for full apply convergence.
//! 4. Pick a coordinator node that is NOT the sequencer leader.
//! 5. From that non-leader coordinator, run
//!    `DELETE FROM <coll> WHERE status = 'inactive'` — a non-PK predicate ⇒
//!    `BulkDelete` ⇒ the dependent/OLLP path. Assert it SUCCEEDS.
//! 6. Assert only the 2 'active' rows remain (the delete actually committed).
//! 7. Shut down.
//!
//! This validates the HAPPY path only. It does NOT force a predicate-drift
//! mismatch — mismatch / re-scan / exhaustion is already covered by the
//! coordinator-loop unit tests (`retry_loop_tests.rs`).
//!
//! `spawn_three` gives RF=3 (every shard on every node), so the coordinator's
//! local pre-exec reconnaissance scan sees the inserted rows without any special
//! scan routing.
//!
//! ## Coverage honesty
//!
//! `ollp_dependent_delete_from_non_leader_coordinator_completes` (below) is a
//! WEAK assertion of OLLP traversal: its collection carries NO edges, so the
//! edge-bearing routing gate does NOT trip and the `BulkDelete` actually runs on
//! the SingleShard FAST PATH — not OLLP. It proves only that a non-leader
//! single-shard dependent delete COMPLETES and commits; it does NOT prove the
//! transaction went through the OLLP/Calvin path. The GENUINE cross-node OLLP
//! coverage is `ollp_implicit_edge_delete_cleans_reverse_cross_node`, whose
//! post-delete reverse-traversal assertion can only pass if the OLLP edge-delete
//! bundle ran. This test is kept (not deleted) as a useful smoke check of the
//! non-leader coordinator completion path; it is intentionally NOT forced
//! through OLLP.
//!
//! File name contains "cluster" via the cluster-tests crate so nextest applies
//! the cluster test-group serialization.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// HOLLOW w.r.t. OLLP: this collection has no edges, so the edge-bearing routing
/// gate does not trip and the predicate `DELETE` runs on the SingleShard fast
/// path, NOT through OLLP. It proves a non-leader single-shard dependent delete
/// COMPLETES and commits — nothing more. For genuine cross-node OLLP coverage
/// see `ollp_implicit_edge_delete_cleans_reverse_cross_node`. Kept as a
/// non-leader-completion smoke check; intentionally not forced through OLLP.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ollp_dependent_delete_from_non_leader_coordinator_completes() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let coll = "ollp_xnode_delete";

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {coll} (id TEXT PRIMARY KEY, status TEXT)"
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

    // Insert 3 rows from node 0; two 'active', one 'inactive'.
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {coll} (id, status) VALUES \
             ('a', 'active'), ('b', 'inactive'), ('c', 'active')"
        ))
        .await
        .expect("INSERT 3 rows");

    // Deterministic barrier: every Raft group has fully propagated the inserts.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // Pick a coordinator node that is NOT the sequencer leader — this is the case
    // the routed OLLP submit must handle: a non-leader coordinator drives the
    // dependent cross-shard write to completion.
    let coordinator = cluster
        .nodes
        .iter()
        .find(|n| n.shared.node_id != leader)
        .expect("a non-sequencer-leader coordinator must exist in a 3-node cluster");
    assert_ne!(
        coordinator.shared.node_id, leader,
        "coordinator must not be the sequencer leader for this test to be meaningful"
    );

    // Enable strict cross-shard mode so the predicate DELETE routes through Calvin.
    coordinator
        .client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    // The dependent write: a non-PK predicate ⇒ `BulkDelete` ⇒ the OLLP path. On a
    // non-leader coordinator this must complete (route to leader for assignment,
    // complete via the replicated ack on the local registry).
    coordinator
        .client
        .simple_query(&format!("DELETE FROM {coll} WHERE status = 'inactive'"))
        .await
        .expect(
            "dependent (BulkDelete) cross-shard write from a non-leader coordinator must complete",
        );

    // Deterministic barrier: Calvin completion guarantees one ack per vshard,
    // not that every replica has applied. Under RF>1 the coordinator hosts its
    // own (possibly lagging) replica of the data group, so wait for every
    // replica — including this coordinator's — to apply the routed delete
    // before reading it back, exactly as the sibling edge-delete test does.
    cluster
        .wait_for_full_apply_convergence(std::time::Duration::from_secs(15))
        .await;

    // Prove the delete committed: only the 2 'active' rows remain.
    let count_rows = |msgs: &[tokio_postgres::SimpleQueryMessage]| -> usize {
        msgs.iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count()
    };

    let rows = coordinator
        .client
        .simple_query(&format!("SELECT * FROM {coll}"))
        .await
        .expect("SELECT all rows");
    assert_eq!(
        count_rows(&rows),
        2,
        "only the 2 'active' rows must remain after the routed OLLP delete"
    );

    cluster.shutdown().await;
}

/// IMPLICIT-edge cleanup on a predicate (`BulkDelete`) OLLP delete, cross-shard.
///
/// Schemaless documents carrying `_from`/`_to` auto-create a graph edge on
/// INSERT (the Control-Plane implicit-edge extraction). The symmetric invariant:
/// when such a document is removed via a predicate `DELETE` (a `BulkDelete` ⇒
/// the OLLP dependent-read Calvin path), the implicit edge must ALSO be deleted,
/// atomically in the SAME Calvin transaction and cross-shard-correctly.
///
/// This test seeds many `src_i -> hub` implicit edges (distinct source names so a
/// meaningful fraction are cross-shard, `from_key(src_i) != from_key(hub)`),
/// confirms a reverse (in-direction) traversal from `hub` finds them all, then
/// runs a predicate `DELETE` from a NON-leader coordinator and asserts the docs
/// are gone AND the reverse traversal from `hub` no longer reaches any source —
/// i.e. the cross-shard implicit edges were cleaned up by the same OLLP txn.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ollp_implicit_edge_delete_cleans_reverse_cross_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let coll = "ollp_impl_edge_del";

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

    // src_0..src_11 -> hub as IMPLICIT edges (plain docs carrying _from/_to/_type).
    // A `mark` field lets the predicate DELETE match them all without a PK. The
    // distinct source names spread across vShards so several edges are cross-shard.
    const SOURCES: usize = 12;
    for i in 0..SOURCES {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {coll} {{ id: 'edge_{i}', _from: 'src_{i}', _to: 'hub', _type: 'l', mark: 'del' }}"
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
                    .traversed_node_ids("GRAPH TRAVERSE IN 'ollp_impl_edge_del' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in")
                    .len()
                    > SOURCES
            },
        )
        .await;
    }

    // Coordinate the predicate DELETE from a NON-sequencer-leader node.
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

    // Predicate DELETE ⇒ BulkDelete ⇒ OLLP dependent-read path. The implicit
    // edges of the matched edge documents must be cleaned up in the same txn.
    coordinator
        .client
        .simple_query(&format!("DELETE FROM {coll} WHERE mark = 'del'"))
        .await
        .expect("OLLP implicit-edge BulkDelete from a non-leader coordinator must complete");

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // The documents are gone.
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
        0,
        "all matched edge documents must be deleted"
    );

    // POST-DELETE: the reverse traversal from `hub` must no longer reach ANY
    // source — only `hub` itself (the start node) remains — on every node. If the
    // implicit edges were NOT cleaned cross-shard, a reverse copy would still
    // dangle on hub's home shard and a source would still be reachable.
    //
    // THIS is what makes the test GENUINE OLLP proof. The edges here are
    // cross-shard (`from_key(src_i) != from_key(hub)` for several `i`), and the
    // implicit `EdgeDelete` for each matched edge document is derived ONLY inside
    // the OLLP/Calvin dependent-delete bundle (`append_implicit_edge_delete_tasks`
    // off the pre-exec scan). A fast-path (non-OLLP) `BulkDelete` deletes the
    // documents but emits no edge-delete and cannot dual-home a tombstone onto
    // `hub`'s shard — so a cross-shard reverse edge would survive. Therefore
    // edges-gone (no source reverse-reachable from `hub`) ⟹ the OLLP edge-delete
    // bundle ran, which is exactly what the edge-bearing routing gate enables.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} reverse traversal from hub finds no sources after delete"),
            Duration::from_secs(20),
            Duration::from_millis(100),
            || {
                let ids = node
                    .traversed_node_ids("GRAPH TRAVERSE IN 'ollp_impl_edge_del' FROM 'hub' DEPTH 1 LABEL 'l' DIRECTION in");
                // Only the start node `hub` should remain reachable.
                ids.iter().all(|id| id == "hub")
            },
        )
        .await;
    }

    cluster.shutdown().await;
}
