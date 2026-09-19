// SPDX-License-Identifier: BUSL-1.1

//! `ROLLBACK TO SAVEPOINT` for the GRAPH staging overlay, driven directly
//! through the SPSC bridge.
//!
//! The SQL surface cannot create a multi-node graph edge inside an explicit
//! transaction today: an implicit-edge insert is only staged through the
//! per-task gate when it is a SELF-LOOP (so every task homes on one vShard),
//! and an in-txn edge DELETE routes through the OLLP/Calvin cleanup
//! coordinator rather than the staging overlay (see the documented limitation
//! in `sql_transactions_graph_overlay.rs`). So the savepoint mechanism for the
//! GRAPH overlay is exercised here at the bridge level instead: build
//! `MetaOp::StageWrite { plan: GraphOp::EdgePut / EdgeDelete }` tasks stamped
//! with a `txn_id`, `MetaOp::MarkSavepoint` to capture the composite marker,
//! stage more, then `MetaOp::RollbackToSavepoint` and read back through
//! `GraphOp::Neighbors` (which merges the overlay for the same `txn_id`).
//!
//! The pure journal mechanics (cross-set clearing, node-label deltas) are also
//! covered as unit tests on `GraphTxnOverlay` in `graph_staged.rs`; these tests
//! additionally verify the full composite-marker meta-op path through
//! `dispatch_meta` and that ONE savepoint reverts the value AND graph overlays
//! together (U7-1 is not regressed).

use nodedb::bridge::envelope::{Request, Status};
use nodedb::engine::graph::edge_store::Direction;
use nodedb::types::TxnId;
use nodedb_physical::physical_plan::{GraphOp, KvOp, MetaOp, PhysicalPlan};

use super::helpers::*;

fn send_txn(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    req_tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    resp_rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    txn_id: TxnId,
    plan: PhysicalPlan,
) -> nodedb::bridge::envelope::Response {
    let request = Request {
        txn_id: Some(txn_id),
        ..make_request(plan)
    };
    req_tx
        .try_push(nodedb::bridge::dispatch::BridgeRequest { inner: request })
        .unwrap();
    core.tick();
    resp_rx.try_pop().unwrap().inner
}

fn stage_edge_put(collection: &str, src: &str, label: &str, dst: &str) -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::StageWrite {
        plan: Box::new(PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            src_id: src.into(),
            label: label.into(),
            dst_id: dst.into(),
            properties: Vec::new(),
            src_surrogate: nodedb_types::Surrogate::ZERO,
            dst_surrogate: nodedb_types::Surrogate::ZERO,
        })),
    })
}

fn stage_edge_delete(collection: &str, src: &str, label: &str, dst: &str) -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::StageWrite {
        plan: Box::new(PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            src_id: src.into(),
            label: label.into(),
            dst_id: dst.into(),
            src_surrogate: nodedb_types::Surrogate::ZERO,
            dst_surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })),
    })
}

fn neighbors(node: &str, label: &str) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::Neighbors {
        node_id: node.into(),
        edge_label: Some(label.into()),
        direction: Direction::Out,
        rls_filters: Vec::new(),
        collection: None,
    })
}

/// Parse the 24-byte `MarkSavepoint` payload into `(value_marker,
/// graph_marker, array_marker)`.
fn parse_markers(payload: &[u8]) -> (u64, u64, u64) {
    assert_eq!(payload.len(), 24, "MarkSavepoint must return 24 bytes");
    let mut v = [0u8; 8];
    v.copy_from_slice(&payload[..8]);
    let mut g = [0u8; 8];
    g.copy_from_slice(&payload[8..16]);
    let mut a = [0u8; 8];
    a.copy_from_slice(&payload[16..24]);
    (
        u64::from_le_bytes(v),
        u64::from_le_bytes(g),
        u64::from_le_bytes(a),
    )
}

fn neighbor_nodes(payload: &[u8]) -> Vec<String> {
    let json = payload_json(payload);
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap_or_default();
    parsed
        .iter()
        .filter_map(|e| e.get("node").and_then(|n| n.as_str()).map(String::from))
        .collect()
}

#[test]
fn rollback_to_savepoint_discards_graph_edge_staged_after_marker() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let txn_id = TxnId::new(1);

    // Stage A→B before the savepoint.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        stage_edge_put("g", "a", "knows", "b"),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

    // Mark the savepoint.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Meta(MetaOp::MarkSavepoint { txn_id }),
    );
    assert_eq!(resp.status, Status::Ok);
    let (value_marker, graph_marker, array_marker) = parse_markers(resp.payload.as_ref());

    // Stage A→C after the savepoint.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        stage_edge_put("g", "a", "knows", "c"),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

    // In-tx both B and C are visible.
    let resp = send_txn(&mut core, &mut tx, &mut rx, txn_id, neighbors("a", "knows"));
    let before = neighbor_nodes(resp.payload.as_ref());
    assert!(
        before.contains(&"b".to_string()) && before.contains(&"c".to_string()),
        "{before:?}"
    );

    // Roll back to the savepoint.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Meta(MetaOp::RollbackToSavepoint {
            txn_id,
            value_marker,
            graph_marker,
            array_marker,
        }),
    );
    assert_eq!(resp.status, Status::Ok);

    // A→B (pre-marker) survives; A→C (post-marker) is gone.
    let resp = send_txn(&mut core, &mut tx, &mut rx, txn_id, neighbors("a", "knows"));
    let after = neighbor_nodes(resp.payload.as_ref());
    assert!(
        after.contains(&"b".to_string()),
        "A→B must survive rollback, got {after:?}"
    );
    assert!(
        !after.contains(&"c".to_string()),
        "A→C must be discarded by rollback, got {after:?}"
    );
}

#[test]
fn rollback_to_savepoint_restores_cross_set_cleared_tombstone() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let txn_id = TxnId::new(2);

    // Durable committed edge X→Y (no txn).
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "g",
            ),
            src_id: "x".into(),
            label: "knows".into(),
            dst_id: "y".into(),
            properties: Vec::new(),
            src_surrogate: nodedb_types::Surrogate::ZERO,
            dst_surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    // Stage a tombstone of X→Y: in-tx neighbors of X must now be empty.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        stage_edge_delete("g", "x", "knows", "y"),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    let resp = send_txn(&mut core, &mut tx, &mut rx, txn_id, neighbors("x", "knows"));
    assert!(
        neighbor_nodes(resp.payload.as_ref()).is_empty(),
        "tombstone must hide durable Y"
    );

    // Mark savepoint AFTER the tombstone.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Meta(MetaOp::MarkSavepoint { txn_id }),
    );
    let (value_marker, graph_marker, array_marker) = parse_markers(resp.payload.as_ref());

    // Re-put X→Y: this CLEARS the tombstone (cross-set), so Y is visible again.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        stage_edge_put("g", "x", "knows", "y"),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    let resp = send_txn(&mut core, &mut tx, &mut rx, txn_id, neighbors("x", "knows"));
    assert!(
        neighbor_nodes(resp.payload.as_ref()).contains(&"y".to_string()),
        "re-put must restore Y"
    );

    // Roll back: the re-put is undone AND the tombstone it cleared is restored,
    // so Y is hidden once more.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Meta(MetaOp::RollbackToSavepoint {
            txn_id,
            value_marker,
            graph_marker,
            array_marker,
        }),
    );
    assert_eq!(resp.status, Status::Ok);
    let resp = send_txn(&mut core, &mut tx, &mut rx, txn_id, neighbors("x", "knows"));
    assert!(
        neighbor_nodes(resp.payload.as_ref()).is_empty(),
        "rollback must restore the tombstone the re-put cleared, hiding durable Y again"
    );
}

#[test]
fn one_savepoint_reverts_value_and_graph_overlays_together() {
    // U7-1 regression guard: a single ROLLBACK TO must rewind BOTH the value/
    // TTL overlay and the GRAPH overlay via the composite marker.
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let txn_id = TxnId::new(3);

    // Base KV row with no TTL.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Stage one graph edge and one KV TTL delta BEFORE the savepoint.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        stage_edge_put("g", "a", "knows", "b"),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Meta(MetaOp::StageWrite {
            plan: Box::new(PhysicalPlan::Kv(KvOp::Expire {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "c",
                ),
                key: b"k".to_vec(),
                ttl_ms: 60_000,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            })),
        }),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

    // Composite savepoint marker.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Meta(MetaOp::MarkSavepoint { txn_id }),
    );
    let (value_marker, graph_marker, array_marker) = parse_markers(resp.payload.as_ref());
    assert_eq!(
        (value_marker, graph_marker, array_marker),
        (1, 1, 0),
        "one value + one graph mutation staged, no array mutation"
    );

    // Stage more of BOTH after the savepoint: another edge and a PERSIST that
    // overwrites the staged EXPIRE.
    send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        stage_edge_put("g", "a", "knows", "c"),
    );
    send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Meta(MetaOp::StageWrite {
            plan: Box::new(PhysicalPlan::Kv(KvOp::Persist {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "c",
                ),
                key: b"k".to_vec(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            })),
        }),
    );

    // One rollback reverts both overlays to the marker.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Meta(MetaOp::RollbackToSavepoint {
            txn_id,
            value_marker,
            graph_marker,
            array_marker,
        }),
    );
    assert_eq!(resp.status, Status::Ok);

    // Graph: A→B survives, A→C discarded.
    let resp = send_txn(&mut core, &mut tx, &mut rx, txn_id, neighbors("a", "knows"));
    let after = neighbor_nodes(resp.payload.as_ref());
    assert!(
        after.contains(&"b".to_string()) && !after.contains(&"c".to_string()),
        "{after:?}"
    );

    // Value/TTL: the post-marker PERSIST is undone, so the staged EXPIRE (~60s)
    // is what an in-tx GetTtl observes — NOT the persisted -1.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
        }),
    );
    let ttl_ms = payload_value(resp.payload.as_ref())["ttl_ms"]
        .as_i64()
        .unwrap();
    assert!(
        (0..=60_000).contains(&ttl_ms),
        "value overlay must revert the post-marker PERSIST, leaving the staged EXPIRE; got {ttl_ms}"
    );
}
