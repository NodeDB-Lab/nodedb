//! End-to-end cluster tests for array CRDT Raft replication.
//!
//! Covers: basic replication, idempotency, and schema sync across a 3-node
//! cluster. Every test function name includes `/cluster/` so nextest routes
//! them to the `cluster` test group (max-threads = 1, serialised).
//!
//! ## Write flow exercised
//!
//! 1. `OriginArrayInbound::handle_delta` detects `raft_proposer` is set.
//! 2. Proposes `ReplicatedWrite::ArrayOp` via `RaftLoop::propose`.
//! 3. `DistributedApplier` receives the committed entry from the mpsc.
//! 4. `run_apply_loop` dispatches to the local Data Plane and calls
//!    `ProposeTracker::complete` so the proposing session unblocks.
//! 5. `engine.already_seen()` returns true on re-proposal → `Idempotent`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use nodedb::control::array_sync::{InboundOutcome, OriginApplyEngine, OriginArrayInbound};
use nodedb::control::state::SharedState;
use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
use nodedb_array::sync::op_codec;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::TenantId;
use nodedb_types::sync::wire::array::{ArrayDeltaMsg, ArraySchemaSyncMsg};

use common::array_sync::{
    build_schema_snapshot, hlc, import_schema_snapshot, register_catalog_entry,
    register_schema_on_all,
};
use common::cluster_harness::{TestCluster, wait_for};

// ── helpers ───────────────────────────────────────────────────────────────────

fn shareds(cluster: &TestCluster) -> Vec<&Arc<SharedState>> {
    cluster.nodes.iter().map(|n| &n.shared).collect()
}

fn put_delta(
    array: &str,
    coord_x: i64,
    v: f64,
    schema_hlc: nodedb_array::sync::hlc::Hlc,
    ms: u64,
) -> ArrayDeltaMsg {
    let op = ArrayOp {
        header: ArrayOpHeader {
            array: array.to_string(),
            hlc: hlc(ms, 1),
            schema_hlc,
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: ms as i64,
        },
        kind: ArrayOpKind::Put,
        coord: vec![CoordValue::Int64(coord_x)],
        attrs: Some(vec![CellValue::Float64(v)]),
    };
    let payload = op_codec::encode_op(&op).expect("encode op");
    ArrayDeltaMsg {
        array: array.to_string(),
        op_payload: payload,
    }
}

fn make_inbound(shared: &Arc<SharedState>) -> OriginArrayInbound {
    let engine = Arc::new(OriginApplyEngine::new(
        Arc::clone(&shared.array_sync_schemas),
        Arc::clone(&shared.array_sync_op_log),
    ));
    OriginArrayInbound::new(
        engine,
        Arc::clone(&shared.array_sync_schemas),
        Arc::clone(shared),
        TenantId::new(0),
    )
}

fn op_log_count(shared: &Arc<SharedState>) -> u64 {
    use nodedb_array::sync::op_log::OpLog;
    shared.array_sync_op_log.len().unwrap_or(0)
}

// ── test: basic 3-node replication ───────────────────────────────────────────

/// cluster/array_raft_basic_replication
#[tokio::test(flavor = "multi_thread")]
async fn cluster_array_raft_basic_replication() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let shareds = shareds(&cluster);
    let schema_hlc = register_schema_on_all(&shareds, "prices");

    let inbound = make_inbound(shareds[0]);
    let msg = put_delta("prices", 5, 42.0, schema_hlc, 1000);

    let outcome = inbound.handle_delta(&msg).await.expect("handle_delta");
    assert_eq!(outcome, InboundOutcome::Applied, "leader node should apply");

    wait_for(
        "op-log count == 1 on all 3 nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || shareds.iter().all(|s| op_log_count(s) == 1),
    )
    .await;

    cluster.shutdown().await;
}

// ── test: multiple ops replicate in order ────────────────────────────────────

/// cluster/array_raft_multi_op_replication
#[tokio::test(flavor = "multi_thread")]
async fn cluster_array_raft_multi_op_replication() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let shareds = shareds(&cluster);
    let schema_hlc = register_schema_on_all(&shareds, "multi");

    let inbound = make_inbound(shareds[0]);

    for i in 0u64..5 {
        let msg = put_delta("multi", i as i64, i as f64, schema_hlc, 1000 + i);
        let outcome = inbound.handle_delta(&msg).await.expect("handle_delta");
        assert_eq!(outcome, InboundOutcome::Applied);
    }

    wait_for(
        "all 3 nodes have 5 ops",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || shareds.iter().all(|s| op_log_count(s) == 5),
    )
    .await;

    cluster.shutdown().await;
}

// ── test: idempotency ─────────────────────────────────────────────────────────

/// cluster/array_raft_idempotency
#[tokio::test(flavor = "multi_thread")]
async fn cluster_array_raft_idempotency() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let shareds = shareds(&cluster);
    let schema_hlc = register_schema_on_all(&shareds, "idempot");

    let inbound = make_inbound(shareds[0]);
    let msg = put_delta("idempot", 10, 1.0, schema_hlc, 2000);

    let first = inbound
        .handle_delta(&msg)
        .await
        .expect("first handle_delta");
    assert_eq!(first, InboundOutcome::Applied);

    wait_for(
        "all nodes see 1 op",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || shareds.iter().all(|s| op_log_count(s) == 1),
    )
    .await;

    let second = inbound
        .handle_delta(&msg)
        .await
        .expect("second handle_delta");
    assert_eq!(
        second,
        InboundOutcome::Idempotent,
        "re-proposal must be idempotent"
    );

    for (i, s) in shareds.iter().enumerate() {
        assert_eq!(
            op_log_count(s),
            1,
            "node {} must not have a duplicate",
            i + 1
        );
    }

    cluster.shutdown().await;
}

// ── test: schema sync ─────────────────────────────────────────────────────────

/// cluster/array_raft_schema_sync
#[tokio::test(flavor = "multi_thread")]
async fn cluster_array_raft_schema_sync() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let shareds = shareds(&cluster);

    // Build and register the schema only on node 1.
    let (snap_bytes, schema_hlc) = build_schema_snapshot("schemarray");
    import_schema_snapshot(shareds[0], "schemarray", &snap_bytes, schema_hlc);
    register_catalog_entry(shareds[0], "schemarray");

    let inbound = make_inbound(shareds[0]);
    let schema_msg = ArraySchemaSyncMsg {
        array: "schemarray".into(),
        replica_id: 1,
        schema_hlc_bytes: schema_hlc.to_bytes(),
        snapshot_payload: snap_bytes.clone(),
    };

    let outcome = inbound
        .handle_schema(&schema_msg)
        .await
        .expect("handle_schema");
    assert_eq!(outcome, InboundOutcome::SchemaImported);

    // All nodes must have the schema after Raft commit + apply.
    wait_for(
        "all nodes have schema_hlc for schemarray",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            shareds.iter().all(|s| {
                s.array_sync_schemas
                    .schema_hlc("schemarray")
                    .map(|h| h == schema_hlc)
                    .unwrap_or(false)
            })
        },
    )
    .await;

    cluster.shutdown().await;
}
