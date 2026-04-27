//! Snapshot install test for array CRDT Raft replication.
//!
//! Verifies that a schema snapshot proposed via `handle_schema` is applied
//! on all nodes through the Raft log, and that subsequent data ops on that
//! schema are accepted and replicated correctly.
//!
//! This exercises the full path through `ReplicatedWrite::ArraySchema` in
//! `run_apply_loop` → `apply_array_schema` → `OriginSchemaRegistry::import_snapshot`.
//! After that, `ReplicatedWrite::ArrayOp` entries are accepted because every
//! node now has the schema.

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
};
use common::cluster_harness::{TestCluster, wait_for};

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

/// cluster/array_raft_snapshot_install
///
/// Proposes a schema snapshot on node 1 only (node 2 and 3 start without
/// the schema). After the schema proposal is committed, all nodes must have
/// the schema. Subsequently, data ops are replicated correctly to all nodes.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_array_raft_snapshot_install() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let shareds = shareds(&cluster);

    // Register the schema on node 1 only (nodes 2+3 start unaware).
    let (snap_bytes, schema_hlc) = build_schema_snapshot("snparray");
    import_schema_snapshot(shareds[0], "snparray", &snap_bytes, schema_hlc);
    register_catalog_entry(shareds[0], "snparray");

    // Also register catalog entries on all nodes so the Data Plane can
    // accept ops — the catalog is local and must be pre-populated.
    register_catalog_entry(shareds[1], "snparray");
    register_catalog_entry(shareds[2], "snparray");

    let inbound = make_inbound(shareds[0]);

    // Propose the schema snapshot through Raft.
    let schema_msg = ArraySchemaSyncMsg {
        array: "snparray".into(),
        replica_id: 1,
        schema_hlc_bytes: schema_hlc.to_bytes(),
        snapshot_payload: snap_bytes.clone(),
    };
    let outcome = inbound
        .handle_schema(&schema_msg)
        .await
        .expect("handle_schema");
    assert_eq!(outcome, InboundOutcome::SchemaImported);

    // All nodes must import the schema after Raft commit.
    wait_for(
        "all 3 nodes have schema for snparray",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            shareds.iter().all(|s| {
                s.array_sync_schemas
                    .schema_hlc("snparray")
                    .map(|h| h == schema_hlc)
                    .unwrap_or(false)
            })
        },
    )
    .await;

    // Now propose data ops — all nodes must accept them.
    for i in 0u64..3 {
        let msg = put_delta("snparray", i as i64, i as f64, schema_hlc, 5000 + i);
        let outcome = inbound.handle_delta(&msg).await.expect("handle_delta");
        assert_eq!(outcome, InboundOutcome::Applied, "op {i} must be Applied");
    }

    wait_for(
        "all 3 nodes have 3 ops for snparray",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || shareds.iter().all(|s| op_log_count(s) == 3),
    )
    .await;

    cluster.shutdown().await;
}
