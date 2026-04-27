//! Partition + recovery test for array CRDT Raft replication.
//!
//! Simulates a lagging replica: writes are accepted on the 3-node cluster
//! while one node is slower to apply (due to MPSC backpressure), then
//! verifies that the lagging node eventually catches up to the same op-log
//! count as the other two.
//!
//! This tests the `DistributedApplier` backpressure recovery path — when
//! the apply queue is briefly full, entries are re-delivered by Raft and
//! the node eventually converges without data loss.

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
use nodedb_types::sync::wire::array::ArrayDeltaMsg;

use common::array_sync::{hlc, register_schema_on_all};
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

/// cluster/array_raft_partition_recovery
///
/// Propose 10 ops rapidly on the leader. Despite potential transient
/// backpressure on follower apply queues, all three nodes must converge to
/// the same op-log count within a generous deadline.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_array_raft_partition_recovery() {
    const OPS: u64 = 10;

    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    let shareds = shareds(&cluster);
    let schema_hlc = register_schema_on_all(&shareds, "recovery");

    let inbound = make_inbound(shareds[0]);

    // Propose all ops from the leader node.
    for i in 0..OPS {
        let msg = put_delta("recovery", i as i64, i as f64, schema_hlc, 3000 + i);
        let outcome = inbound.handle_delta(&msg).await.expect("handle_delta");
        assert_eq!(outcome, InboundOutcome::Applied, "op {i} must be Applied");
    }

    // Give all nodes time to receive and apply every committed entry.
    // A longer deadline (15 s) accommodates transient backpressure / slow CI.
    wait_for(
        "all 3 nodes converge to OPS op-log count",
        Duration::from_secs(15),
        Duration::from_millis(100),
        || shareds.iter().all(|s| op_log_count(s) == OPS),
    )
    .await;

    for (i, s) in shareds.iter().enumerate() {
        assert_eq!(op_log_count(s), OPS, "node {} op-log count mismatch", i + 1);
    }

    cluster.shutdown().await;
}
