// SPDX-License-Identifier: BUSL-1.1

//! Tests for static-set multi-shard Calvin execution path.
//!
//! Verifies the types and plan structures used by `MetaOp::CalvinExecuteStatic`.
//! End-to-end 3-replica coverage lives in
//! `nodedb-cluster/tests/calvin_3node_normal.rs`.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;
use nodedb_physical::physical_plan::wire as plan_wire;
use nodedb_types::TenantId;

#[test]
fn calvin_execute_static_round_trip_msgpack() {
    // Build a CalvinExecuteStatic variant with a simple empty plans vec and
    // verify it serializes/deserializes correctly.
    let op = MetaOp::CalvinExecuteStatic {
        epoch: 42,
        position: 7,
        tenant_id: TenantId::new(1),
        plans: vec![],
        epoch_system_ms: 0,
        versioned_reads: vec![],
        body_plans: vec![],
    };

    let plan = PhysicalPlan::Meta(op.clone());

    // Encode + decode via the wire codec.
    let batch_bytes = plan_wire::encode_batch(&vec![plan]).expect("encode");
    let decoded_batch = plan_wire::decode_batch(&batch_bytes).expect("decode");

    assert_eq!(decoded_batch.len(), 1);
    match &decoded_batch[0] {
        PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
            epoch,
            position,
            tenant_id,
            plans,
            ..
        }) => {
            assert_eq!(*epoch, 42);
            assert_eq!(*position, 7);
            assert_eq!(tenant_id.as_u64(), 1);
            assert!(plans.is_empty());
        }
        other => panic!("unexpected plan variant: {other:?}"),
    }
}

#[test]
fn calvin_execute_static_and_active_are_distinct_variants() {
    // Confirm the three Calvin variants are distinguishable.
    let static_op = MetaOp::CalvinExecuteStatic {
        epoch: 1,
        position: 0,
        tenant_id: TenantId::new(1),
        plans: vec![],
        epoch_system_ms: 0,
        versioned_reads: vec![],
        body_plans: vec![],
    };
    let passive_op = MetaOp::CalvinExecutePassive {
        epoch: 1,
        position: 0,
        tenant_id: TenantId::new(1),
        keys_to_read: vec![],
    };

    // Verify matching works correctly.
    let is_static = matches!(static_op, MetaOp::CalvinExecuteStatic { .. });
    let is_passive = matches!(passive_op, MetaOp::CalvinExecutePassive { .. });
    let static_not_passive = !matches!(static_op, MetaOp::CalvinExecutePassive { .. });

    assert!(is_static);
    assert!(is_passive);
    assert!(static_not_passive);
}
