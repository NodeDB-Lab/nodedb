// SPDX-License-Identifier: BUSL-1.1

//! Data Plane apply and rollback coverage for `MetaOp::TransactionBatch`.
//!
//! These tests exercise the cross-shard apply path using
//! `MetaOp::TransactionBatch` (CalvinExecuteStatic) directly — no
//! `ReplicatedWrite::CrossShardForward`, no `decode_forwarded_plans`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{ErrorCode, Priority, Request, Status};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::{KvOp, MetaOp, PhysicalPlan, VectorOp};
use nodedb_types::{OrdinalClock, QualifiedCollection};

// ── Helpers ─────────────────────────────────────────────────────────────────

fn make_core() -> (
    CoreLoop,
    Producer<BridgeRequest>,
    Consumer<BridgeResponse>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir.path(),
        Arc::new(OrdinalClock::new()),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

fn make_request(plan: PhysicalPlan) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        database_id: nodedb::types::DatabaseId::DEFAULT,
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: nodedb_types::TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: nodedb::event::EventSource::RaftFollower,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: nodedb::bridge::envelope::Admission::Admitted,
    }
}

fn send_raw(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> nodedb::bridge::envelope::Response {
    tx.try_push(BridgeRequest::unfloored(make_request(plan)))
        .unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}

fn send_ok(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> Vec<u8> {
    let resp = send_raw(core, tx, rx, plan);
    assert_eq!(
        resp.status,
        Status::Ok,
        "expected Ok, got {:?}",
        resp.error_code
    );
    resp.payload.to_vec()
}

fn kv_put(coll: &str, key: &[u8], value: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, coll),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
        returning: None,
        rls_filters: Vec::new(),
    })
}

fn kv_get(coll: &str, key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Get {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, coll),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    })
}

// ── Test 1: successful Calvin static-set apply ───────────────────────────────

/// A `MetaOp::TransactionBatch` dispatched directly (CalvinExecuteStatic) must
/// apply atomically. After a successful apply the written key must be readable.
#[test]
fn calvin_static_apply_success() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let batch_plan = PhysicalPlan::Meta(MetaOp::TransactionBatch {
        txn_id: None,
        plans: vec![kv_put("orders", b"k1", b"v1")],
    });

    let resp = send_raw(&mut core, &mut tx, &mut rx, batch_plan);
    assert_eq!(
        resp.status,
        Status::Ok,
        "Calvin apply must succeed; got {:?}",
        resp.error_code
    );

    let payload = send_ok(&mut core, &mut tx, &mut rx, kv_get("orders", b"k1"));
    assert!(
        !payload.is_empty(),
        "row must exist after successful Calvin apply"
    );
}

// ── Test 2: failing sub-plan → error without RollbackFailed ──────────────────

/// When a sub-plan in the batch fails, the batch must roll back cleanly.
/// The response must be `Status::Error` and must NOT be `RollbackFailed`
/// (the rollback itself must succeed).
///
/// We trigger the failure via a `VectorOp::Insert` with a dimension mismatch:
/// the vector index is configured for dim=3 but we insert a dim=2 vector.
#[test]
fn calvin_static_apply_failure_rolls_back_cleanly() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Configure and seed the vector collection (dim=3).
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::SetParams {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vec_coll"),
            field_name: String::new(),
            dim: 3,
            m: 16,
            ef_construction: 200,
            metric: "cosine".into(),
            index_type: String::new(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
        }),
    );
    // Seed one valid vector so the index knows dim=3.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vec_coll"),
            vector: vec![1.0, 2.0, 3.0],
            dim: 3,
            field_name: String::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: None,
            provenance: None,
        }),
    );

    let plans = vec![
        // First sub-plan succeeds.
        kv_put("rollback_coll", b"should_be_gone", b"present"),
        // Second sub-plan fails: dim mismatch (index expects 3, we provide 2).
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vec_coll"),
            vector: vec![1.0, 2.0],
            dim: 3,
            field_name: String::new(),
            surrogate: nodedb_types::Surrogate::new(99),
            pk_bytes: None,
            provenance: None,
        }),
    ];

    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            txn_id: None,
            plans,
        }),
    );

    assert_eq!(
        resp.status,
        Status::Error,
        "a failing sub-plan must cause the batch to fail; got {:?}",
        resp.error_code
    );
    // Rollback must succeed — the engine must not be in an unknown state.
    assert!(
        !matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RollbackFailed { .. })
        ),
        "rollback must succeed on failure path; got {:?}",
        resp.error_code
    );

    // The first sub-plan's write ("should_be_gone") must have been rolled back.
    let get_resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("rollback_coll", b"should_be_gone"),
    );
    assert!(
        get_resp.payload.is_empty() || get_resp.status == Status::Error,
        "rolled-back write must not persist; got {:?}",
        get_resp
    );
}
