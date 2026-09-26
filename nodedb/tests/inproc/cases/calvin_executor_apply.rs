// SPDX-License-Identifier: BUSL-1.1

//! Data Plane apply and rollback coverage for a committed Calvin transaction.
//!
//! These drive the participant's Data Plane steps directly: stage
//! (`CalvinExecuteStatic`), resolve (`CalvinResolve`) and flush the resolved
//! redo record at its LSN (`CalvinFlush`) — the steps the scheduler dispatches
//! once the global verdict is commit.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{ErrorCode, Priority, Request, Status};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb::wal::{RedoRecord, RedoSubRecord};
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::{KvOp, MetaOp, PhysicalPlan};
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
        provenance: None,
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

/// Stage `plans` as the Calvin transaction at `(epoch, 0)` and resolve it.
/// Returns the resolved redo record.
fn stage_and_resolve(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    epoch: u64,
    plans: Vec<PhysicalPlan>,
) -> RedoRecord {
    send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
            epoch,
            position: 0,
            tenant_id: TenantId::new(1),
            plans,
            epoch_system_ms: 0,
            is_group_leader: true,
            versioned_reads: Vec::new(),
        }),
    );
    let redo = send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Meta(MetaOp::CalvinResolve { epoch, position: 0 }),
    );
    RedoRecord::from_bytes(&redo).expect("decode resolved redo")
}

/// Flush `redo` as the Calvin transaction at `(epoch, 0)` with its record at
/// `lsn`.
fn flush(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    epoch: u64,
    redo: &RedoRecord,
    lsn: u64,
) -> nodedb::bridge::envelope::Response {
    let mut request = make_request(PhysicalPlan::Meta(MetaOp::CalvinFlush {
        epoch,
        position: 0,
        redo: redo.to_bytes().expect("encode redo"),
        collections: vec!["orders".into(), "rollback_coll".into()],
        sum_targets: Vec::new(),
    }));
    request.wal_lsn = Some(Lsn::new(lsn));
    tx.try_push(BridgeRequest::unfloored(request)).unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}

/// A timeseries batch whose line names another measurement than its
/// collection: it passes validation and fails while it installs.
fn failing_install_sub_record() -> RedoSubRecord {
    let lines = zerompk::to_msgpack_vec(&vec!["other_probe,host=a value=1 1".to_string()])
        .expect("encode lines");
    RedoSubRecord {
        record_type: nodedb_wal::record::RecordType::TimeseriesBatch as u32,
        payload: zerompk::to_msgpack_vec(&(
            "timeseries",
            "refusal_probe",
            lines.as_slice(),
            None::<&nodedb_types::sync::wire::SyncProvenance>,
            "ilp-msgpack",
        ))
        .expect("encode ingest"),
    }
}

// ── Test 1: successful Calvin flush ──────────────────────────────────────────

/// A committed Calvin transaction installs its redo record at the flush. The
/// written key is readable after it.
#[test]
fn calvin_static_apply_success() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let redo = stage_and_resolve(
        &mut core,
        &mut tx,
        &mut rx,
        1,
        vec![kv_put("orders", b"k1", b"v1")],
    );
    let resp = flush(&mut core, &mut tx, &mut rx, 1, &redo, 10);
    assert_eq!(
        resp.status,
        Status::Ok,
        "Calvin flush must succeed; got {:?}",
        resp.error_code
    );

    let payload = send_ok(&mut core, &mut tx, &mut rx, kv_get("orders", b"k1"));
    assert!(
        !payload.is_empty(),
        "row must exist after a successful Calvin flush"
    );
}

// ── Test 2: failing install → error without RollbackFailed ───────────────────

/// When a sub-record of the redo record fails while it installs, the flush
/// rolls every write back. The response is `Status::Error` and is NOT
/// `RollbackFailed` (the rollback itself succeeded), and the transaction's
/// write is gone.
#[test]
fn calvin_static_apply_failure_rolls_back_cleanly() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let mut redo = stage_and_resolve(
        &mut core,
        &mut tx,
        &mut rx,
        2,
        vec![kv_put("rollback_coll", b"should_be_gone", b"present")],
    );
    redo.ops.push(failing_install_sub_record());
    let resp = flush(&mut core, &mut tx, &mut rx, 2, &redo, 20);

    assert_eq!(
        resp.status,
        Status::Error,
        "a failing sub-record must fail the flush; got {:?}",
        resp.error_code
    );
    assert!(
        matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RetryableRefusal { .. })
        ),
        "the install rolled back every write; got {:?}",
        resp.error_code
    );

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
