// SPDX-License-Identifier: BUSL-1.1

//! A Calvin slice that holds a `RETURNING` contract answers its frozen reply.
//!
//! The participant's Data Plane stages the plan and resolves it. A slice
//! whose stage rendered its rows installs with `CalvinReplySpec::Frozen`:
//! the install renders nothing, and every replica answers the frozen reply
//! the slice's data-group log stored before the vote. These drive the Data
//! Plane steps directly and the store the apply loop writes. The scheduler's
//! proposal of the frozen entry and its vote are covered by the scheduler's
//! own tests.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{Priority, Request, Status};
use nodedb::control::cluster::calvin::scheduler::frozen_replies;
use nodedb::control::cluster::calvin::scheduler::lock_manager::TxnId as SchedulerTxnId;
use nodedb::control::security::catalog::SystemCatalog;
use nodedb::control::wal_replication::types::{
    CalvinFrozenReply, FrozenPlanReply, FrozenSequenceEffect,
};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb::wal::RedoRecord;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::{
    CalvinInstall, CalvinReplySpec, CalvinResolved, DocumentOp, KvOp, MetaOp, PhysicalPlan,
    RedoOrigin, ReturningColumns, ReturningSpec,
};
use nodedb_types::{OrdinalClock, QualifiedCollection, RlsWriteCheck, Surrogate, Value};

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
        commit_hlc: None,
        system_ordinal: None,
        admission: nodedb::bridge::envelope::Admission::Admitted,
    }
}

fn send(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    request: Request,
) -> nodedb::bridge::envelope::Response {
    tx.try_push(BridgeRequest::unfloored(request)).unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}

fn send_ok(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> Vec<u8> {
    let resp = send(core, tx, rx, make_request(plan));
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    resp.payload.to_vec()
}

fn star() -> Option<ReturningSpec> {
    Some(ReturningSpec {
        columns: ReturningColumns::Star,
        precommit: None,
    })
}

fn kv_put(key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
        key: key.to_vec(),
        value: b"v1".to_vec(),
        ttl_ms: 0,
        surrogate: nodedb_test_support::kv_rows::kv_row_surrogate(key),
        returning: star(),
        rls_filters: Vec::new(),
        provenance: None,
    })
}

fn kv_get(key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Get {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
        system_time: nodedb_types::SystemTimeScope::Current,
    })
}

fn doc_insert(id: &str, surrogate: u32) -> PhysicalPlan {
    let fields = std::collections::HashMap::from([("amount".to_string(), Value::Integer(5))]);
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "ledger"),
        document_id: id.to_string(),
        value: nodedb_types::value_to_msgpack(&Value::Object(fields)).expect("encode body"),
        if_absent: false,
        surrogate: Surrogate::new(surrogate),
        returning: star(),
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    })
}

/// Stage `plans` as the Calvin transaction at `(epoch, 0)` and resolve it.
fn stage_and_resolve(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    epoch: u64,
    plans: Vec<PhysicalPlan>,
) -> (RedoRecord, CalvinReplySpec) {
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
            versioned_reads: Vec::new(),
            body_plans: Vec::new(),
            snapshot_ordinal: None,
        }),
    );
    let answer = send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Meta(MetaOp::CalvinResolve { epoch, position: 0 }),
    );
    let answer: CalvinResolved = zerompk::from_msgpack(&answer).expect("decode resolved answer");
    let record = RedoRecord::from_bytes(&answer.redo).expect("decode resolved redo");
    (record, answer.reply)
}

/// Install `redo` as the stamped slice at `(epoch, 0)`, answering `reply`.
fn install(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    epoch: u64,
    redo: &RedoRecord,
    reply: CalvinReplySpec,
    collection: &str,
) -> nodedb::bridge::envelope::Response {
    let mut request = make_request(PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: redo.to_bytes().expect("encode redo"),
        collections: vec![collection.into()],
        sum_targets: Vec::new(),
        origin: RedoOrigin::Commit,
        calvin: Some(CalvinInstall {
            epoch,
            position: 0,
            epoch_system_ms: 0,
            reply,
            user_write: true,
            mirror: false,
        }),
        reads: Vec::new(),
        snapshot_ordinal: None,
    }));
    request.wal_lsn = Some(Lsn::new(10 + epoch));
    send(core, tx, rx, request)
}

/// A KV `RETURNING` slice renders its rows at the stage. Its install with
/// the frozen reply writes the row and renders nothing: the Control Plane
/// answers the stored reply.
#[test]
fn a_frozen_install_writes_the_row_and_renders_nothing() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let (redo, staged) = stage_and_resolve(&mut core, &mut tx, &mut rx, 1, vec![kv_put(b"k1")]);
    assert!(
        matches!(staged, CalvinReplySpec::Rows(_)),
        "the stage renders the rows: {staged:?}"
    );

    let resp = install(
        &mut core,
        &mut tx,
        &mut rx,
        1,
        &redo,
        CalvinReplySpec::Frozen,
        "orders",
    );

    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    assert!(resp.error_code.is_none());
    assert!(resp.payload.is_empty(), "a frozen install renders nothing");
    assert!(
        !send_ok(&mut core, &mut tx, &mut rx, kv_get(b"k1")).is_empty(),
        "the row is installed"
    );
}

/// A document row with no hash-chain link is stored exactly as its image,
/// so its stage renders the rows the install stores.
#[test]
fn a_document_returning_slice_renders_its_rows_at_the_stage() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let (_redo, staged) =
        stage_and_resolve(&mut core, &mut tx, &mut rx, 2, vec![doc_insert("l1", 7)]);
    assert!(
        matches!(staged, CalvinReplySpec::Rows(_)),
        "the stage renders the rows: {staged:?}"
    );
}

/// The store keeps the first copy of a txn's reply, and the answer the
/// install's replica gives carries it whole.
#[test]
fn the_store_keeps_the_first_frozen_reply() {
    let catalog = SystemCatalog::open_in_memory().expect("catalog");
    let reply = |value: i64| CalvinFrozenReply {
        epoch: 4,
        position: 1,
        vshard: 3,
        plans: vec![FrozenPlanReply {
            plan_index: 0,
            payload: vec![0x90],
            column_types: Vec::new(),
            sequence_effects: vec![FrozenSequenceEffect::Nextval {
                name: "s".into(),
                value,
            }],
            rejected: None,
            deferred_currval: Vec::new(),
        }],
    };
    assert!(frozen_replies::store(&catalog, &reply(1)).expect("store"));
    assert!(!frozen_replies::store(&catalog, &reply(2)).expect("store"));
    let stored = frozen_replies::load(&catalog, 3, SchedulerTxnId::new(4, 1))
        .expect("load")
        .expect("a stored reply");
    assert_eq!(stored, reply(1));
    let bytes = stored.to_bytes().expect("encode");
    assert_eq!(
        CalvinFrozenReply::from_bytes(&bytes).expect("decode"),
        stored
    );
}
