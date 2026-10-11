// SPDX-License-Identifier: BUSL-1.1

//! Helpers every staged Calvin apply case drives a `CoreLoop` with.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{Priority, Request, Response, Status};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::{
    CalvinInstall, CalvinResolved, DocumentOp, KvOp, MetaOp, PhysicalPlan, RedoOrigin,
};
use nodedb_types::QualifiedCollection;
use nodedb_types::Surrogate;
use nodedb_types::calvin::VersionedReadEntry;

/// The version a single-node write at WAL LSN `lsn` records: no data-group
/// entry applied it, so its version is its LSN on top of an empty vShard.
pub(super) fn local_version(lsn: u64) -> nodedb_types::WriteVersion {
    nodedb_types::WriteVersion::local_after(nodedb_types::WriteVersion::ZERO, lsn)
}

/// The vShard a Calvin participant writes `collection` on: the collection's
/// home vShard.
pub(super) fn home_vshard(collection: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection)
        .vshard()
        .as_u32()
}

pub(super) fn make_core() -> (
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
        Arc::new(nodedb_types::OrdinalClock::new()),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

/// Build a request for `plan` on `vshard`, carrying an optional committed WAL
/// LSN (present on the seed write so its version is recorded).
pub(super) fn make_request(plan: PhysicalPlan, vshard: u32, wal_lsn: Option<Lsn>) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(vshard),
        database_id: DatabaseId::DEFAULT,
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
        wal_lsn,
        resolved_now_ms: None,
        commit_hlc: None,
        entry_version: None,
        admission: nodedb::bridge::envelope::Admission::Admitted,
    }
}

pub(super) fn send(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
    vshard: u32,
    wal_lsn: Option<Lsn>,
) -> Response {
    tx.try_push(BridgeRequest::unfloored(make_request(
        plan, vshard, wal_lsn,
    )))
    .unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}

/// Resolve the transaction staged at `(epoch, 0)` on `vshard` and build the
/// stamped install of its redo record.
pub(super) fn resolved_install(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    epoch: u64,
    vshard: u32,
) -> PhysicalPlan {
    let resolved = send(
        core,
        tx,
        rx,
        PhysicalPlan::Meta(MetaOp::CalvinResolve { epoch, position: 0 }),
        vshard,
        None,
    );
    assert_eq!(
        resolved.status,
        Status::Ok,
        "resolve must succeed: {resolved:?}"
    );
    stamped_install(epoch, &resolved, Vec::new())
}

/// The stamped install of the slice at `(epoch, 0)` whose `CalvinResolve`
/// answered `resolved`.
pub(super) fn stamped_install(
    epoch: u64,
    resolved: &Response,
    collections: Vec<String>,
) -> PhysicalPlan {
    let answer: CalvinResolved =
        zerompk::from_msgpack(resolved.payload.as_bytes()).expect("decode resolved answer");
    PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: answer.redo,
        collections,
        sum_targets: Vec::new(),
        origin: RedoOrigin::Commit,
        calvin: Some(CalvinInstall {
            epoch,
            position: 0,
            epoch_system_ms: 0,
            reply: answer.reply,
            user_write: true,
        }),
    })
}

/// A write committed through the Calvin path to seed a write version.
pub(super) struct CalvinSeed<'a> {
    pub(super) epoch: u64,
    pub(super) vshard: u32,
    /// The collection `plans` write; the install records its floor at `lsn`.
    pub(super) collection: &'a str,
    pub(super) plans: Vec<PhysicalPlan>,
    pub(super) lsn: u64,
}

/// Commit `seed.plans` as the Calvin transaction at `(seed.epoch, 0)` on
/// `seed.vshard` and install its redo record at `seed.lsn`: the path a
/// committed multi-shard write takes. The install records each written key
/// and the collection floor at that LSN.
pub(super) fn commit_calvin(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    seed: CalvinSeed<'_>,
) -> Response {
    let staged = send(
        core,
        tx,
        rx,
        stage_static(seed.epoch, 0, seed.plans, Vec::new()),
        seed.vshard,
        None,
    );
    assert_eq!(
        staged.status,
        Status::Ok,
        "seed stage must succeed: {staged:?}"
    );
    let resolved = send(
        core,
        tx,
        rx,
        PhysicalPlan::Meta(MetaOp::CalvinResolve {
            epoch: seed.epoch,
            position: 0,
        }),
        seed.vshard,
        None,
    );
    assert_eq!(
        resolved.status,
        Status::Ok,
        "seed resolve must succeed: {resolved:?}"
    );
    send(
        core,
        tx,
        rx,
        stamped_install(seed.epoch, &resolved, vec![seed.collection.to_string()]),
        seed.vshard,
        Some(Lsn::new(seed.lsn)),
    )
}

pub(super) fn kv_put(coll: &str, key: &[u8], value: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, coll),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms: 0,
        surrogate: nodedb_test_support::kv_rows::kv_row_surrogate(key),
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    })
}

pub(super) fn kv_get(coll: &str, key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Get {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, coll),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    })
}

/// A minimal msgpack-encoded document body, `{"a": "1"}`.
pub(super) fn doc_value() -> Vec<u8> {
    let mut obj = std::collections::HashMap::new();
    obj.insert("a".to_string(), nodedb_types::Value::String("1".into()));
    zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap()
}

/// A minimal document `PointInsert` plan, used to record a write-version at a
/// specific surrogate (the document engine's per-key identity).
pub(super) fn doc_insert(coll: &str, document_id: &str, surrogate: u32) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, coll),
        document_id: document_id.to_string(),
        value: doc_value(),
        if_absent: false,
        surrogate: Surrogate::new(surrogate),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    })
}

pub(super) fn stage_static(
    epoch: u64,
    position: u32,
    plans: Vec<PhysicalPlan>,
    versioned_reads: Vec<VersionedReadEntry>,
) -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
        epoch,
        position,
        tenant_id: TenantId::new(1),
        plans,
        epoch_system_ms: 0,
        versioned_reads,
        body_plans: Vec::new(),
    })
}

/// Push one prebuilt request through the ring and return its response.
pub(super) fn send_request(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    request: Request,
) -> Response {
    tx.try_push(BridgeRequest::unfloored(request)).unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}
