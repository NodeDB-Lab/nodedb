// SPDX-License-Identifier: BUSL-1.1

//! `CoreLoop` fixtures shared by the write-version-index tests and by other
//! executor test modules (`make_core_with_dir` / `make_default_task` are
//! `pub` for exactly that reason).

use std::time::{Duration, Instant};

use nodedb_bridge::buffer::RingBuffer;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate, TenantId, WriteVersion};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::put::PointPutExec;
use crate::data::executor::task::ExecutionTask;
use crate::types::{Lsn, ReadConsistency, RequestId, TraceId, VShardId};

pub(crate) fn make_core() -> (
    CoreLoop,
    nodedb_bridge::buffer::Producer<BridgeRequest>,
    nodedb_bridge::buffer::Consumer<BridgeResponse>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let (core, req_tx, resp_rx) = make_core_with_dir(dir.path());
    (core, req_tx, resp_rx, dir)
}

pub fn make_core_with_dir(
    dir: &std::path::Path,
) -> (
    CoreLoop,
    nodedb_bridge::buffer::Producer<BridgeRequest>,
    nodedb_bridge::buffer::Consumer<BridgeResponse>,
) {
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir,
        std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        crate::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    (core, req_tx, resp_rx)
}

fn point_get() -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointGet {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
        document_id: "y".into(),
        surrogate: None,
        pk_bytes: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    })
}

pub(crate) fn make_request(plan: PhysicalPlan) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        commit_hlc: None,
        entry_version: None,
        admission: crate::bridge::envelope::Admission::Admitted,
    }
}

/// A minimal `ExecutionTask` (DEFAULT database/tenant, vShard 0, no WAL LSN)
/// for handler unit tests that only read `request.database_id`. The carried
/// plan is inert: edge/point handlers take their parameters directly.
pub fn make_default_task() -> ExecutionTask {
    ExecutionTask::new(make_request(point_get()))
}

/// A `{k: v}` document body: a standard MessagePack map.
pub(crate) fn doc_value(k: &str, v: &str) -> Vec<u8> {
    let mut obj = std::collections::HashMap::new();
    obj.insert(k.to_string(), nodedb_types::Value::String(v.into()));
    nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(obj)).unwrap()
}

/// An `ExecutionTask` carrying WAL LSN `lsn` and applying no data-group
/// entry: tenant 1, database DEFAULT, vShard 0.
pub(crate) fn wal_task(lsn: u64) -> ExecutionTask {
    ExecutionTask::with_wal_lsn(make_request(point_get()), Some(Lsn::new(lsn)))
}

/// An `ExecutionTask` carrying WAL LSN `lsn` that applies the data-group
/// entry at `entry`.
pub(crate) fn entry_task(lsn: u64, entry: WriteVersion) -> ExecutionTask {
    ExecutionTask::with_wal_lsn(
        Request {
            entry_version: Some(entry),
            ..make_request(point_get())
        },
        Some(Lsn::new(lsn)),
    )
}

/// An `ExecutionTask` homing to `vshard_id`, carrying no WAL LSN.
pub(crate) fn task_with_vshard(vshard_id: VShardId) -> ExecutionTask {
    ExecutionTask::new(Request {
        vshard_id,
        ..make_request(point_get())
    })
}

/// The version a write at `lsn` records on a vShard no entry ever wrote:
/// a single-node write.
pub(crate) fn local(lsn: u64) -> WriteVersion {
    WriteVersion::local_after(WriteVersion::ZERO, lsn)
}

/// The vShard a replayed write of `collection` in `db` records under when no
/// registered record stamp names one: the collection's home vShard.
pub(crate) fn replay_home(db: DatabaseId, collection: &str) -> VShardId {
    nodedb_types::CollectionKey::from_qualified_str(db, collection)
        .unwrap_or_else(|_| nodedb_types::CollectionKey::from_bare(db, collection))
        .vshard()
}

/// Put `{a: value}` as document `document_id` of `collection` under `task`.
pub(crate) fn point_put(
    core: &mut CoreLoop,
    task: &ExecutionTask,
    collection: &str,
    document_id: &str,
    surrogate: u32,
    value: &str,
) -> Response {
    core.execute_point_put(
        task,
        PointPutExec {
            tid: 1,
            collection,
            document_id,
            surrogate: Surrogate::new(surrogate),
            value: &doc_value("a", value),
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
        },
    )
}
