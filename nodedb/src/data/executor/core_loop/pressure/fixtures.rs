// SPDX-License-Identifier: BUSL-1.1

//! Test fixtures shared by the pressure module's unit tests.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor};
use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::{QualifiedCollection, Surrogate};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{Admission, ExemptReason, PhysicalPlan, Priority, Request};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::*;

/// Per-engine budget every test governor is built with.
const TEST_BUDGET_BYTES: usize = 10_000;

/// A core wired to empty 64-slot request and response rings.
pub(super) fn make_core() -> (
    CoreLoop,
    Producer<BridgeRequest>,
    Consumer<BridgeResponse>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir.path(),
        Arc::new(nodedb_types::OrdinalClock::new()),
        make_governor_at(EngineId::Vector, 0),
    )
    .expect("open core");
    (core, req_tx, resp_rx, dir)
}

/// A governor where every engine holds `TEST_BUDGET_BYTES`, with `engine`
/// pre-filled to `utilization_percent` of its budget.
pub(super) fn make_governor_at(engine: EngineId, utilization_percent: u8) -> Arc<MemoryGovernor> {
    let global_ceiling = TEST_BUDGET_BYTES * EngineId::ALL.len() * 2;
    let gov = MemoryGovernor::new(GovernorConfig {
        global_ceiling,
        engine_limits: EngineLimits::uniform(TEST_BUDGET_BYTES),
    })
    .expect("governor");
    let fill = (TEST_BUDGET_BYTES as u64 * utilization_percent as u64 / 100) as usize;
    if fill > 0
        && let Ok(token) = gov.try_reserve(DatabaseId::DEFAULT, TenantId::new(1), engine, fill)
    {
        // `ReservationToken` is RAII: dropping it releases the reservation and
        // resets the engine to 0%. Leak it so the budget stays charged for the
        // lifetime of the governor this fixture hands out.
        std::mem::forget(token);
    }
    Arc::new(gov)
}

/// A minimal vector-insert request, numbered so several can coexist.
pub(super) fn make_stub_request(id: u64) -> Request {
    Request {
        request_id: RequestId::new(id),
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Vector(VectorOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "test"),
            vector: vec![0.1],
            dim: 1,
            field_name: "emb".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: None,
            provenance: None,
        }),
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
        admission: Admission::Exempt(ExemptReason::Read),
    }
}

/// Push one request onto the inbound ring.
pub(super) fn push_request(tx: &mut Producer<BridgeRequest>, id: u64) {
    tx.try_push(BridgeRequest::unfloored(make_stub_request(id)))
        .expect("ring has room");
}

/// A task wrapping [`make_stub_request`], for the per-handler pressure gate.
pub(super) fn make_task() -> ExecutionTask {
    ExecutionTask::new(make_stub_request(1))
}
