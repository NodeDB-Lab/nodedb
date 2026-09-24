// SPDX-License-Identifier: BUSL-1.1

//! Shared test helpers for CoreLoop integration tests.
#![allow(dead_code)]

use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{Priority, Request, Status};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::PhysicalPlan;

/// Bundles the core + bridge channels that every test helper needs.
/// Eliminates repeated `(core, tx, rx)` triple parameters.
/// `_dir` keeps the TempDir alive — dropped when TestCtx is dropped.
pub struct TestCtx {
    pub core: CoreLoop,
    pub tx: Producer<BridgeRequest>,
    pub rx: Consumer<BridgeResponse>,
    _dir: tempfile::TempDir,
}

pub fn make_core() -> (
    CoreLoop,
    Producer<BridgeRequest>,
    Consumer<BridgeResponse>,
    tempfile::TempDir,
) {
    make_core_with_id(0)
}

pub fn make_core_with_id(
    core_id: usize,
) -> (
    CoreLoop,
    Producer<BridgeRequest>,
    Consumer<BridgeResponse>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        core_id,
        req_rx,
        resp_tx,
        dir.path(),
        std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

pub fn make_ctx_with_id(core_id: usize) -> TestCtx {
    let (core, tx, rx, dir) = make_core_with_id(core_id);
    TestCtx {
        core,
        tx,
        rx,
        _dir: dir,
    }
}

pub fn make_ctx() -> TestCtx {
    let (core, tx, rx, dir) = make_core();
    TestCtx {
        core,
        tx,
        rx,
        _dir: dir,
    }
}

pub fn make_request(plan: PhysicalPlan) -> Request {
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
        event_source: nodedb::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: nodedb::bridge::envelope::Admission::Admitted,
    }
}

pub fn make_request_with_id(id: u64, plan: PhysicalPlan) -> Request {
    Request {
        request_id: RequestId::new(id),
        ..make_request(plan)
    }
}

/// Push a request, tick, pop response — asserts Ok status.
pub fn send_ok(
    core: &mut CoreLoop,
    req_tx: &mut Producer<BridgeRequest>,
    resp_rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> Vec<u8> {
    req_tx
        .try_push(BridgeRequest::unfloored(make_request(plan)))
        .unwrap();
    core.tick();
    let resp = resp_rx.try_pop().unwrap();
    assert_eq!(
        resp.inner.status,
        Status::Ok,
        "expected Ok, got {:?}",
        resp.inner.error_code
    );
    resp.inner.payload.to_vec()
}

/// Push a request, tick, pop response — returns the raw response.
pub fn send_raw(
    core: &mut CoreLoop,
    req_tx: &mut Producer<BridgeRequest>,
    resp_rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> nodedb::bridge::envelope::Response {
    req_tx
        .try_push(BridgeRequest::unfloored(make_request(plan)))
        .unwrap();
    core.tick();
    resp_rx.try_pop().unwrap().inner
}

pub fn payload_json(payload: &[u8]) -> String {
    nodedb::data::executor::response_codec::decode_payload_to_json(payload)
}

pub fn payload_value(payload: &[u8]) -> serde_json::Value {
    let json = payload_json(payload);
    serde_json::from_str(&json).unwrap_or(serde_json::Value::Null)
}

// ── Tenant-aware helpers ────────────────────────────────────────────

/// Canonical tenant IDs used across tenant-isolation integration tests.
/// Two distinct, non-zero IDs suffice to exercise every isolation boundary;
/// keep them stable so test output is comparable across files.
pub const TENANT_A: u64 = 10;
pub const TENANT_B: u64 = 20;

pub fn make_request_for_tenant(tenant_id: u64, plan: PhysicalPlan) -> Request {
    Request {
        tenant_id: TenantId::new(tenant_id),
        ..make_request(plan)
    }
}

/// Push a request as a specific tenant, tick, pop response — asserts Ok status.
pub fn send_ok_as_tenant(
    core: &mut CoreLoop,
    req_tx: &mut Producer<BridgeRequest>,
    resp_rx: &mut Consumer<BridgeResponse>,
    tenant_id: u64,
    plan: PhysicalPlan,
) -> Vec<u8> {
    req_tx
        .try_push(BridgeRequest::unfloored(make_request_for_tenant(
            tenant_id, plan,
        )))
        .unwrap();
    core.tick();
    let resp = resp_rx.try_pop().unwrap();
    assert_eq!(
        resp.inner.status,
        Status::Ok,
        "expected Ok for tenant {tenant_id}, got {:?}",
        resp.inner.error_code
    );
    resp.inner.payload.to_vec()
}

/// Push a request as a specific tenant, tick, pop response — returns raw response.
pub fn send_raw_as_tenant(
    core: &mut CoreLoop,
    req_tx: &mut Producer<BridgeRequest>,
    resp_rx: &mut Consumer<BridgeResponse>,
    tenant_id: u64,
    plan: PhysicalPlan,
) -> nodedb::bridge::envelope::Response {
    req_tx
        .try_push(BridgeRequest::unfloored(make_request_for_tenant(
            tenant_id, plan,
        )))
        .unwrap();
    core.tick();
    resp_rx.try_pop().unwrap().inner
}
