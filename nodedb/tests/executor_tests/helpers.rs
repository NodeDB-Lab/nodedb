//! Shared test helpers for CoreLoop integration tests.

use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};

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
    let dir = tempfile::tempdir().unwrap();
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(0, req_rx, resp_tx, dir.path()).unwrap();
    (core, req_tx, resp_rx, dir)
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
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: 0,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: nodedb::event::EventSource::User,
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
        .try_push(BridgeRequest {
            inner: make_request(plan),
        })
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
        .try_push(BridgeRequest {
            inner: make_request(plan),
        })
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
