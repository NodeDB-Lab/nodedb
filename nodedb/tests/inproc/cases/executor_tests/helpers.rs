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
        nodedb::data::executor::core_loop::CoreOpenParams::unencrypted(
            core_id,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            nodedb::data::executor::core_loop::test_governor(),
        ),
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
        commit_hlc: None,
        system_ordinal: None,
        config_pins: None,
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

/// Render a Data-Plane payload as JSON text the way a client JSON path
/// does. Executor fixtures read scan payloads, so rows decode as
/// `{id, data}` scan envelopes; a native row renders as its typed values.
pub fn payload_json(payload: &[u8]) -> String {
    nodedb::data::executor::response_codec::decode_row_payload_to_json(
        payload,
        nodedb::data::executor::response_codec::RowContext::ScanEnvelope,
    )
    .expect("payload rows decode")
}

/// Render a query or join payload as JSON text. Each row is a flat body,
/// a plain map or a native object, and renders as its typed values.
pub fn rows_json(payload: &[u8]) -> String {
    nodedb::data::executor::response_codec::decode_row_payload_to_json(
        payload,
        nodedb::data::executor::response_codec::RowContext::Flat,
    )
    .expect("payload rows decode")
}

pub fn payload_value(payload: &[u8]) -> serde_json::Value {
    let json = payload_json(payload);
    serde_json::from_str(&json).unwrap_or(serde_json::Value::Null)
}

/// The rows of a `NeighborsMulti` payload as JSON. A row's `properties`
/// travel as the edge's stored property image (`bin`). Each renders here as
/// its decoded property object.
pub fn neighbor_rows_value(payload: &[u8]) -> Vec<serde_json::Value> {
    use nodedb_types::Value;
    if payload.is_empty() {
        return Vec::new();
    }
    let Value::Array(rows) = nodedb_types::value_from_msgpack(payload).expect("rows decode") else {
        panic!("neighbor rows are an array");
    };
    rows.into_iter()
        .map(|row| {
            let Value::Object(mut fields) = row else {
                panic!("a neighbor row is a map: {row:?}");
            };
            if let Some(Value::Bytes(image)) = fields.remove("properties") {
                let properties =
                    nodedb_graph::decode_edge_properties(&image).expect("properties decode");
                fields.insert("properties".to_string(), Value::Object(properties));
            }
            serde_json::Value::from(Value::Object(fields))
        })
        .collect()
}

/// A deterministic bound surrogate for a document id, never
/// `Surrogate::ZERO`. Executor-direct fixtures bypass the catalog, so each
/// row threads its own binding and lands on its own storage key.
pub fn doc_surrogate(id: &str) -> nodedb_types::Surrogate {
    nodedb_test_support::kv_rows::kv_row_surrogate(id.as_bytes())
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

/// Register `collection` of database DEFAULT under `tenant_id` as a
/// schemaless collection whose full-text index covers `text_fields`.
///
/// A write indexes only declared fields, and a text read on an undeclared
/// field errors. A fixture that searches text registers its fields first.
pub fn register_text_fields(
    core: &mut CoreLoop,
    req_tx: &mut Producer<BridgeRequest>,
    resp_rx: &mut Consumer<BridgeResponse>,
    tenant_id: u64,
    collection: &str,
    text_fields: &[&str],
) {
    let fields: Vec<String> = text_fields.iter().map(|f| f.to_string()).collect();
    send_ok_as_tenant(
        core,
        req_tx,
        resp_rx,
        tenant_id,
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Register {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb::types::DatabaseId::DEFAULT,
                collection,
            ),
            indexes: Vec::new(),
            crdt_enabled: false,
            storage_mode: Default::default(),
            enforcement: Box::default(),
            bitemporal: false,
            conflict_policy: None,
            timeseries: None,
            vector_primary: None,
            vector_fields: Vec::new(),
            declared_columns: Vec::new(),
            declared_key: None,
            text_fields: nodedb_physical::physical_plan::TextFieldSet::all_readable(fields),
            incarnation: nodedb_types::Hlc::ZERO,
            digest: 0,
        }),
    );
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
