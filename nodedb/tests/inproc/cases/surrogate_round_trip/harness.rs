// SPDX-License-Identifier: BUSL-1.1

use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{Priority, Request, Status};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::data::executor::response_codec::decode_payload_to_json;
use nodedb::types::*;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::PhysicalPlan;

// ── Harness ────────────────────────────────────────────────────────────────

pub(super) fn open_core() -> (
    CoreLoop,
    Producer<BridgeRequest>,
    Consumer<BridgeResponse>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(128);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(128);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir.path(),
        std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

pub(super) fn make_req(plan: PhysicalPlan) -> Request {
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
        admission: nodedb::bridge::envelope::Admission::Admitted,
    }
}

/// Push a plan, tick, pop response, assert `Ok`. Returns the payload.
pub(super) fn send_ok(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> Vec<u8> {
    tx.try_push(BridgeRequest::unfloored(make_req(plan)))
        .unwrap();
    core.tick();
    let resp = rx.try_pop().unwrap();
    assert_eq!(
        resp.inner.status,
        Status::Ok,
        "expected Ok status, got {:?}: {:?}",
        resp.inner.status,
        resp.inner.error_code
    );
    resp.inner.payload.to_vec()
}

/// Batch-push plans without consuming responses in between, then drain
/// all responses at once. Each plan must succeed.
pub(super) fn send_batch_ok(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plans: Vec<PhysicalPlan>,
) {
    let n = plans.len();
    for plan in plans {
        tx.try_push(BridgeRequest::unfloored(make_req(plan)))
            .unwrap();
    }
    core.tick();
    for _ in 0..n {
        let resp = rx.try_pop().unwrap();
        assert_eq!(
            resp.inner.status,
            Status::Ok,
            "batch send: expected Ok, got {:?}: {:?}",
            resp.inner.status,
            resp.inner.error_code
        );
    }
}

pub(super) fn parse_json(payload: &[u8]) -> serde_json::Value {
    let s = decode_payload_to_json(payload);
    serde_json::from_str(&s).unwrap_or(serde_json::Value::Array(vec![]))
}

pub(super) fn extract_vector_surrogates(payload: &[u8]) -> Vec<u32> {
    parse_json(payload)
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|h| h.get("id").and_then(|v| v.as_str()))
        .filter_map(|key| nodedb_types::StorageKey::parse(key).map(|k| k.surrogate().as_u32()))
        .collect()
}

// ── Surrogate constants ────────────────────────────────────────────────────
//
// The 32-bit surrogate space is partitioned into non-overlapping ranges so
// uniqueness across engines is structurally guaranteed (no accidental reuse).
// A deliberate overlap set drives the three-way intersection test.
//
// Layout:
//   Vector  : 10, 11, 12, 13
//   Document: 20, 21, 22
//   Graph   : 10, 12, 30, 31   ← 10 and 12 shared with Vector
//   KV      : 40, 41
//   Columnar: 50, 51, 52
//   Array   : 10, 13, 60, 61   ← 10 and 13 shared with Vector; 12 NOT shared
//   FTS     : 70, 71  (written as Document because TextOp has no insert op;
//                      the doc engine drives the BM25 index)
//   Spatial : 80, 81  (written as Document because SpatialOp has no insert)
//
// Three-way intersection (Vector ∩ Graph ∩ Array) = {10}
//   - surrogate 10 is in ALL THREE: vector row, graph edge endpoints, array cell
//   - surrogate 11 is Vector-only
//   - surrogate 12 is Vector + Graph (not Array)
//   - surrogate 13 is Vector + Array (not Graph)
//   - surrogates 30,31 are Graph-only
//   - surrogates 60,61 are Array-only

pub(super) const V_SURS: &[u32] = &[10, 11, 12, 13]; // Vector
pub(super) const D_SURS: &[u32] = &[20, 21, 22]; // Document schemaless
pub(super) const G_SRC_SURS: &[u32] = &[10, 12, 30, 31]; // Graph src nodes
pub(super) const KV_SURS: &[u32] = &[40, 41]; // KV
pub(super) const COL_SURS: &[u32] = &[50, 51, 52]; // Columnar
pub(super) const ARR_SURS: &[u32] = &[10, 13, 60, 61]; // Array cells
pub(super) const FTS_SURS: &[u32] = &[70, 71]; // FTS (via Document)
pub(super) const SPATIAL_SURS: &[u32] = &[80, 81]; // Spatial (via Document + surrogate field)

// ── Helper: build a simple 1-D array schema ──────────────────────────────

pub(super) fn array_schema_1d(name: &str) -> nodedb_array::schema::ArraySchema {
    ArraySchemaBuilder::new(name)
        .dim(DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(255)),
        ))
        .attr(AttrSpec::new("v", AttrType::Float64, true))
        .tile_extents(vec![32])
        .build()
        .unwrap()
}
