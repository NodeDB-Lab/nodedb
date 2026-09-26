// SPDX-License-Identifier: BUSL-1.1

//! Plan builders and a single-core commit driver shared by the transaction
//! cross-engine tests.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{Priority, Request, Status};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::{
    AggregateSpec, ColumnarInsertIntent, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp,
    PhysicalPlan, QueryOp, TimeseriesOp, VectorOp,
};

pub use crate::tx_commit::commit_plans;
use nodedb_types::OrdinalClock;

// ── Core setup ──────────────────────────────────────────────────────────────

pub fn make_core() -> (
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

pub fn make_request(plan: PhysicalPlan) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        database_id: nodedb::types::DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
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

pub fn send_ok(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> Vec<u8> {
    tx.try_push(BridgeRequest::unfloored(make_request(plan)))
        .unwrap();
    core.tick();
    let resp = rx.try_pop().unwrap();
    assert_eq!(
        resp.inner.status,
        Status::Ok,
        "send_ok expected Ok, got {:?}",
        resp.inner.error_code
    );
    resp.inner.payload.to_vec()
}

pub fn send_raw(
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

pub fn payload_json(payload: &[u8]) -> String {
    nodedb::data::executor::response_codec::decode_payload_to_json(payload)
}

// ── Plan builders ────────────────────────────────────────────────────────────

/// Qualify a plain collection name for the default database, matching the
/// helpers below: every caller here operates on the default database.
fn qualify(collection: &str) -> nodedb_types::QualifiedCollection {
    nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, collection)
}

pub fn doc_put(collection: &str, doc_id: &str, val: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointPut {
        collection: qualify(collection),
        document_id: doc_id.into(),
        value: val.to_vec(),
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    })
}

pub fn doc_get(collection: &str, doc_id: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointGet {
        collection: qualify(collection),
        document_id: doc_id.into(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
    })
}

/// Fails if doc_id already exists (if_absent=false but key present causes conflict).
pub fn doc_conflict(collection: &str, doc_id: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: qualify(collection),
        document_id: doc_id.into(),
        value: b"conflict".to_vec(),
        surrogate: nodedb_types::Surrogate::ZERO,
        if_absent: false,
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    })
}

pub fn edge_put(collection: &str, src: &str, dst: &str) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: qualify(collection),
        src_id: src.into(),
        label: "REL".into(),
        dst_id: dst.into(),
        properties: Vec::new(),
        src_surrogate: nodedb_types::Surrogate::ZERO,
        dst_surrogate: nodedb_types::Surrogate::ZERO,
    })
}

pub fn neighbors(_collection: &str, src: &str) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::Neighbors {
        collection: None,
        node_id: src.into(),
        edge_label: Some("REL".into()),
        direction: nodedb::engine::graph::edge_store::Direction::Out,
        rls_filters: Vec::new(),
    })
}

pub fn kv_put(key: &[u8], value: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: qualify("kv_coll"),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    })
}

pub fn kv_get(key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Get {
        collection: qualify("kv_coll"),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    })
}

pub fn columnar_insert(collection: &str, id: &str, val: i64) -> PhysicalPlan {
    let rows = serde_json::json!([{"id": id, "val": val}]);
    let payload = nodedb_types::json_to_msgpack(&rows).unwrap();
    PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: qualify(collection),
        payload,
        format: "msgpack".into(),
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        // One surrogate per row, as the planner binds it: a transaction
        // stages each columnar row under its surrogate.
        surrogates: vec![row_surrogate(id)],
        schema_bytes: Vec::new(),
        provenance: None,
        wal_lsn: None,
        // These helpers build plans for transaction-batch tests on
        // collections that carry no RLS policy.
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
    })
}

/// A stable surrogate for the row whose primary key is `id`. Never zero, and
/// distinct from the small fixed surrogates the other helpers use.
fn row_surrogate(id: &str) -> nodedb_types::Surrogate {
    let hash = id.bytes().fold(2_166_136_261u32, |h, b| {
        (h ^ u32::from(b)).wrapping_mul(16_777_619)
    });
    nodedb_types::Surrogate::new(hash | 0x8000_0000)
}

pub fn columnar_count(collection: &str) -> PhysicalPlan {
    PhysicalPlan::Query(QueryOp::Aggregate {
        collection: qualify(collection),
        input: None,
        group_by: Vec::new(),
        aggregates: vec![AggregateSpec {
            function: "count".into(),
            alias: "count(*)".into(),
            user_alias: None,
            field: "*".into(),
            expr: None,
        }],
        filters: Vec::new(),
        having: Vec::new(),
        limit: 10,
        sub_group_by: Vec::new(),
        sub_aggregates: Vec::new(),
        grouping_sets: Vec::new(),
        sort_keys: Vec::new(),
    })
}

pub fn timeseries_ingest(collection: &str, ilp: &str) -> PhysicalPlan {
    PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: qualify(collection),
        payload: ilp.as_bytes().to_vec(),
        format: "ilp".into(),
        wal_lsn: None,
        surrogates: Vec::new(),
        provenance: None,
        // No RLS policy on the collections these helpers build plans for.
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
    })
}

pub fn timeseries_scan(collection: &str) -> PhysicalPlan {
    PhysicalPlan::Timeseries(TimeseriesOp::Scan {
        collection: qualify(collection),
        time_range: (0, i64::MAX),
        projection: Vec::new(),
        limit: 100,
        filters: Vec::new(),
        sort_keys: Vec::new(),
        bucket_interval_ms: 0,
        group_by: Vec::new(),
        aggregates: Vec::new(),
        gap_fill: String::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        computed_columns: Vec::new(),
    })
}

pub fn crdt_apply(collection: &str, doc_id: &str) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: qualify(collection),
        document_id: doc_id.into(),
        delta: vec![0u8; 8],
        peer_id: 1,
        mutation_id: 42,
        surrogate: nodedb_types::Surrogate::ZERO,
        provenance: None,
        constraint_version_required: 0,
        expected_frontier_digest: None,
    })
}

/// A vector-primary direct insert of a 3-dimensional vector.
pub fn vector_direct_insert(collection: &str, surrogate: u32) -> PhysicalPlan {
    let mut payload = std::collections::HashMap::new();
    payload.insert(
        "id".to_string(),
        nodedb_types::Value::String(format!("r{surrogate}")),
    );
    PhysicalPlan::Vector(VectorOp::DirectInsert {
        collection: qualify(collection),
        field: "vec".into(),
        surrogate: nodedb_types::Surrogate::new(surrogate),
        pk_bytes: format!("r{surrogate}").into_bytes(),
        vector: vec![0.5, 0.5, 0.5],
        payload: zerompk::to_msgpack_vec(&payload).unwrap(),
        quantization: nodedb_types::VectorQuantization::None,
        storage_dtype: nodedb_types::VectorStorageDtype::F32,
        payload_indexes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
    })
}

/// A CRDT row write of `{"title": title}`.
pub fn crdt_upsert(collection: &str, doc_id: &str, surrogate: u32) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::DocUpsert {
        collection: qualify(collection),
        document_id: doc_id.into(),
        fields_json: r#"{"title":"staged"}"#.into(),
        surrogate: nodedb_types::Surrogate::new(surrogate),
        partial: false,
        verb: nodedb_physical::physical_plan::CrdtWriteVerb::Insert,
        returning: None,
        rls_filters: Vec::new(),
    })
}

/// `plans` followed by two inserts of one key: the transaction's staging
/// refuses the second as a unique violation, so the transaction commits
/// none of `plans`.
pub fn with_unique_refusal(mut plans: Vec<PhysicalPlan>) -> Vec<PhysicalPlan> {
    let insert = PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: qualify("refusal_probe"),
        document_id: "dup".into(),
        value: nodedb_types::json_to_msgpack(&serde_json::json!({"n": 1})).unwrap(),
        surrogate: nodedb_types::Surrogate::new(9_001),
        if_absent: false,
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    });
    plans.push(insert.clone());
    plans.push(insert);
    plans
}

// ── Assertion helpers ─────────────────────────────────────────────────────────

/// Assert that a KV key is absent (NotFound or empty payload).
pub fn assert_kv_absent(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    key: &[u8],
) {
    let r = send_raw(core, tx, rx, kv_get(key));
    let is_absent = r.status == Status::Error || r.payload.is_empty();
    assert!(
        is_absent,
        "KV key {key:?} must be absent after rollback; status={:?} payload_len={}",
        r.status,
        r.payload.len()
    );
}

/// Assert that a document is absent (NotFound or empty payload).
pub fn assert_doc_absent(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    collection: &str,
    doc_id: &str,
) {
    let r = send_raw(core, tx, rx, doc_get(collection, doc_id));
    let is_absent = r.status == Status::Error || r.payload.is_empty() || r.payload.len() <= 3;
    assert!(
        is_absent,
        "doc {collection}/{doc_id} must be absent after rollback; status={:?} payload_len={}",
        r.status,
        r.payload.len()
    );
}

/// Assert that graph node `src` has no REL neighbors.
pub fn assert_edge_absent(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    collection: &str,
    src: &str,
) {
    let r = send_raw(core, tx, rx, neighbors(collection, src));
    assert_eq!(r.status, Status::Ok);
    assert!(
        r.payload.len() <= 3,
        "edge from {src} must be absent after rollback; payload_len={}",
        r.payload.len()
    );
}

/// Assert columnar row count equals `expected`.
/// An empty or missing collection counts as 0.
pub fn assert_columnar_count(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    collection: &str,
    expected: i64,
) {
    let payload = send_ok(core, tx, rx, columnar_count(collection));
    let json = payload_json(&payload);
    let val: serde_json::Value = serde_json::from_str(&json).unwrap_or(serde_json::Value::Null);
    let count = val
        .as_array()
        .and_then(|a| a.first())
        .and_then(|row| row.get("count(*)"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0); // empty array = no rows = 0
    assert_eq!(
        count, expected,
        "columnar {collection} count: expected {expected}, got {count}; json={json}"
    );
}

/// Assert timeseries row count equals `expected`.
pub fn assert_ts_count(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    collection: &str,
    expected: usize,
) {
    let r = send_raw(core, tx, rx, timeseries_scan(collection));
    assert_eq!(r.status, Status::Ok);
    let json = payload_json(&r.payload);
    let val: serde_json::Value =
        serde_json::from_str(&json).unwrap_or(serde_json::Value::Array(vec![]));
    let arr = val.as_array().map(|v| v.len()).unwrap_or(0);
    assert_eq!(
        arr, expected,
        "timeseries {collection} row count: expected {expected}, got {arr}; json={json}"
    );
}
