//! Integration-style dispatch tests for array read handlers.
//!
//! Each test drives a fresh `CoreLoop` through a sequence of bridge
//! requests (OpenArray → Put [→ Flush]* → read op) and inspects the
//! response payload. The harness mirrors the smoke tests in
//! `mutate.rs`; we keep them in a sibling file because `mutate.rs` is
//! already close to the file-size limit.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_array::query::slice::{DimRange, Slice as ArraySlice};
use nodedb_array::schema::ArraySchema;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_types::{ArrayCell, Value};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::bridge::physical_plan::{ArrayBinaryOp, ArrayOp, ArrayReducer};
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::array::wal::ArrayPutCell;
use crate::types::*;

fn make_request(plan: PhysicalPlan, id: u64) -> Request {
    Request {
        request_id: RequestId::new(id),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: 0,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
    }
}

fn schema_2d_f64(name: &str) -> ArraySchema {
    ArraySchemaBuilder::new(name)
        .dim(DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
        ))
        .dim(DimSpec::new(
            "y",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
        ))
        .attr(AttrSpec::new("v", AttrType::Float64, true))
        .tile_extents(vec![4, 4])
        .build()
        .unwrap()
}

struct Harness {
    core: CoreLoop,
    req_tx: Producer<BridgeRequest>,
    resp_rx: Consumer<BridgeResponse>,
    next_id: u64,
    _dir: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .unwrap();
        Harness {
            core,
            req_tx,
            resp_rx,
            next_id: 1,
            _dir: dir,
        }
    }

    fn send(&mut self, op: ArrayOp) -> crate::bridge::envelope::Response {
        let id = self.next_id;
        self.next_id += 1;
        self.req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::Array(op), id),
            })
            .unwrap();
        self.core.tick();
        let resp = self.resp_rx.try_pop().unwrap();
        resp.inner
    }

    fn open(&mut self, aid: &ArrayId, schema: &ArraySchema, schema_hash: u64) {
        let bytes = zerompk::to_msgpack_vec(schema).unwrap();
        let r = self.send(ArrayOp::OpenArray {
            array_id: aid.clone(),
            schema_msgpack: bytes,
            schema_hash,
        });
        assert_eq!(r.status, Status::Ok, "open failed: {r:?}");
    }

    fn put(&mut self, aid: &ArrayId, cells: Vec<ArrayPutCell>, lsn: u64) {
        let bytes = zerompk::to_msgpack_vec(&cells).unwrap();
        let r = self.send(ArrayOp::Put {
            array_id: aid.clone(),
            cells_msgpack: bytes,
            wal_lsn: lsn,
        });
        assert_eq!(r.status, Status::Ok, "put failed: {r:?}");
    }

    fn flush(&mut self, aid: &ArrayId) {
        let r = self.send(ArrayOp::Flush {
            array_id: aid.clone(),
            wal_lsn: 0,
        });
        assert_eq!(r.status, Status::Ok, "flush failed: {r:?}");
    }
}

fn cell(x: i64, y: i64, v: f64) -> ArrayPutCell {
    ArrayPutCell {
        coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
        attrs: vec![CellValue::Float64(v)],
    }
}

fn decode_agg_rows(bytes: &[u8]) -> Vec<std::collections::BTreeMap<String, serde_json::Value>> {
    // Aggregate payloads are zerompk maps; transcode to JSON via the
    // shared msgpack→JSON streamer (same path pgwire uses), then parse.
    let json = nodedb_types::msgpack_to_json_string(bytes).expect("agg msgpack→json");
    serde_json::from_str(&json).expect("agg json parse")
}

fn decode_value_vec(bytes: &[u8]) -> Vec<Value> {
    // Slice/Project/Elementwise ship rows as a standard msgpack array of
    // `value_to_msgpack`-encoded values — `Value::NdArrayCell` lands as
    // `{coords: [...], attrs: [...]}` so the pgwire transcoder is clean.
    // Tests parse the JSON back into typed `Value`s here.
    let json = nodedb_types::msgpack_to_json_string(bytes).expect("payload msgpack→json");
    let arr: serde_json::Value = serde_json::from_str(&json).expect("payload json parse");
    let arr = arr.as_array().expect("rows are an array").clone();
    arr.into_iter().map(json_to_value).collect()
}

fn json_to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Object(map)
            if map.contains_key("coords") && map.contains_key("attrs") =>
        {
            let coords = map["coords"]
                .as_array()
                .expect("coords array")
                .iter()
                .cloned()
                .map(json_to_value)
                .collect();
            let attrs = map["attrs"]
                .as_array()
                .expect("attrs array")
                .iter()
                .cloned()
                .map(json_to_value)
                .collect();
            Value::NdArrayCell(ArrayCell { coords, attrs })
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s),
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Array(a) => Value::Array(a.into_iter().map(json_to_value).collect()),
        serde_json::Value::Object(_) => Value::Null,
    }
}

#[test]
fn slice_returns_only_cells_in_range() {
    let mut h = Harness::new();
    let s = schema_2d_f64("t6_slice");
    let aid = ArrayId::new(TenantId::new(1), "t6_slice");
    h.open(&aid, &s, 0xA1);
    h.put(
        &aid,
        vec![
            cell(0, 0, 1.0),
            cell(1, 1, 2.0),
            cell(5, 5, 3.0),
            cell(7, 7, 4.0),
        ],
        100,
    );
    h.flush(&aid);

    // Slice x in [4, 9]: expects (5,5)=3 and (7,7)=4.
    let slice = ArraySlice::new(vec![
        Some(DimRange::new(DomainBound::Int64(4), DomainBound::Int64(9))),
        None,
    ]);
    let slice_bytes = zerompk::to_msgpack_vec(&slice).unwrap();
    let r = h.send(ArrayOp::Slice {
        array_id: aid.clone(),
        slice_msgpack: slice_bytes,
        attr_projection: vec![],
        limit: 0,
    });
    assert_eq!(r.status, Status::Ok, "slice failed: {r:?}");
    let rows = decode_value_vec(r.payload.as_ref());
    assert_eq!(rows.len(), 2, "expected two cells, got {rows:?}");
    let mut sums = 0.0;
    for v in rows {
        match v {
            Value::NdArrayCell(ArrayCell { attrs, .. }) => match &attrs[0] {
                Value::Float(f) => sums += f,
                other => panic!("attr not Float: {other:?}"),
            },
            other => panic!("row not NdArrayCell: {other:?}"),
        }
    }
    assert!((sums - 7.0).abs() < 1e-9);
}

#[test]
fn aggregate_sum_scalar_across_multiple_tiles() {
    let mut h = Harness::new();
    let s = schema_2d_f64("t6_agg");
    let aid = ArrayId::new(TenantId::new(1), "t6_agg");
    h.open(&aid, &s, 0xA2);

    // Two batches forced into separate sealed segments via Flush.
    h.put(&aid, vec![cell(0, 0, 1.0), cell(1, 1, 2.0)], 1);
    h.flush(&aid);
    h.put(&aid, vec![cell(2, 2, 3.0), cell(3, 3, 4.0)], 2);
    h.flush(&aid);

    let r = h.send(ArrayOp::Aggregate {
        array_id: aid.clone(),
        attr_idx: 0,
        reducer: ArrayReducer::Sum,
        group_by_dim: -1,
    });
    assert_eq!(r.status, Status::Ok, "agg failed: {r:?}");
    let rows = decode_agg_rows(r.payload.as_ref());
    assert_eq!(rows.len(), 1);
    let f = rows[0]
        .get("result")
        .and_then(|v| v.as_f64())
        .expect("result f64");
    assert!((f - 10.0).abs() < 1e-9, "sum got {f}");
}

#[test]
fn aggregate_group_by_dim_buckets_per_x() {
    let mut h = Harness::new();
    let s = schema_2d_f64("t6_grp");
    let aid = ArrayId::new(TenantId::new(1), "t6_grp");
    h.open(&aid, &s, 0xA3);
    // Two cells per x-row across two tiles.
    h.put(&aid, vec![cell(0, 0, 1.0), cell(0, 1, 2.0)], 1);
    h.flush(&aid);
    h.put(&aid, vec![cell(1, 0, 10.0), cell(1, 1, 20.0)], 2);
    h.flush(&aid);

    let r = h.send(ArrayOp::Aggregate {
        array_id: aid.clone(),
        attr_idx: 0,
        reducer: ArrayReducer::Sum,
        group_by_dim: 0,
    });
    assert_eq!(r.status, Status::Ok, "group agg failed: {r:?}");
    let rows = decode_agg_rows(r.payload.as_ref());
    assert_eq!(rows.len(), 2);
    let mut totals: std::collections::HashMap<i64, f64> = std::collections::HashMap::new();
    for row in rows {
        let g = row
            .get("group")
            .and_then(|v| v.as_i64())
            .expect("group i64");
        let r = row
            .get("result")
            .and_then(|v| v.as_f64())
            .expect("result f64");
        totals.insert(g, r);
    }
    assert!((totals[&0] - 3.0).abs() < 1e-9);
    assert!((totals[&1] - 30.0).abs() < 1e-9);
}

#[test]
fn elementwise_add_two_arrays() {
    let mut h = Harness::new();
    let s = schema_2d_f64("t6_ew");
    let left = ArrayId::new(TenantId::new(1), "t6_ew_l");
    let right = ArrayId::new(TenantId::new(1), "t6_ew_r");
    h.open(&left, &s, 0xA4);
    h.open(&right, &s, 0xA4);
    h.put(&left, vec![cell(0, 0, 1.0), cell(1, 1, 2.0)], 1);
    h.put(&right, vec![cell(0, 0, 10.0), cell(1, 1, 20.0)], 2);
    h.flush(&left);
    h.flush(&right);

    let r = h.send(ArrayOp::Elementwise {
        left: left.clone(),
        right: right.clone(),
        op: ArrayBinaryOp::Add,
        attr_idx: 0,
    });
    assert_eq!(r.status, Status::Ok, "ew failed: {r:?}");
    let rows = decode_value_vec(r.payload.as_ref());
    assert_eq!(rows.len(), 2);
    let mut total = 0.0;
    for v in rows {
        match v {
            Value::NdArrayCell(ArrayCell { attrs, .. }) => match &attrs[0] {
                Value::Float(f) => total += f,
                Value::Integer(i) => total += *i as f64,
                other => panic!("attr not numeric: {other:?}"),
            },
            other => panic!("row not NdArrayCell: {other:?}"),
        }
    }
    assert!((total - 33.0).abs() < 1e-9);
}

#[test]
fn elementwise_schema_hash_mismatch_errors() {
    let mut h = Harness::new();
    let s = schema_2d_f64("t6_ew_mis");
    let left = ArrayId::new(TenantId::new(1), "t6_ew_mis_l");
    let right = ArrayId::new(TenantId::new(1), "t6_ew_mis_r");
    h.open(&left, &s, 0xB1);
    h.open(&right, &s, 0xB2); // different hash
    let r = h.send(ArrayOp::Elementwise {
        left,
        right,
        op: ArrayBinaryOp::Add,
        attr_idx: 0,
    });
    assert_ne!(r.status, Status::Ok);
}
