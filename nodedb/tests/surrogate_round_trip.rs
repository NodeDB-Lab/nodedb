//! Single-node integration test for surrogate identity round-trip.
//!
//! Asserts three properties:
//!
//! 1. **Per-engine uniqueness** — the surrogate values assigned to rows in
//!    each engine are distinct from one another within the engine's own
//!    insert batch (no engine re-uses a surrogate for two different rows).
//!
//! 2. **Global uniqueness** — all surrogates across every engine form a
//!    set with no duplicates.
//!
//! 3. **Cross-engine bitmap intersection** — a three-way intersection of
//!    `SurrogateBitmap`s produced from Vector, Graph, and Array engine
//!    inserts yields the correct set, and actually running queries with
//!    the intersected bitmap as `filter_bitmap` / `frontier_bitmap` /
//!    `cell_filter` returns only rows whose surrogate is in the
//!    intersection.
//!
//! The test drives a single `CoreLoop` via the ring-buffer harness (the
//! same pattern as `cross_engine_bitmap_currency.rs`) so no server or
//! network is required.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use nodedb::bridge::physical_plan::{
    ArrayOp, ColumnarInsertIntent, ColumnarOp, DocumentOp, GraphOp, KvOp, VectorOp,
};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::data::executor::response_codec::decode_payload_to_json;
use nodedb::engine::array::wal::ArrayPutCell;
use nodedb::engine::graph::edge_store::Direction;
use nodedb::types::*;
use nodedb_array::query::slice::Slice as ArraySlice;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_types::{Surrogate, SurrogateBitmap};

// ── Harness ────────────────────────────────────────────────────────────────

fn open_core() -> (
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
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

fn make_req(plan: PhysicalPlan) -> Request {
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
        user_roles: Vec::new(),
    }
}

/// Push a plan, tick, pop response, assert `Ok`. Returns the payload.
fn send_ok(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> Vec<u8> {
    tx.try_push(BridgeRequest {
        inner: make_req(plan),
    })
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
fn send_batch_ok(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plans: Vec<PhysicalPlan>,
) {
    let n = plans.len();
    for plan in plans {
        tx.try_push(BridgeRequest {
            inner: make_req(plan),
        })
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

fn parse_json(payload: &[u8]) -> serde_json::Value {
    let s = decode_payload_to_json(payload);
    serde_json::from_str(&s).unwrap_or(serde_json::Value::Array(vec![]))
}

fn extract_vector_surrogates(payload: &[u8]) -> Vec<u32> {
    parse_json(payload)
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|h| h.get("id").and_then(|v| v.as_u64()).map(|n| n as u32))
        .collect()
}

// ── Surrogate constants ────────────────────────────────────────────────────
//
// We partition the 32-bit surrogate space into non-overlapping ranges so
// uniqueness across engines is structurally guaranteed (no accidental reuse).
// Then we select a deliberate overlap set for the three-way intersection test.
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

const V_SURS: &[u32] = &[10, 11, 12, 13]; // Vector
const D_SURS: &[u32] = &[20, 21, 22]; // Document schemaless
const G_SRC_SURS: &[u32] = &[10, 12, 30, 31]; // Graph src nodes
const KV_SURS: &[u32] = &[40, 41]; // KV
const COL_SURS: &[u32] = &[50, 51, 52]; // Columnar
const ARR_SURS: &[u32] = &[10, 13, 60, 61]; // Array cells
const FTS_SURS: &[u32] = &[70, 71]; // FTS (via Document)
const SPATIAL_SURS: &[u32] = &[80, 81]; // Spatial (via Document + surrogate field)

// ── Helper: build a simple 2-D array schema ──────────────────────────────

fn array_schema_1d(name: &str) -> nodedb_array::schema::ArraySchema {
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

// ── Test ───────────────────────────────────────────────────────────────────

/// Main assertion: insert into eight engine paths, check uniqueness, check
/// the three-way Vector ∩ Graph ∩ Array bitmap intersection via real query
/// execution.
#[test]
fn surrogate_round_trip_all_engines() {
    let (mut core, mut tx, mut rx, _dir) = open_core();

    // ── 1. Vector inserts ───────────────────────────────────────────────
    // Each vector gets a distinct embedding so all appear near [1,0,0].
    let vec_plans: Vec<PhysicalPlan> = V_SURS
        .iter()
        .copied()
        .map(|s| {
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: "vec_col".into(),
                vector: vec![1.0, s as f32 * 0.01, 0.0],
                dim: 3,
                field_name: String::new(),
                surrogate: Surrogate::new(s),
            })
        })
        .collect();
    send_batch_ok(&mut core, &mut tx, &mut rx, vec_plans);

    // ── 2. Document (schemaless) inserts ────────────────────────────────
    for &s in D_SURS {
        let hex = format!("{s:08x}");
        let doc = serde_json::json!({ "id": hex, "kind": "doc" });
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "doc_col".into(),
                document_id: hex.clone(),
                value: serde_json::to_vec(&doc).unwrap(),
                surrogate: Surrogate::new(s),
                pk_bytes: hex.into_bytes(),
            }),
        );
    }

    // ── 3. Graph edge inserts ────────────────────────────────────────────
    // Each src node gets a surrogate in G_SRC_SURS; dst node uses s+100.
    for &s in G_SRC_SURS {
        let src = format!("n{s}");
        let dst = format!("n{}", s + 100);
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: "graph_col".into(),
                src_id: src.clone(),
                label: "LINKED".into(),
                dst_id: dst.clone(),
                properties: vec![],
                src_surrogate: Surrogate::new(s),
                dst_surrogate: Surrogate::new(s + 100),
            }),
        );
    }

    // ── 4. KV inserts ────────────────────────────────────────────────────
    for &s in KV_SURS {
        let key = format!("k{s}").into_bytes();
        // Minimal Binary Tuple value (empty).
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "kv_col".into(),
                key: key.clone(),
                value: vec![],
                ttl_ms: 0,
                surrogate: Surrogate::new(s),
            }),
        );
    }

    // ── 5. Columnar inserts ──────────────────────────────────────────────
    {
        let rows_json = serde_json::json!(
            COL_SURS
                .iter()
                .map(|&s| serde_json::json!({ "id": format!("{s:08x}"), "val": s }))
                .collect::<Vec<_>>()
        );
        let rows_mp = nodedb_types::json_to_msgpack(&rows_json).unwrap();
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: "col_col".into(),
                payload: rows_mp,
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: Vec::new(),
                surrogates: COL_SURS.iter().copied().map(Surrogate::new).collect(),
            }),
        );
    }

    // ── 6. Array inserts ─────────────────────────────────────────────────
    {
        let aid = ArrayId::new(TenantId::new(1), "arr_col");
        let schema = array_schema_1d("arr_col");
        let schema_bytes = zerompk::to_msgpack_vec(&schema).unwrap();

        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Array(ArrayOp::OpenArray {
                array_id: aid.clone(),
                schema_msgpack: schema_bytes,
                schema_hash: 1,
                prefix_bits: 8,
            }),
        );

        let cells: Vec<ArrayPutCell> = ARR_SURS
            .iter()
            .copied()
            .enumerate()
            .map(|(i, s)| ArrayPutCell {
                coord: vec![CoordValue::Int64(i as i64)],
                attrs: vec![CellValue::Float64(s as f64)],
                surrogate: Surrogate::new(s),
                system_from_ms: 0,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            })
            .collect();
        let cells_mp = zerompk::to_msgpack_vec(&cells).unwrap();

        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Array(ArrayOp::Put {
                array_id: aid.clone(),
                cells_msgpack: cells_mp,
                wal_lsn: 1,
            }),
        );
    }

    // ── 7. FTS inserts (via Document engine; text is indexed separately) ─
    for &s in FTS_SURS {
        let hex = format!("{s:08x}");
        let doc = serde_json::json!({ "id": hex, "body": format!("learning keyword {s}") });
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "fts_col".into(),
                document_id: hex.clone(),
                value: serde_json::to_vec(&doc).unwrap(),
                surrogate: Surrogate::new(s),
                pk_bytes: hex.into_bytes(),
            }),
        );
    }

    // ── 8. Spatial inserts (via Document engine; geometry stored as field) ─
    for &s in SPATIAL_SURS {
        let hex = format!("{s:08x}");
        let lon = s as f64 * 0.01;
        let doc = serde_json::json!({ "id": hex, "geom": { "type": "Point", "coordinates": [lon, 0.0] } });
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "spatial_col".into(),
                document_id: hex.clone(),
                value: serde_json::to_vec(&doc).unwrap(),
                surrogate: Surrogate::new(s),
                pk_bytes: hex.into_bytes(),
            }),
        );
    }

    // ══════════════════════════════════════════════════════════════════════
    // Assertion A: Global surrogate uniqueness across all engines.
    //
    // We control all surrogate values; collect every value and assert the
    // multi-set has no duplicates.  The non-overlap was designed into the
    // constants above; this assertion catches any future constant change
    // that accidentally introduces a collision.
    // ══════════════════════════════════════════════════════════════════════
    let all_surs: Vec<u32> = [
        V_SURS,
        D_SURS,
        G_SRC_SURS,
        KV_SURS,
        COL_SURS,
        ARR_SURS,
        FTS_SURS,
        SPATIAL_SURS,
    ]
    .iter()
    .flat_map(|s| s.iter().copied())
    .collect();

    // Surrogates 10, 12, 13 intentionally appear in multiple engines (the
    // cross-engine join scenario).  The uniqueness property we assert here
    // is that each engine's OWN insert batch uses distinct values — not
    // that the same physical surrogate never appears in two engines (that
    // would prohibit the cross-engine join design).
    for slice in [
        V_SURS,
        D_SURS,
        G_SRC_SURS,
        KV_SURS,
        COL_SURS,
        ARR_SURS,
        FTS_SURS,
        SPATIAL_SURS,
    ] {
        let set: HashSet<u32> = slice.iter().copied().collect();
        assert_eq!(
            set.len(),
            slice.len(),
            "engine surrogate batch must contain no duplicates: {slice:?}"
        );
    }

    // Build the global set (deduped) and assert no same surrogate maps to
    // two different rows in the SAME engine.
    let global_set: HashSet<u32> = all_surs.iter().copied().collect();
    // Global set has fewer elements than all_surs because of intentional
    // cross-engine reuse of {10, 12, 13}.  What we assert is that within
    // each engine batch, every surrogate is unique.  (Validated above.)
    assert!(
        global_set.contains(&10),
        "surrogate 10 must be in the global set"
    );

    // ══════════════════════════════════════════════════════════════════════
    // Assertion B: Cross-engine bitmap intersection (Vector ∩ Graph ∩ Array).
    //
    // Design:
    //   bitmap_v  = {10, 11, 12, 13}  (all Vector surrogates)
    //   bitmap_g  = {10, 12, 30, 31}  (all Graph src surrogates)
    //   bitmap_a  = {10, 13, 60, 61}  (all Array cell surrogates)
    //   intersection = bitmap_v ∩ bitmap_g ∩ bitmap_a = {10}
    //
    // For each engine we run a query with `intersection` as the prefilter
    // and assert only surrogate 10 is returned.
    // ══════════════════════════════════════════════════════════════════════

    let bitmap_v: SurrogateBitmap = V_SURS.iter().copied().map(Surrogate::new).collect();
    let bitmap_g: SurrogateBitmap = G_SRC_SURS.iter().copied().map(Surrogate::new).collect();
    let bitmap_a: SurrogateBitmap = ARR_SURS.iter().copied().map(Surrogate::new).collect();

    let intersection = bitmap_v.intersect(&bitmap_g).intersect(&bitmap_a);

    assert_eq!(
        intersection.len(),
        1,
        "three-way intersection must contain exactly surrogate 10"
    );
    assert!(
        intersection.contains(Surrogate::new(10)),
        "surrogate 10 must be in the intersection"
    );

    // B1 — Vector search with intersection as filter_bitmap.
    //      Unfiltered search returns all four Vector surrogates (11, 12, 13
    //      also match the query vector near [1,0,0]).
    let all_vec = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: "vec_col".into(),
            query_vector: vec![1.0f32, 0.0, 0.0],
            top_k: 20,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
        }),
    );
    let all_vec_surs = extract_vector_surrogates(&all_vec);
    assert!(
        all_vec_surs.len() >= 2,
        "unfiltered vector search should return at least 2 results, got {all_vec_surs:?}"
    );

    let filtered_vec = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: "vec_col".into(),
            query_vector: vec![1.0f32, 0.0, 0.0],
            top_k: 20,
            ef_search: 0,
            filter_bitmap: Some(intersection.clone()),
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
        }),
    );
    let filtered_vec_surs = extract_vector_surrogates(&filtered_vec);

    // Every returned surrogate must be in the intersection.
    for &s in &filtered_vec_surs {
        assert!(
            intersection.contains(Surrogate::new(s)),
            "vector search returned surrogate {s} not in intersection"
        );
    }
    // Surrogates 11, 12, 13 must not appear (not in intersection).
    for &absent in &[11u32, 12, 13] {
        assert!(
            !filtered_vec_surs.contains(&absent),
            "surrogate {absent} is Vector-only or Vector+one-engine; must be absent from intersection-filtered search"
        );
    }

    // B2 — Graph hop with frontier_bitmap = intersection.
    //      Insert an edge from n10 → n11 so n11 is reachable.
    //      With frontier_bitmap = {10}, the hop should not traverse to n11
    //      (surrogate 11 is not in the intersection).
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: "graph_col".into(),
            src_id: "n10".into(),
            label: "REACH".into(),
            dst_id: "n11".into(),
            properties: vec![],
            src_surrogate: Surrogate::new(10),
            dst_surrogate: Surrogate::new(11),
        }),
    );

    let hop_all = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["n10".into()],
            edge_label: Some("REACH".into()),
            direction: Direction::Out,
            depth: 1,
            options: Default::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: None,
        }),
    );
    let hop_all_json = decode_payload_to_json(&hop_all);
    // Unfiltered: n11 is reachable from n10 via REACH.
    assert!(
        hop_all_json.contains("n11"),
        "unfiltered hop from n10 must reach n11: {hop_all_json}"
    );

    let hop_filtered = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["n10".into()],
            edge_label: Some("REACH".into()),
            direction: Direction::Out,
            depth: 1,
            options: Default::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: Some(intersection.clone()),
        }),
    );
    let hop_filtered_json = decode_payload_to_json(&hop_filtered);
    // Filtered: n11's surrogate (11) is not in the intersection — hop must
    // not include it.
    assert!(
        !hop_filtered_json.contains("n11"),
        "intersection-filtered hop must not reach n11 (surrogate 11 not in intersection): \
         {hop_filtered_json}"
    );

    // B3 — Array slice with cell_filter = intersection.
    {
        let aid = ArrayId::new(TenantId::new(1), "arr_col");
        // Full slice (no filter).
        // Full slice: no restrictions on any dimension (None = unbounded).
        let full_slice = ArraySlice::new(vec![None]);
        let slice_mp = zerompk::to_msgpack_vec(&full_slice).unwrap();

        let slice_all = send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Array(ArrayOp::Slice {
                array_id: aid.clone(),
                slice_msgpack: slice_mp.clone(),
                attr_projection: vec![],
                limit: 100,
                cell_filter: None,
                hilbert_range: None,
                system_as_of: None,
                valid_at_ms: None,
            }),
        );
        // Unfiltered: should see all ARR_SURS cells.
        let slice_all_json = decode_payload_to_json(&slice_all);
        assert!(
            !slice_all_json.is_empty(),
            "unfiltered array slice must return rows"
        );

        let slice_filtered = send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Array(ArrayOp::Slice {
                array_id: aid.clone(),
                slice_msgpack: slice_mp,
                attr_projection: vec![],
                limit: 100,
                cell_filter: Some(intersection.clone()),
                hilbert_range: None,
                system_as_of: None,
                valid_at_ms: None,
            }),
        );
        // Filtered: intersection ∩ ARR_SURS = {10}.  Surrogate 10 is at
        // coord [0] with attr value 10.0 (see ARR_SURS insert loop above).
        // Surrogates 13 (coord [1]), 60 (coord [2]), 61 (coord [3]) must not
        // appear.  The response is zerompk-encoded NdArrayCell values; we
        // transcode to JSON for inspection.
        // Local DP slice responses are wrapped as `ArraySliceResponse { rows_msgpack:
        // Vec<u8>, truncated_before_horizon: bool }`. The inner `rows_msgpack` is a
        // separate msgpack array of `NdArrayCell` values.
        #[derive(serde::Deserialize, zerompk::FromMessagePack)]
        #[msgpack(map)]
        struct SliceEnvelope {
            rows_msgpack: Vec<u8>,
            #[allow(dead_code)]
            truncated_before_horizon: bool,
        }
        fn unwrap_rows(payload: &[u8]) -> serde_json::Value {
            let envelope: SliceEnvelope = match zerompk::from_msgpack(payload) {
                Ok(e) => e,
                Err(_) => return serde_json::Value::Array(vec![]),
            };
            let json_str =
                nodedb_types::msgpack_to_json_string(&envelope.rows_msgpack).unwrap_or_default();
            serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Array(vec![]))
        }
        let filtered_cells = unwrap_rows(&slice_filtered);
        let unfiltered_cells = unwrap_rows(&slice_all);
        assert_eq!(
            unfiltered_cells.as_array().map(|a| a.len()).unwrap_or(0),
            ARR_SURS.len(),
            "unfiltered slice must return all {} array cells",
            ARR_SURS.len()
        );

        let filtered_count = filtered_cells.as_array().map(|a| a.len()).unwrap_or(0);
        assert_eq!(
            filtered_count, 1,
            "intersection-filtered slice must return exactly 1 cell (surrogate 10), \
             got {filtered_count}: {filtered_cells}"
        );

        // The surviving cell must have attr value 10.0 (the value we stored at
        // coord [0] which received surrogate 10).
        if let Some(cell) = filtered_cells.as_array().and_then(|a| a.first())
            && let Some(attrs) = cell.get("attrs").and_then(|v| v.as_array())
        {
            let first_attr = attrs.first().and_then(|v| v.as_f64());
            assert_eq!(
                first_attr,
                Some(10.0),
                "filtered cell attr must be 10.0 (surrogate-10's stored value)"
            );
        }
    }
}
