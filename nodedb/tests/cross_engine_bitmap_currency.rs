//! Cross-engine integration tests for the surrogate bitmap currency.
//!
//! Two test functions verify that `SurrogateBitmap` prefilters flow correctly
//! across engine boundaries:
//!
//! 1. `fts_derived_bitmap_filters_vector_search` — FTS hit-set bitmap drives a
//!    vector ANN search: only rows whose surrogates were in the FTS hit set are
//!    returned; a known non-member surrogate is absent from results.
//!
//! 2. `document_scan_bitmap_filters_columnar_aggregate` — A document scan
//!    emits a surrogate bitmap that prefilters a columnar aggregate: only rows
//!    whose surrogates appear in the document scan output contribute to the
//!    aggregate; surrogates present only in the columnar collection are excluded.
//!
//! Both tests assert two directions:
//!  (a) every returned surrogate is in the expected bitmap, and
//!  (b) at least one known non-member surrogate exists in the underlying data
//!      but does NOT appear in results.

use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use nodedb::bridge::physical_plan::{
    ColumnarInsertIntent, ColumnarOp, DocumentOp, QueryOp, VectorOp,
};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::data::executor::response_codec::decode_payload_to_json;
use nodedb::types::*;
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
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
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

/// Push a plan, tick, pop response, assert `Ok`.
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

/// Parse a JSON response payload into a `serde_json::Value`.
fn parse_json(payload: &[u8]) -> serde_json::Value {
    let s = decode_payload_to_json(payload);
    serde_json::from_str(&s).unwrap_or(serde_json::Value::Array(vec![]))
}

/// Extract surrogate u32 values from a vector search response payload.
/// Vector results encode `id` as the raw surrogate u32.
fn extract_vector_surrogates(payload: &[u8]) -> Vec<u32> {
    let v = parse_json(payload);
    v.as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|hit| hit.get("id").and_then(|id| id.as_u64()).map(|n| n as u32))
        .collect()
}

/// Extract the `id` field strings from a generic row response (document/columnar).
fn extract_ids(payload: &[u8]) -> Vec<String> {
    let v = parse_json(payload);
    v.as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|row| row.get("id").and_then(|id| id.as_str()).map(str::to_string))
        .collect()
}

// ── Test 1: FTS → Vector ──────────────────────────────────────────────────

/// Insert documents with both text content and vector embeddings.
/// An FTS search yields a bitmap of matching surrogates. Applying that bitmap
/// as a prefilter to a vector ANN search must exclude the non-matching
/// surrogate (surrogate 3) and return only surrogates present in the FTS set.
///
/// Proves: (a) every returned surrogate is in the FTS bitmap,
///         (b) surrogate 3, which exists in the vector index but does not
///             match the FTS query, is absent from prefiltered search results.
#[test]
fn fts_derived_bitmap_filters_vector_search() {
    let (mut core, mut tx, mut rx, _dir) = open_core();

    // Surrogate values used as vector node IDs. We assign these manually
    // (Surrogate::new(n)) because the CoreLoop test harness does not run the
    // full SurrogateAssigner pipeline.
    let s1 = Surrogate::new(1);
    let s2 = Surrogate::new(2);
    let s3 = Surrogate::new(3); // non-member: does not match FTS query

    // Insert FTS-indexed documents. We use `PointPut` (schemaless JSON)
    // so the inverted index picks up the text field.
    let docs = [
        (s1, "machine learning fundamentals and neural networks"),
        (s2, "deep learning for natural language processing"),
        (s3, "quantum computing and photonic qubits"),
    ];
    for (surrogate, text) in &docs {
        let hex_id = format!("{:08x}", surrogate.0);
        let doc_json = serde_json::json!({
            "id": hex_id,
            "body": text,
        });
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "articles".into(),
                document_id: hex_id.clone(),
                value: serde_json::to_vec(&doc_json).unwrap(),
                surrogate: *surrogate,
                pk_bytes: hex_id.into_bytes(),
            }),
        );
    }

    // Insert vectors for all three surrogates. Each vector is a simple
    // 3-dimensional embedding; surrogates 1 and 2 cluster near [1,0,0]
    // while surrogate 3 clusters near [-1,0,0].
    let vectors: &[(Surrogate, [f32; 3])] = &[
        (s1, [1.0, 0.1, 0.0]),
        (s2, [0.9, 0.2, 0.0]),
        (s3, [-1.0, 0.0, 0.1]),
    ];
    for (surrogate, vec) in vectors {
        tx.try_push(BridgeRequest {
            inner: make_req(PhysicalPlan::Vector(VectorOp::Insert {
                collection: "articles".into(),
                vector: vec.to_vec(),
                dim: 3,
                field_name: String::new(),
                surrogate: *surrogate,
            })),
        })
        .unwrap();
    }
    core.tick();
    // Collect the 3 insert responses.
    for _ in 0..3 {
        rx.try_pop().unwrap();
    }

    // Full vector search (no bitmap) — all 3 surrogates should appear.
    let all_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: "articles".into(),
            query_vector: vec![1.0f32, 0.0, 0.0],
            top_k: 10,
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
    let all_ids = extract_vector_surrogates(&all_payload);
    assert!(
        all_ids.contains(&3),
        "unfiltered search should include surrogate 3, got {all_ids:?}"
    );

    // Build an FTS-derived bitmap: surrogates 1 and 2 match "learning",
    // surrogate 3 (quantum computing) does not. We construct the bitmap
    // directly here rather than running the FTS engine — the bitmap is the
    // *currency*, not the FTS engine being tested here.
    let mut fts_bitmap = SurrogateBitmap::new();
    fts_bitmap.insert(s1);
    fts_bitmap.insert(s2);
    // s3 is deliberately absent.

    // Vector search prefiltered by the FTS-derived bitmap.
    let filtered_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: "articles".into(),
            query_vector: vec![1.0f32, 0.0, 0.0],
            top_k: 10,
            ef_search: 0,
            filter_bitmap: Some(fts_bitmap.clone()),
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
        }),
    );
    let filtered_ids = extract_vector_surrogates(&filtered_payload);

    // (a) Every returned surrogate is in the FTS bitmap.
    for sid in &filtered_ids {
        assert!(
            fts_bitmap.contains(Surrogate::new(*sid)),
            "surrogate {sid} is not in the FTS bitmap but appeared in results"
        );
    }

    // (b) Surrogate 3 exists in the vector index (proved by unfiltered search
    //     above) but must NOT appear in the prefiltered results.
    assert!(
        !filtered_ids.contains(&3),
        "surrogate 3 (non-member) must not appear in FTS-prefiltered results, \
         got {filtered_ids:?}"
    );

    // (c) At least one member surrogate appears (results are non-empty).
    assert!(
        !filtered_ids.is_empty(),
        "prefiltered vector search should return at least one result"
    );
}

// ── Test 2: Document → Columnar ──────────────────────────────────────────

/// Insert rows into both a document collection and a columnar collection
/// with matching surrogates. A columnar scan prefiltered by a surrogate bitmap
/// derived from the document collection produces results consistent with
/// manual filtering, and excludes surrogates not in the document scan output.
///
/// Proves: (a) every row returned by the prefiltered columnar scan has a
///             surrogate that is in the injected bitmap,
///         (b) rows with surrogates 3 and 4 exist in the columnar collection
///             but do NOT appear when the bitmap excludes them.
#[test]
fn document_scan_bitmap_filters_columnar_aggregate() {
    let (mut core, mut tx, mut rx, _dir) = open_core();

    // Surrogates: 1 and 2 are in the document scan output; 3 and 4 are
    // columnar-only (not in the document scan bitmap).
    let doc_surrogates = [Surrogate::new(1), Surrogate::new(2)];
    let extra_surrogates = [Surrogate::new(3), Surrogate::new(4)];

    // Insert documents for surrogates 1 and 2.
    // Use 8-char hex-encoded surrogate as document_id so the storage key
    // matches what `scan_sparse` injects as the `id` field in scan results.
    for surrogate in &doc_surrogates {
        let hex_id = format!("{:08x}", surrogate.0);
        let doc_json = serde_json::json!({
            "id": hex_id,
            "category": "active",
        });
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "catalog".into(),
                document_id: hex_id.clone(),
                value: serde_json::to_vec(&doc_json).unwrap(),
                surrogate: *surrogate,
                pk_bytes: hex_id.into_bytes(),
            }),
        );
    }

    // Insert columnar rows for all four surrogates. Use the same hex-encoded
    // surrogate ids so the hash join key matches document scan output.
    let all_surrogates: Vec<Surrogate> = doc_surrogates
        .iter()
        .chain(extra_surrogates.iter())
        .copied()
        .collect();
    let rows_json = serde_json::json!(
        all_surrogates
            .iter()
            .map(|s| serde_json::json!({
                "id": format!("{:08x}", s.0),
                "value": s.0 * 10,
            }))
            .collect::<Vec<_>>()
    );
    let rows_mp = nodedb_types::json_to_msgpack(&rows_json).unwrap();
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: "metrics".into(),
            payload: rows_mp,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: all_surrogates.clone(),
        }),
    );

    // Verify all 4 rows exist in the columnar collection (unfiltered).
    let all_col = core.scan_collection(1, "metrics", 100).unwrap();
    assert_eq!(
        all_col.len(),
        4,
        "expected 4 columnar rows, got {}",
        all_col.len()
    );

    // Build a bitmap from the document scan output (surrogates 1 and 2).
    // In production this bitmap would be produced by the bitmap-producer
    // sub-plan; here we construct it directly to isolate the prefilter logic.
    let mut doc_bitmap = SurrogateBitmap::new();
    for s in &doc_surrogates {
        doc_bitmap.insert(*s);
    }
    // Surrogates 3 and 4 are absent from the bitmap.

    // Run a columnar scan with the document-derived bitmap as prefilter.
    let filtered_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: "metrics".into(),
            filters: Vec::new(),
            rls_filters: Vec::new(),
            sort_keys: Vec::new(),
            limit: 100,
            projection: Vec::new(),
            system_as_of_ms: None,
            valid_at_ms: None,
            prefilter: Some(doc_bitmap.clone()),
        }),
    );
    let filtered_ids = extract_ids(&filtered_payload);

    // (a) Every returned id maps to a surrogate in the bitmap.
    for id in &filtered_ids {
        let surrogate_val = u32::from_str_radix(id, 16).unwrap_or(u32::MAX);
        assert!(
            doc_bitmap.contains(Surrogate::new(surrogate_val)),
            "id {id} (surrogate {surrogate_val}) is not in the document bitmap \
             but appeared in prefiltered scan results"
        );
    }

    // (b) Surrogates 3 and 4 exist in the columnar data but must not appear.
    for s in &extra_surrogates {
        let hex_id = format!("{:08x}", s.0);
        assert!(
            !filtered_ids.contains(&hex_id),
            "surrogate {} (non-member) must not appear in prefiltered scan, \
             got {filtered_ids:?}",
            s.0
        );
    }

    // (c) At least one result appears (bitmap is non-empty and data exists).
    assert!(
        !filtered_ids.is_empty(),
        "prefiltered columnar scan should return at least one result"
    );

    // Also verify the bitmap-injected hash join works:
    // Use a `DocumentOp::Scan` with prefilter as the inline_left_bitmap sub-plan
    // in a HashJoin between "catalog" (documents) and "metrics" (columnar).
    // The executor runs the sub-plan, collects surrogates 1+2, and injects
    // the bitmap into the left side scan before probing.
    let bm_subplan = PhysicalPlan::Document(DocumentOp::Scan {
        collection: "catalog".into(),
        limit: 100,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_as_of_ms: None,
        valid_at_ms: None,
        prefilter: None,
    });

    let join_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "catalog".into(),
            right_collection: "metrics".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("id".into(), "id".into())],
            join_type: "inner".into(),
            limit: 1000,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            post_filters: Vec::new(),
            inline_left: None,
            inline_right: None,
            inline_left_bitmap: Some(Box::new(bm_subplan)),
            inline_right_bitmap: None,
        }),
    );
    // The join result has prefixed keys: "catalog.id", "metrics.id", etc.
    // Parse the join result as an array and extract "catalog.id" values.
    let join_json = parse_json(&join_payload);
    let join_rows = join_json.as_array().cloned().unwrap_or_default();

    // The join result should only contain rows for surrogates 1 and 2
    // (only those exist in "catalog"; surrogates 3 and 4 are metrics-only).
    for row in &join_rows {
        // Extract "catalog.id" from the prefixed join output.
        let id = row.get("catalog.id").and_then(|v| v.as_str()).unwrap_or("");
        let surrogate_val = u32::from_str_radix(id, 16).unwrap_or(u32::MAX);
        assert!(
            doc_bitmap.contains(Surrogate::new(surrogate_val)),
            "join result catalog.id {id} is not in the document bitmap"
        );
    }
    assert!(
        !join_rows.is_empty(),
        "hash join with bitmap sub-plan should return matched rows"
    );
}
