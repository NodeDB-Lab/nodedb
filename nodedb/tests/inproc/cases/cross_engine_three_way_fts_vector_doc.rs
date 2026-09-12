// SPDX-License-Identifier: BUSL-1.1

//! Cross-engine three-way bitmap test: FTS → Vector → Document.
//!
//! Scenario:
//!   - 10 rows inserted into a single document collection.
//!   - Rows 1–7 have a `body` field containing the word "learning";
//!     rows 8–10 do not.
//!   - Each row also has a vector embedding in the `emb` field.
//!
//! Queries:
//!   Q1 — FTS search for "learning" → `SurrogateBitmap` A.
//!        Assert A == {1..7}.
//!
//!   Q2 — Vector ANN search prefiltered by A.
//!        Assert results are a subset of {1..7}; rows 8–10 absent.
//!
//!   Q3 — Document scan prefiltered by A.
//!        Assert results == {1..7}.
//!
//!   Q4 — Bitmap intersection of A with the result surrogates from Q3.
//!        Assert the intersection == {1..7}.
//!
//! Uses the same CoreLoop ring-buffer harness as `cross_engine_bitmap_currency.rs`
//! and `surrogate_round_trip.rs`. No server or network required.

use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{Priority, Request, Status};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::data::executor::response_codec::decode_payload_to_json;
use nodedb::types::*;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, TextOp, VectorOp};
use nodedb_types::vector_distance::DistanceMetric;
use nodedb_types::{Surrogate, SurrogateBitmap};

// ── Harness (mirrors cross_engine_bitmap_currency.rs) ────────────────────

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
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

fn make_req(plan: PhysicalPlan) -> Request {
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

// ── Helper: parse a JSON response payload ────────────────────────────────

fn parse_json(payload: &[u8]) -> serde_json::Value {
    let s = decode_payload_to_json(payload);
    serde_json::from_str(&s).unwrap_or(serde_json::Value::Array(vec![]))
}

/// Extract surrogate u32 values from a vector search response.
/// A bound vector hit encodes `id` as its storage key.
fn extract_vector_surrogates(payload: &[u8]) -> Vec<u32> {
    parse_json(payload)
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|h| h.get("id").and_then(|v| v.as_str()))
        .filter_map(|key| nodedb_types::StorageKey::parse(key).map(|k| k.surrogate().as_u32()))
        .collect()
}

/// Extract surrogate u32 values from a `TextOp::Search` response.
/// FTS hits share the document-scan envelope `{id, data}`; `id` is the
/// 8-char hex surrogate produced by `surrogate_to_doc_id`.
fn extract_fts_surrogates(payload: &[u8]) -> Vec<u32> {
    parse_json(payload)
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|h| {
            h.get("id")
                .and_then(|v| v.as_str())
                .and_then(|hex| u32::from_str_radix(hex, 16).ok())
        })
        .collect()
}

/// Extract the `id` field (hex surrogate string) from a document scan response
/// and parse it back to u32.
fn extract_doc_surrogates(payload: &[u8]) -> Vec<u32> {
    parse_json(payload)
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|h| {
            // Document scan rows have shape {"id": "<8-char hex>", "data": {...}}
            h.get("id")
                .and_then(|v| v.as_str())
                .and_then(|hex| u32::from_str_radix(hex, 16).ok())
        })
        .collect()
}

// ── Constants ─────────────────────────────────────────────────────────────

/// Surrogates 1–7 have "learning" in their body text.
const LEARNING_SURS: &[u32] = &[1, 2, 3, 4, 5, 6, 7];
/// Surrogates 8–10 do NOT have "learning" in their body text.
const NON_LEARNING_SURS: &[u32] = &[8, 9, 10];
/// Collection name shared across all engine layers.
const COLLECTION: &str = "articles_3way";

// ── Test ──────────────────────────────────────────────────────────────────

#[test]
fn three_way_fts_vector_doc_bitmap() {
    let (mut core, mut tx, mut rx, _dir) = open_core();

    // ── Insert rows 1–10 ──────────────────────────────────────────────────
    //
    // Each row is inserted via `DocumentOp::PointPut` so the inverted index
    // picks up the `body` text field for FTS. Rows 1–7 contain "learning";
    // rows 8–10 contain only "quantum computing" (no match for the query).
    //
    // A 3-D vector embedding is stored per row so the vector engine can
    // participate in the cross-engine prefilter test.

    for &s in LEARNING_SURS {
        let hex = format!("{s:08x}");
        let doc = serde_json::json!({
            "id": hex,
            "body": format!("machine learning fundamentals row {s}"),
            "emb": [1.0f64, s as f64 * 0.01, 0.0],
        });
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    COLLECTION,
                ),
                document_id: hex.clone(),
                value: serde_json::to_vec(&doc).unwrap(),
                surrogate: Surrogate::new(s),
                pk_bytes: hex.into_bytes(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }

    for &s in NON_LEARNING_SURS {
        let hex = format!("{s:08x}");
        let doc = serde_json::json!({
            "id": hex,
            "body": format!("quantum computing photonic qubits row {s}"),
            "emb": [-1.0f64, s as f64 * 0.01, 0.0],
        });
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    COLLECTION,
                ),
                document_id: hex.clone(),
                value: serde_json::to_vec(&doc).unwrap(),
                surrogate: Surrogate::new(s),
                pk_bytes: hex.into_bytes(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }

    // Insert vector embeddings for all 10 rows.
    for &s in LEARNING_SURS.iter().chain(NON_LEARNING_SURS) {
        tx.try_push(BridgeRequest {
            inner: make_req(PhysicalPlan::Vector(VectorOp::Insert {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    COLLECTION,
                ),
                vector: if s <= 7 {
                    vec![1.0f32, s as f32 * 0.01, 0.0]
                } else {
                    vec![-1.0f32, s as f32 * 0.01, 0.0]
                },
                dim: 3,
                field_name: String::new(),
                surrogate: Surrogate::new(s),
                pk_bytes: None,
                provenance: None,
            })),
        })
        .unwrap();
    }
    core.tick();
    // Drain 10 vector insert responses.
    for _ in 0..10 {
        let resp = rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);
    }

    // ── Q1: FTS search for "learning" → bitmap A ─────────────────────────

    let fts_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Text(TextOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                COLLECTION,
            ),
            query: "learning".into(),
            top_k: 20,
            fuzzy: false,
            prefilter: None,
            rls_filters: Vec::new(),
        }),
    );

    let fts_surs = extract_fts_surrogates(&fts_payload);

    // Build bitmap A from FTS results.
    let mut bitmap_a = SurrogateBitmap::new();
    for &s in &fts_surs {
        bitmap_a.insert(Surrogate::new(s));
    }

    // Assert A == {1..7}: every learning surrogate present, no non-learning ones.
    for &s in LEARNING_SURS {
        assert!(
            bitmap_a.contains(Surrogate::new(s)),
            "FTS bitmap must contain surrogate {s} (row {s} has 'learning')"
        );
    }
    for &s in NON_LEARNING_SURS {
        assert!(
            !bitmap_a.contains(Surrogate::new(s)),
            "FTS bitmap must NOT contain surrogate {s} (row {s} has no 'learning')"
        );
    }
    assert_eq!(
        bitmap_a.len(),
        LEARNING_SURS.len() as u64,
        "FTS bitmap must contain exactly {} surrogates, got {}",
        LEARNING_SURS.len(),
        bitmap_a.len()
    );

    // ── Q2: Vector ANN search prefiltered by A ────────────────────────────
    //
    // The query vector [1,0,0] is close to learning-row vectors.
    // Without prefilter, rows 8–10 (near [-1,0,0]) may or may not appear.
    // With prefilter A, rows 8–10 must be absent.

    let vec_filtered = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                COLLECTION,
            ),
            query_vector: vec![1.0f32, 0.0, 0.0],
            top_k: 20,
            ef_search: 0,
            filter_bitmap: Some(bitmap_a.clone()),
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            metric: DistanceMetric::L2,
        }),
    );
    let vec_surs = extract_vector_surrogates(&vec_filtered);

    // Every vector result must be in bitmap A.
    for &s in &vec_surs {
        assert!(
            bitmap_a.contains(Surrogate::new(s)),
            "vector result surrogate {s} is not in the FTS bitmap"
        );
    }
    // Rows 8–10 must be absent.
    for &s in NON_LEARNING_SURS {
        assert!(
            !vec_surs.contains(&s),
            "non-learning surrogate {s} must not appear in FTS-prefiltered vector search"
        );
    }
    assert!(
        !vec_surs.is_empty(),
        "prefiltered vector search must return at least one result"
    );

    // ── Q3: Document scan prefiltered by A ───────────────────────────────

    let doc_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                COLLECTION,
            ),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            filters: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: Some(bitmap_a.clone()),
        }),
    );
    let doc_surs = extract_doc_surrogates(&doc_payload);

    // Document scan must return exactly {1..7}.
    let mut doc_sur_set: std::collections::HashSet<u32> = doc_surs.iter().copied().collect();
    for &s in LEARNING_SURS {
        assert!(
            doc_sur_set.remove(&s),
            "document scan must include surrogate {s} (in FTS bitmap)"
        );
    }
    assert!(
        doc_sur_set.is_empty(),
        "document scan returned unexpected surrogates not in FTS bitmap: {doc_sur_set:?}"
    );

    // ── Q4: Bitmap intersection of A with doc scan results ────────────────

    let mut result_bitmap = SurrogateBitmap::new();
    for &s in &doc_surs {
        result_bitmap.insert(Surrogate::new(s));
    }
    let intersection = bitmap_a.intersect(&result_bitmap);

    assert_eq!(
        intersection.len(),
        LEARNING_SURS.len() as u64,
        "intersection of FTS bitmap and doc scan results must equal {{1..7}}"
    );
    for &s in LEARNING_SURS {
        assert!(
            intersection.contains(Surrogate::new(s)),
            "intersection must contain surrogate {s}"
        );
    }
}
