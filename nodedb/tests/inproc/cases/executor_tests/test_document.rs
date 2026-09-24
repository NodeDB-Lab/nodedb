// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for document operations (PointGet/Put/Delete, RangeScan, CRDT).

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb_bridge::buffer::{Consumer, Producer};
use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, PhysicalPlan};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::data::executor::core_loop::CoreLoop;

use super::helpers::*;

/// Build a `DocumentOp::Scan` over `collection` with an optional row limit.
/// `limit == usize::MAX` models a no-LIMIT `SELECT * FROM collection`.
fn doc_scan_plan(collection: &str, limit: usize) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            collection,
        ),
        limit,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    })
}

/// Push one scan request, tick once, then drain every chunk response,
/// returning `(total_row_count, terminal_status, terminal_error_code)`.
///
/// A document scan whose result exceeds `stream_chunk_size` streams as
/// multiple `Partial` responses followed by a terminal `Ok` (or a terminal
/// `Error` when a budget is exceeded mid-flight). The terminal frame is the
/// one whose `status` is not `Partial`.
fn run_scan_and_count(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> (usize, Status, Option<ErrorCode>) {
    tx.try_push(BridgeRequest::unfloored(make_request(plan)))
        .unwrap();
    core.tick();

    let mut total = 0usize;
    let mut terminal_status = Status::Ok;
    let mut terminal_error = None;
    while let Ok(resp) = rx.try_pop() {
        let r = resp.inner;
        if !r.payload.is_empty() {
            let json = payload_json(&r.payload);
            if let Ok(serde_json::Value::Array(rows)) =
                serde_json::from_str::<serde_json::Value>(&json)
            {
                total += rows.len();
            }
        }
        if r.status != Status::Partial {
            terminal_status = r.status;
            terminal_error = r.error_code.map(|b| *b);
        }
    }
    (total, terminal_status, terminal_error)
}

#[test]
fn point_get_not_found() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            ),
            document_id: "nonexistent".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok);
    assert!(resp.payload.is_empty());
    assert_eq!(resp.error_code, None);
}

#[test]
fn point_put_and_get() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".into(),
            value: b"hello world".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok);
    assert_eq!(&*resp.payload, b"hello world");
}

#[test]
fn point_delete_removes() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert then delete via SPSC.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".into(),
            value: b"data".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        }),
    );

    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok);
    assert!(resp.payload.is_empty());
    assert_eq!(resp.error_code, None);
}

#[test]
fn crdt_read_not_found() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Crdt(CrdtOp::Read {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            document_id: "s1".into(),
        }),
    );
    assert_eq!(resp.status, Status::Error);
    assert_eq!(resp.error_code.as_deref(), Some(&ErrorCode::NotFound));
}

#[test]
fn range_scan_returns_results() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert documents with indexed fields via PointPut.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            ),
            document_id: "u1".into(),
            value: b"{\"name\":\"alice\",\"age\":25}".to_vec(),
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: b"u1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            ),
            document_id: "u2".into(),
            value: b"{\"name\":\"bob\",\"age\":30}".to_vec(),
            surrogate: nodedb_types::Surrogate::new(2),
            pk_bytes: b"u2".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    // DocumentScan should return both.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            ),
            limit: 10,
            offset: 0,
            sort_keys: Vec::new(),
            filters: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        }),
    );
    let json = payload_json(&payload);
    assert!(json.contains("alice"), "payload: {json}");
    assert!(json.contains("bob"), "payload: {json}");
}

/// Insert `count` small documents into `collection` in a single batch.
fn batch_insert_docs(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    collection: &str,
    count: usize,
) {
    let documents: Vec<(String, Vec<u8>)> = (0..count)
        .map(|i| {
            let id = format!("d{i}");
            let value = format!("{{\"i\":{i}}}").into_bytes();
            (id, value)
        })
        .collect();
    let surrogates: Vec<nodedb_types::Surrogate> = (0..count)
        .map(|i| nodedb_types::Surrogate::new((i as u32) + 1))
        .collect();

    send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            documents,
            surrogates,
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        }),
    );
}

/// A no-LIMIT scan over a collection with more than the historical 10k
/// truncation point returns EVERY row (not silently capped at 10 000) when the
/// result fits the scan memory budget.
#[test]
fn unbounded_scan_returns_all_rows_when_within_budget() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let count = 12_000;
    batch_insert_docs(&mut core, &mut tx, &mut rx, "wide", count);

    // usize::MAX models a no-LIMIT `SELECT * FROM wide`.
    let (rows, status, err) = run_scan_and_count(
        &mut core,
        &mut tx,
        &mut rx,
        doc_scan_plan("wide", usize::MAX),
    );

    assert_eq!(status, Status::Ok, "unexpected error: {err:?}");
    assert_eq!(
        rows, count,
        "no-LIMIT scan must return all {count} rows, not the old 10k cap"
    );
    assert!(
        rows > 10_000,
        "regression: result was truncated at/under the old 10k default"
    );
}

/// A no-LIMIT scan whose materialized result exceeds the scan memory budget
/// surfaces a deterministic `ResourcesExhausted` error — it never silently
/// returns a partial result.
#[test]
fn unbounded_scan_over_budget_surfaces_error() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tiny per-query scan budget so a modest collection trips it.
    let tuning = nodedb_types::config::tuning::QueryTuning {
        max_scan_result_bytes: 256, // bytes
        ..nodedb_types::config::tuning::QueryTuning::default()
    };
    core.set_query_tuning(tuning);

    batch_insert_docs(&mut core, &mut tx, &mut rx, "big", 5_000);

    let (rows, status, err) = run_scan_and_count(
        &mut core,
        &mut tx,
        &mut rx,
        doc_scan_plan("big", usize::MAX),
    );

    assert_eq!(
        status,
        Status::Error,
        "over-budget scan must surface an error, got {rows} rows"
    );
    assert_eq!(
        err,
        Some(ErrorCode::ResourcesExhausted),
        "must surface the deterministic resource-exhausted error"
    );
}

/// An explicit `LIMIT n` still returns exactly `n` rows — the budget bound only
/// applies to unbounded scans and must not change explicit-limit behaviour.
#[test]
fn explicit_limit_returns_exactly_n() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    batch_insert_docs(&mut core, &mut tx, &mut rx, "limited", 12_000);

    let (rows, status, err) =
        run_scan_and_count(&mut core, &mut tx, &mut rx, doc_scan_plan("limited", 250));

    assert_eq!(status, Status::Ok, "unexpected error: {err:?}");
    assert_eq!(rows, 250, "explicit LIMIT 250 must return exactly 250 rows");
}
