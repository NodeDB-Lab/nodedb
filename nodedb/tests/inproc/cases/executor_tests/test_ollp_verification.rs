// SPDX-License-Identifier: BUSL-1.1

//! Executor-level OLLP surrogate verification tests.
//!
//! These tests validate the optimistic lock-based predicate (OLLP) verification
//! path in `execute_bulk_update` and `execute_bulk_delete`. The executor compares
//! the `ollp_predicted_surrogates` embedded in the plan against the set of
//! document surrogates that actually match the predicate at admission time.
//!
//! Scenarios covered:
//!
//! 1. **No OLLP** (`ollp_predicted_surrogates: None`): the existing behaviour is
//!    preserved — bulk update and delete proceed without any surrogate check.
//!
//! 2. **Correct prediction**: `ollp_predicted_surrogates` matches the actual
//!    matching set — the write proceeds and returns `Ok`.
//!
//! 3. **Stale prediction (race)**: a document was inserted between the pre-exec
//!    scan and admission, so the predicted set is smaller than the actual set.
//!    The executor returns `ErrorCode::OllpRetryRequired` WITHOUT writing.
//!
//! 4. **Retry with corrected prediction**: after receiving `OllpRetryRequired`,
//!    the caller re-scans and re-submits with the corrected surrogate set.
//!    The executor accepts and writes.
//!
//! The "race" is simulated synchronously: insert a document after recording the
//! predicted surrogates but before the bulk operation. The executor sees the
//! mismatch because it scans live storage at admission time.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb::bridge::scan_filter::ScanFilter;
use nodedb_physical::physical_plan::{DocumentOp, OllpPredictedEdge, PhysicalPlan, UpdateValue};

use super::helpers::*;

// ── helpers ────────────────────────────────────────────────────────────────

const COLLECTION: &str = "ollp_items";

fn filter_active() -> Vec<u8> {
    let f = ScanFilter {
        field: "active".into(),
        op: "eq".into(),
        value: nodedb_types::Value::Bool(true),
        clauses: Vec::new(),
        expr: None,
    };
    zerompk::to_msgpack_vec(&vec![f]).unwrap()
}

/// Deterministic surrogate for a string ID — same formula as other test files.
fn surrogate_for(id: &str) -> nodedb_types::Surrogate {
    let mut h: u32 = 2_166_136_261;
    for &b in id.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(16_777_619);
    }
    nodedb_types::Surrogate::new(h.max(1))
}

/// The u32 value of a surrogate — what the OLLP comparison operates on.
fn surrogate_u32(id: &str) -> u32 {
    surrogate_for(id).as_u32()
}

/// Insert a document with `active: true`.
fn insert_active(ctx: &mut TestCtx, id: &str) {
    let value = format!(r#"{{"active":true,"name":"{id}"}}"#);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                COLLECTION,
            ),
            document_id: id.into(),
            value: value.into_bytes(),
            surrogate: surrogate_for(id),
            pk_bytes: id.as_bytes().to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
}

/// Insert an edge document with `active: true` plus `_from`/`_to` (and an
/// optional `_type`). Such a doc both matches `filter_active()` AND is an
/// implicit graph edge, so it participates in edge-content drift validation.
fn insert_active_edge(ctx: &mut TestCtx, id: &str, from: &str, to: &str, etype: Option<&str>) {
    let value = match etype {
        Some(t) => {
            format!(
                r#"{{"active":true,"name":"{id}","_from":"{from}","_to":"{to}","_type":"{t}"}}"#
            )
        }
        None => format!(r#"{{"active":true,"name":"{id}","_from":"{from}","_to":"{to}"}}"#),
    };
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                COLLECTION,
            ),
            document_id: id.into(),
            value: value.into_bytes(),
            surrogate: surrogate_for(id),
            pk_bytes: id.as_bytes().to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
}

/// Build a predicted edge from an id + endpoints, keyed on the same surrogate
/// the data plane recomputes from the stored doc.
fn predicted_edge(id: &str, from: &str, to: &str, label: Option<&str>) -> OllpPredictedEdge {
    OllpPredictedEdge {
        surrogate: surrogate_u32(id),
        from: from.to_string(),
        to: to.to_string(),
        label: label.map(str::to_string),
    }
}

/// Build a BulkUpdate plan that sets `name = "updated"` for all `active = true` docs.
fn bulk_update_plan(predicted: Option<Vec<u32>>) -> PhysicalPlan {
    let updates = vec![(
        "name".to_string(),
        UpdateValue::Literal(nodedb_types::json_to_msgpack(&serde_json::json!("updated")).unwrap()),
    )];
    PhysicalPlan::Document(DocumentOp::BulkUpdate {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            COLLECTION,
        ),
        filters: filter_active(),
        updates,
        returning: None,
        ollp_predicted_surrogates: predicted,
        ollp_predicted_edges: None,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
    })
}

/// Build a BulkDelete plan that deletes all `active = true` docs.
fn bulk_delete_plan(predicted: Option<Vec<u32>>) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::BulkDelete {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            COLLECTION,
        ),
        filters: filter_active(),
        returning: None,
        ollp_predicted_surrogates: predicted,
        ollp_predicted_edges: None,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
    })
}

/// Build a BulkDelete plan carrying BOTH a predicted surrogate set and a
/// predicted edge-content set. Used by the edge-content drift tests.
fn bulk_delete_plan_with_edges(
    predicted: Option<Vec<u32>>,
    edges: Option<Vec<OllpPredictedEdge>>,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::BulkDelete {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            COLLECTION,
        ),
        filters: filter_active(),
        returning: None,
        ollp_predicted_surrogates: predicted,
        ollp_predicted_edges: edges,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
    })
}

// ── tests ──────────────────────────────────────────────────────────────────

/// BulkUpdate without OLLP (`predicted = None`) continues to work normally.
#[test]
fn bulk_update_no_ollp_proceeds() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "a");
    insert_active(&mut ctx, "b");

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(None),
    );
    assert_eq!(resp.status, Status::Ok, "no-OLLP BulkUpdate should succeed");
}

/// BulkDelete without OLLP continues to work normally.
#[test]
fn bulk_delete_no_ollp_proceeds() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "a");
    insert_active(&mut ctx, "b");

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan(None),
    );
    assert_eq!(resp.status, Status::Ok, "no-OLLP BulkDelete should succeed");
}

/// BulkUpdate with a correct prediction succeeds.
#[test]
fn bulk_update_correct_prediction_succeeds() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "x1");
    insert_active(&mut ctx, "x2");

    // Pre-exec scan would have returned exactly these two surrogates.
    let predicted = vec![surrogate_u32("x1"), surrogate_u32("x2")];
    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(Some(predicted)),
    );
    assert_eq!(
        resp.status,
        Status::Ok,
        "BulkUpdate with correct prediction should succeed"
    );
}

/// BulkDelete with a correct prediction succeeds.
#[test]
fn bulk_delete_correct_prediction_succeeds() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "y1");
    insert_active(&mut ctx, "y2");

    let predicted = vec![surrogate_u32("y1"), surrogate_u32("y2")];
    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan(Some(predicted)),
    );
    assert_eq!(
        resp.status,
        Status::Ok,
        "BulkDelete with correct prediction should succeed"
    );
}

/// BulkUpdate with a stale prediction (concurrent insert raced) returns
/// OllpRetryRequired WITHOUT writing.
///
/// Scenario:
/// 1. Pre-exec scan sees {z1, z2} → predicted = [z1, z2].
/// 2. A concurrent insert adds z3 (active=true).
/// 3. BulkUpdate with predicted=[z1, z2] is admitted.
/// 4. Executor scans and finds {z1, z2, z3} — mismatch → OllpRetryRequired.
/// 5. The z1/z2 values are NOT updated.
#[test]
fn bulk_update_stale_prediction_returns_ollp_retry_required() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "z1");
    insert_active(&mut ctx, "z2");

    // Simulate: pre-exec captured [z1, z2] as predicted surrogates.
    let predicted = vec![surrogate_u32("z1"), surrogate_u32("z2")];

    // Concurrent insert: z3 lands after the pre-exec scan but before admission.
    insert_active(&mut ctx, "z3");

    // Submit BulkUpdate with the stale predicted set.
    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(Some(predicted)),
    );

    assert_eq!(
        resp.status,
        Status::Error,
        "stale prediction should produce Status::Error"
    );
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired),
        "error code must be OllpRetryRequired, got {:?}",
        resp.error_code
    );

    // Verify no write occurred: z1 should still have name="z1", not "updated".
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                COLLECTION,
            ),
            document_id: "z1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: surrogate_for("z1"),
            pk_bytes: "z1".as_bytes().to_vec(),
        }),
    );
    let val = payload_value(&payload);
    let name = val.get("name").and_then(|n| n.as_str()).unwrap_or_default();
    assert_eq!(
        name, "z1",
        "OllpRetryRequired must not have modified the document"
    );
}

/// BulkDelete with a stale prediction returns OllpRetryRequired WITHOUT deleting.
#[test]
fn bulk_delete_stale_prediction_returns_ollp_retry_required() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "d1");
    insert_active(&mut ctx, "d2");

    let predicted = vec![surrogate_u32("d1"), surrogate_u32("d2")];

    // Concurrent insert adds d3.
    insert_active(&mut ctx, "d3");

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan(Some(predicted)),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired)
    );
}

// ── edge-content drift (TOCTOU on _from/_to/_type) ───────────────────────────
//
// These tests cover the data-plane edge-content validation added to
// `execute_bulk_delete`. The surrogate set is held CORRECT in every case so the
// surrogate check passes — the ONLY thing under test is whether a divergence in
// the actual edge tuples `(surrogate, _from, _to, _type)` versus the predicted
// edge set triggers `OllpRetryRequired` before any write. The full concurrent
// race is NOT force-tested here (no flaky timing); the detection mechanism is
// proven by mutating the stored doc between building the prediction and
// admission. The retry/rescan path itself is already e2e-proven by
// `ollp_implicit_edge_delete_cleans_reverse_cross_node`.

/// Matching predicted + actual edge sets → delete proceeds (no false retry).
#[test]
fn bulk_delete_matching_edges_proceeds() {
    let mut ctx = make_ctx();
    insert_active_edge(&mut ctx, "e1", "a", "b", Some("ROAD"));
    insert_active_edge(&mut ctx, "e2", "c", "d", None);

    let predicted = vec![surrogate_u32("e1"), surrogate_u32("e2")];
    let edges = vec![
        predicted_edge("e1", "a", "b", Some("ROAD")),
        predicted_edge("e2", "c", "d", None),
    ];

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan_with_edges(Some(predicted), Some(edges)),
    );
    assert_eq!(
        resp.status,
        Status::Ok,
        "matching edge content should let the delete proceed"
    );
}

/// A matched doc's `_to` was concurrently changed between recon and admission.
/// Surrogate set is unchanged, so only the edge-content check can catch it →
/// OllpRetryRequired WITHOUT deleting.
#[test]
fn bulk_delete_changed_edge_endpoint_returns_ollp_retry_required() {
    let mut ctx = make_ctx();
    insert_active_edge(&mut ctx, "e1", "a", "b", Some("ROAD"));

    // Pre-exec captured the edge as a->b.
    let predicted = vec![surrogate_u32("e1")];
    let edges = vec![predicted_edge("e1", "a", "b", Some("ROAD"))];

    // Concurrent UPDATE: same surrogate, but `_to` now points at "z".
    insert_active_edge(&mut ctx, "e1", "a", "z", Some("ROAD"));

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan_with_edges(Some(predicted), Some(edges)),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired),
        "changed edge endpoint must trigger OllpRetryRequired, got {:?}",
        resp.error_code
    );

    // No delete occurred: e1 is still present.
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                COLLECTION,
            ),
            document_id: "e1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: surrogate_for("e1"),
            pk_bytes: "e1".as_bytes().to_vec(),
        }),
    );
    let val = payload_value(&payload);
    assert_eq!(
        val.get("_to").and_then(|v| v.as_str()),
        Some("z"),
        "edge doc must not have been deleted"
    );
}

/// A matched doc's `_type` (label) was concurrently changed → retry.
#[test]
fn bulk_delete_changed_edge_label_returns_ollp_retry_required() {
    let mut ctx = make_ctx();
    insert_active_edge(&mut ctx, "e1", "a", "b", Some("ROAD"));

    let predicted = vec![surrogate_u32("e1")];
    let edges = vec![predicted_edge("e1", "a", "b", Some("ROAD"))];

    // Concurrent UPDATE: label changed ROAD → RAIL.
    insert_active_edge(&mut ctx, "e1", "a", "b", Some("RAIL"));

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan_with_edges(Some(predicted), Some(edges)),
    );
    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired)
    );
}

/// A NEW edge appeared among the matched docs (a matched doc gained `_from`/`_to`
/// after recon). Surrogate set unchanged → only edge-content check catches it.
#[test]
fn bulk_delete_appeared_edge_returns_ollp_retry_required() {
    let mut ctx = make_ctx();
    // At recon time "p1" was a plain (non-edge) active doc.
    insert_active(&mut ctx, "p1");

    let predicted = vec![surrogate_u32("p1")];
    // Predicted edge set is EMPTY — p1 was not an edge at recon.
    let edges: Vec<OllpPredictedEdge> = vec![];

    // Concurrent UPDATE: p1 gained `_from`/`_to`, becoming an edge.
    insert_active_edge(&mut ctx, "p1", "a", "b", None);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan_with_edges(Some(predicted), Some(edges)),
    );
    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired)
    );
}

/// `ollp_predicted_edges: None` → edge-content check is skipped entirely; the
/// delete proceeds on the surrogate check alone (back-compat with non-OLLP and
/// surrogate-only OLLP plans).
#[test]
fn bulk_delete_no_predicted_edges_skips_edge_check() {
    let mut ctx = make_ctx();
    insert_active_edge(&mut ctx, "e1", "a", "b", Some("ROAD"));

    let predicted = vec![surrogate_u32("e1")];
    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_delete_plan_with_edges(Some(predicted), None),
    );
    assert_eq!(
        resp.status,
        Status::Ok,
        "absent predicted edges must skip edge validation"
    );
}

/// After OllpRetryRequired, the caller re-scans and retries with the corrected
/// predicted set. The second attempt succeeds and all three docs are updated.
#[test]
fn bulk_update_retry_with_corrected_prediction_succeeds() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "r1");
    insert_active(&mut ctx, "r2");

    // First attempt: stale prediction [r1, r2] — r3 was concurrently inserted.
    let stale_predicted = vec![surrogate_u32("r1"), surrogate_u32("r2")];
    insert_active(&mut ctx, "r3");

    let first_resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(Some(stale_predicted)),
    );
    assert_eq!(
        first_resp.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired)
    );

    // Retry: re-scan sees {r1, r2, r3} → corrected prediction.
    let corrected_predicted = vec![
        surrogate_u32("r1"),
        surrogate_u32("r2"),
        surrogate_u32("r3"),
    ];
    let retry_resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(Some(corrected_predicted)),
    );

    assert_eq!(
        retry_resp.status,
        Status::Ok,
        "retry with corrected prediction must succeed"
    );

    // Verify the write went through for all three docs.
    for id in ["r1", "r2", "r3"] {
        let payload = send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointGet {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    COLLECTION,
                ),
                document_id: id.into(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
                surrogate: surrogate_for(id),
                pk_bytes: id.as_bytes().to_vec(),
            }),
        );
        let val = payload_value(&payload);
        let name = val.get("name").and_then(|n| n.as_str()).unwrap_or_default();
        assert_eq!(
            name, "updated",
            "doc {id} should have been updated on retry"
        );
    }
}

/// Prediction that is a superset of the actual set (a document was deleted
/// concurrently) also triggers OllpRetryRequired.
#[test]
fn bulk_update_superset_prediction_returns_ollp_retry_required() {
    let mut ctx = make_ctx();
    insert_active(&mut ctx, "s1");
    insert_active(&mut ctx, "s2");

    // Pre-exec captured [s1, s2] but s2 was deleted before admission.
    // Delete s2 to simulate the concurrent delete.
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                COLLECTION,
            ),
            document_id: "s2".into(),
            surrogate: surrogate_for("s2"),
            pk_bytes: "s2".as_bytes().to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Submit with the now-stale superset prediction.
    let stale_predicted = vec![surrogate_u32("s1"), surrogate_u32("s2")];
    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        bulk_update_plan(Some(stale_predicted)),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired)
    );
}
