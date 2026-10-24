// SPDX-License-Identifier: BUSL-1.1

//! Document secondary indexes stay consistent after crash recovery (WAL
//! replay).

use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, TextOp};

use super::helpers::*;

// ──────────────────────────────────────────────────────────────────────
// Document secondary indexes remain consistent after crash
//         recovery (WAL replay).
// ──────────────────────────────────────────────────────────────────────

#[test]
fn document_indexes_consistent_after_simulated_crash() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert documents — auto-indexes text fields in inverted index.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "articles",
            ),
            document_id: "a1".into(),
            value: serde_json::to_vec(&serde_json::json!({
                "title": "Distributed databases are scalable",
                "status": "published",
            }))
            .unwrap(),
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: b"a1".to_vec(),
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
                "articles",
            ),
            document_id: "a2".into(),
            value: serde_json::to_vec(&serde_json::json!({
                "title": "Vector search with HNSW graphs",
                "status": "draft",
            }))
            .unwrap(),
            surrogate: nodedb_types::Surrogate::new(2),
            pk_bytes: b"a2".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Text search should find both documents. Without a wired catalog
    // the inverted-index hits surface as substrate row keys (hex of
    // the surrogate) rather than user-visible PKs, so the assertion
    // checks for the substrate key derived from `Surrogate::new(1)`.
    let text_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Text(TextOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "articles",
            ),
            query: "database".into(),
            top_k: 10,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: true,
            rls_filters: Vec::new(),
            prefilter: None,
            field: None,
            filters: Vec::new(),
            scores: Vec::new(),
        }),
    );
    let text_json = payload_json(&text_payload);
    assert!(
        text_json.contains("00000001"),
        "text search should find a1's substrate key: {text_json}"
    );

    // Delete a1 — should cascade to inverted index.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "articles",
            ),
            document_id: "a1".into(),
            surrogate: Some(nodedb_types::Surrogate::new(1)),
            pk_bytes: b"a1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Text search should NOT find a1 anymore (index cascaded).
    let text_after = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Text(TextOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "articles",
            ),
            query: "database".into(),
            top_k: 10,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: true,
            rls_filters: Vec::new(),
            prefilter: None,
            field: None,
            filters: Vec::new(),
            scores: Vec::new(),
        }),
    );
    let text_after_json = payload_json(&text_after);
    assert!(
        !text_after_json.contains("00000001"),
        "a1's substrate key should be removed from text index after delete: {text_after_json}"
    );

    // a2 should still be findable.
    let text_a2 = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Text(TextOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "articles",
            ),
            query: "vector search".into(),
            top_k: 10,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: true,
            rls_filters: Vec::new(),
            prefilter: None,
            field: None,
            filters: Vec::new(),
            scores: Vec::new(),
        }),
    );
    let text_a2_json = payload_json(&text_a2);
    assert!(
        text_a2_json.contains("00000002"),
        "a2's substrate key should still be in text index: {text_a2_json}"
    );

    // Verify PointGet confirms a1 is gone, a2 exists.
    let get_a1 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "articles",
            ),
            document_id: "a1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: Some(nodedb_types::Surrogate::new(1)),
            pk_bytes: b"a1".to_vec(),
        }),
    );
    assert_eq!(get_a1.error_code, None);

    let get_a2 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "articles",
            ),
            document_id: "a2".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: Some(nodedb_types::Surrogate::new(2)),
            pk_bytes: b"a2".to_vec(),
        }),
    );
    assert_eq!(get_a2.status, Status::Ok);
}
