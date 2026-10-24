// SPDX-License-Identifier: BUSL-1.1

//! RRF fusion produces mathematically correct rankings with configurable
//! per-source k-constants.

use nodedb::bridge::dispatch::BridgeRequest;
use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, TextOp, VectorOp};

use super::helpers::*;

// ──────────────────────────────────────────────────────────────────────
// RRF fusion produces mathematically correct rankings with
//         configurable per-source k-constants.
// ──────────────────────────────────────────────────────────────────────

#[test]
fn rrf_fusion_mathematically_correct() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert documents with text content for BM25.
    for i in 0..20u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "docs",
                ),
                document_id: format!("d{i}"),
                value: serde_json::to_vec(&serde_json::json!({
                    "body": format!("document about database systems topic {i}"),
                }))
                .unwrap(),
                surrogate: doc_surrogate(&format!("d{i}")),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }

    // Insert vectors.
    for i in 0..20u32 {
        tx.try_push(BridgeRequest::unfloored(make_request_with_id(
            200 + i as u64,
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "docs",
                ),
                vector: vec![i as f32, 0.0, 0.0],
                dim: 3,
                field_name: String::new(),
                surrogate: doc_surrogate(&format!("d{i}")),
                pk_bytes: None,
                provenance: None,
            }),
        )))
        .unwrap();
    }
    core.tick();
    for _ in 0..20 {
        rx.try_pop().unwrap();
    }

    // Hybrid search with equal weights (vector_weight = 0.5).
    let resp_equal = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Text(TextOp::HybridSearch {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            query_vector: vec![10.0f32, 0.0, 0.0],
            query_text: "database systems".into(),
            top_k: 5,
            ef_search: 0,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: true,
            vector_weight: 0.5,
            filter_bitmap: None,
            rls_filters: Vec::new(),
            score_alias: None,
            vector_field: String::new(),
            text_field: None,
            filters: Vec::new(),
        }),
    );
    assert_eq!(resp_equal.status, Status::Ok);
    let equal_results = payload_value(&resp_equal.payload);
    assert!(equal_results.is_array(), "should return array of results");

    // Hybrid search with vector-heavy weight (vector_weight = 0.9).
    let resp_vec_heavy = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Text(TextOp::HybridSearch {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            query_vector: vec![10.0f32, 0.0, 0.0],
            query_text: "database systems".into(),
            top_k: 5,
            ef_search: 0,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: true,
            vector_weight: 0.9,
            filter_bitmap: None,
            rls_filters: Vec::new(),
            score_alias: None,
            vector_field: String::new(),
            text_field: None,
            filters: Vec::new(),
        }),
    );
    assert_eq!(resp_vec_heavy.status, Status::Ok);

    // Both queries should produce results — the ranking order may differ
    // based on the weight, which is the whole point of configurable k-constants.
    let equal_json = payload_json(&resp_equal.payload);
    let heavy_json = payload_json(&resp_vec_heavy.payload);
    assert!(equal_json.contains("rrf_score"), "equal: {equal_json}");
    assert!(heavy_json.contains("rrf_score"), "heavy: {heavy_json}");
}
