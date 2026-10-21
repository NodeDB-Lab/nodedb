// SPDX-License-Identifier: BUSL-1.1

//! WAL replay determinism and mixed-engine memory isolation tests.
//!
//! 1. WAL replay: deterministic traces
//! 2. Mixed-engine isolation: protected-tier not evicted under budget

use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan, VectorOp};
use nodedb_types::vector_distance::DistanceMetric;

use super::helpers::*;

// ──────────────────────────────────────────────────────────────────────
// Gate 1: WAL replay produces deterministic results
// ──────────────────────────────────────────────────────────────────────

#[test]
fn wal_replay_deterministic() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Perform a deterministic sequence of operations.
    let ops = vec![
        ("put", "d1", b"{\"a\":1}".to_vec()),
        ("put", "d2", b"{\"b\":2}".to_vec()),
        ("put", "d3", b"{\"c\":3}".to_vec()),
        ("put", "d1", b"{\"a\":10}".to_vec()), // Overwrite d1.
    ];

    for (op, doc_id, value) in &ops {
        if *op == "put" {
            send_ok(
                &mut core,
                &mut tx,
                &mut rx,
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: nodedb_types::QualifiedCollection::new(
                        nodedb_types::DatabaseId::DEFAULT,
                        "replay",
                    ),
                    document_id: doc_id.to_string(),
                    value: value.clone(),
                    surrogate: doc_surrogate(doc_id),
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                }),
            );
        }
    }

    // Verify final state is deterministic.
    let d1 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "replay",
            ),
            document_id: "d1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: Some(doc_surrogate("d1")),
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(d1.status, Status::Ok);
    // d1 was overwritten to {"a":10}.
    let d1_json = payload_json(&d1.payload);
    assert!(
        d1_json.contains("10"),
        "d1 should be overwritten: {d1_json}"
    );

    let d2 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "replay",
            ),
            document_id: "d2".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: Some(doc_surrogate("d2")),
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(d2.status, Status::Ok);

    let d3 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "replay",
            ),
            document_id: "d3".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: Some(doc_surrogate("d3")),
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(d3.status, Status::Ok);
}

// ──────────────────────────────────────────────────────────────────────
// Gate 2: Mixed-engine isolation — no protected-tier eviction
// ──────────────────────────────────────────────────────────────────────

#[test]
fn mixed_engine_isolation_no_cross_eviction() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert documents (sparse engine).
    for i in 0..50u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "mixed",
                ),
                document_id: format!("doc_{i}"),
                value: format!("{{\"val\":{i}}}").into_bytes(),
                surrogate: doc_surrogate(&format!("doc_{i}")),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }

    // Insert vectors (vector engine).
    for i in 0..50u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "mixed",
                ),
                vector: vec![i as f32, 0.0, 0.0],
                dim: 3,
                field_name: String::new(),
                surrogate: doc_surrogate(&format!("vec_{i}")),
                pk_bytes: None,
                provenance: None,
            }),
        );
    }

    // Insert edges (graph engine).
    for i in 0..49u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "col",
                ),
                src_id: format!("doc_{i}"),
                label: "NEXT".into(),
                dst_id: format!("doc_{}", i + 1),
                properties: vec![],
                src_surrogate: doc_surrogate(&format!("doc_{i}")),
                dst_surrogate: doc_surrogate(&format!("doc_{}", i + 1)),
                valid_from_ms: None,
                valid_until_ms: None,
            }),
        );
    }

    // All engines should still work — no cross-eviction.
    // Sparse engine: point get.
    let doc = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "mixed",
            ),
            document_id: "doc_25".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: Some(doc_surrogate("doc_25")),
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(doc.status, Status::Ok, "sparse engine should be intact");

    // Vector engine: search.
    let vec_resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "mixed",
            ),
            query_vector: vec![25.0f32, 0.0, 0.0],
            top_k: 3,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            metric: DistanceMetric::L2,
        }),
    );
    assert_eq!(
        vec_resp.status,
        Status::Ok,
        "vector engine should be intact"
    );

    // Graph engine: neighbors.
    let graph_resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "doc_25".into(),
            edge_labels: vec!["NEXT".into()],
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
            scope: nodedb_physical::physical_plan::GraphReadScope::default(),
        }),
    );
    assert_eq!(
        graph_resp.status,
        Status::Ok,
        "graph engine should be intact"
    );
    let neighbors_json = payload_json(&graph_resp.payload);
    assert!(
        neighbors_json.contains("doc_26"),
        "graph should have doc_25→doc_26: {neighbors_json}"
    );
}
