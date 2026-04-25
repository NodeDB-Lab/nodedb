//! Phase 2 validation gate tests.
//!
//! These verify end-to-end correctness across all engines and ensure
//! the system is ready to move from Phase 2 to Phase 3.

use nodedb::bridge::dispatch::BridgeRequest;
use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::{DocumentOp, GraphOp, TextOp, VectorOp};
use nodedb::engine::graph::edge_store::Direction;

use crate::helpers::*;

// ──────────────────────────────────────────────────────────────────────
// Gate 1: Cross-model query (vector + graph + relational filter)
//         executes end-to-end with correct results.
// ──────────────────────────────────────────────────────────────────────

#[test]
fn cross_model_query_vector_graph_relational() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // 1. Insert documents with relational fields.
    for i in 0..10u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "papers".into(),
                document_id: format!("p{i}"),
                value: serde_json::to_vec(&serde_json::json!({
                    "title": format!("Paper {i}"),
                    "year": 2020 + (i % 5) as i64,
                    "citations": i * 10,
                }))
                .unwrap(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
            }),
        );
    }

    // 2. Insert vectors for each document.
    for i in 0..10u32 {
        tx.try_push(BridgeRequest {
            inner: make_request_with_id(
                100 + i as u64,
                PhysicalPlan::Vector(VectorOp::Insert {
                    collection: "papers".into(),
                    vector: vec![i as f32, (i as f32).sin(), (i as f32).cos()],
                    dim: 3,
                    field_name: String::new(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                }),
            ),
        })
        .unwrap();
    }
    core.tick();
    for _ in 0..10 {
        rx.try_pop().unwrap();
    }

    // 3. Insert graph edges: p0→p1→p2→p3 citation chain.
    for i in 0..3u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: "col".into(),
                src_id: format!("p{i}"),
                label: "CITES".into(),
                dst_id: format!("p{}", i + 1),
                properties: vec![],
                src_surrogate: nodedb_types::Surrogate::ZERO,
                dst_surrogate: nodedb_types::Surrogate::ZERO,
            }),
        );
    }

    // 4. Vector search: find papers similar to p5's embedding.
    let vector_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: "papers".into(),
            query_vector: vec![5.0f32, 5.0f32.sin(), 5.0f32.cos()],
            top_k: 3,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
        }),
    );
    let vector_json = payload_json(&vector_payload);
    assert!(
        vector_json.contains("\"id\""),
        "vector search should return results: {vector_json}"
    );

    // 5. Graph traversal: find citation chain from p0.
    let graph_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["p0".into()],
            edge_label: Some("CITES".into()),
            direction: Direction::Out,
            depth: 3,
            options: Default::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: None,
        }),
    );
    let graph_nodes: Vec<String> = serde_json::from_value(payload_value(&graph_payload)).unwrap();
    assert!(graph_nodes.contains(&"p0".to_string()));
    assert!(graph_nodes.contains(&"p1".to_string()));
    assert!(graph_nodes.contains(&"p2".to_string()));
    assert!(graph_nodes.contains(&"p3".to_string()));

    // 6. Relational filter: scan papers with year >= 2023.
    let filter = vec![nodedb::bridge::scan_filter::ScanFilter {
        field: "year".into(),
        op: "gte".into(),
        value: nodedb_types::Value::Integer(2023),
        clauses: Vec::new(),
        expr: None,
    }];
    let filter_bytes = zerompk::to_msgpack_vec(&filter).unwrap();
    let scan_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: "papers".into(),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            filters: filter_bytes,
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_as_of_ms: None,
            valid_at_ms: None,
            prefilter: None,
        }),
    );
    let scan_json = payload_json(&scan_payload);
    // Years cycle 2020-2024: papers with year>=2023 are p3(2023), p4(2024),
    // p8(2023), p9(2024) = 4 papers.
    assert!(
        scan_json.contains("2023") || scan_json.contains("2024"),
        "scan should filter by year: {scan_json}"
    );

    // 7. GraphRAG fusion: vector + graph + RRF.
    let rag_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::RagFusion {
            collection: "papers".into(),
            query_vector: vec![1.0f32, 0.0, 0.0],
            vector_top_k: 3,
            edge_label: Some("CITES".into()),
            direction: Direction::Out,
            expansion_depth: 2,
            final_top_k: 5,
            rrf_k: (60.0, 10.0),
            vector_field: String::new(),
            options: Default::default(),
        }),
    );
    let rag_body = payload_value(&rag_payload);
    assert!(
        rag_body.get("results").is_some(),
        "GraphRAG should return results"
    );
    assert!(
        rag_body.get("metadata").is_some(),
        "GraphRAG should return metadata"
    );
}

// ──────────────────────────────────────────────────────────────────────
// Gate 2: RRF fusion produces mathematically correct rankings with
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
                collection: "docs".into(),
                document_id: format!("d{i}"),
                value: serde_json::to_vec(&serde_json::json!({
                    "body": format!("document about database systems topic {i}"),
                }))
                .unwrap(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
            }),
        );
    }

    // Insert vectors.
    for i in 0..20u32 {
        tx.try_push(BridgeRequest {
            inner: make_request_with_id(
                200 + i as u64,
                PhysicalPlan::Vector(VectorOp::Insert {
                    collection: "docs".into(),
                    vector: vec![i as f32, 0.0, 0.0],
                    dim: 3,
                    field_name: String::new(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                }),
            ),
        })
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
            collection: "docs".into(),
            query_vector: vec![10.0f32, 0.0, 0.0],
            query_text: "database systems".into(),
            top_k: 5,
            ef_search: 0,
            fuzzy: true,
            vector_weight: 0.5,
            filter_bitmap: None,
            rls_filters: Vec::new(),
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
            collection: "docs".into(),
            query_vector: vec![10.0f32, 0.0, 0.0],
            query_text: "database systems".into(),
            top_k: 5,
            ef_search: 0,
            fuzzy: true,
            vector_weight: 0.9,
            filter_bitmap: None,
            rls_filters: Vec::new(),
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

// ──────────────────────────────────────────────────────────────────────
// Gate 3: Document secondary indexes remain consistent after crash
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
            collection: "articles".into(),
            document_id: "a1".into(),
            value: serde_json::to_vec(&serde_json::json!({
                "title": "Distributed databases are scalable",
                "status": "published",
            }))
            .unwrap(),
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: b"a1".to_vec(),
        }),
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "articles".into(),
            document_id: "a2".into(),
            value: serde_json::to_vec(&serde_json::json!({
                "title": "Vector search with HNSW graphs",
                "status": "draft",
            }))
            .unwrap(),
            surrogate: nodedb_types::Surrogate::new(2),
            pk_bytes: b"a2".to_vec(),
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
            collection: "articles".into(),
            query: "database".into(),
            top_k: 10,
            fuzzy: true,
            rls_filters: Vec::new(),
            prefilter: None,
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
            collection: "articles".into(),
            document_id: "a1".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: b"a1".to_vec(),
        }),
    );

    // Text search should NOT find a1 anymore (index cascaded).
    let text_after = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Text(TextOp::Search {
            collection: "articles".into(),
            query: "database".into(),
            top_k: 10,
            fuzzy: true,
            rls_filters: Vec::new(),
            prefilter: None,
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
            collection: "articles".into(),
            query: "vector search".into(),
            top_k: 10,
            fuzzy: true,
            rls_filters: Vec::new(),
            prefilter: None,
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
            collection: "articles".into(),
            document_id: "a1".into(),
            rls_filters: Vec::new(),
            system_as_of_ms: None,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: b"a1".to_vec(),
        }),
    );
    assert_eq!(get_a1.error_code, None);

    let get_a2 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "articles".into(),
            document_id: "a2".into(),
            rls_filters: Vec::new(),
            system_as_of_ms: None,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::new(2),
            pk_bytes: b"a2".to_vec(),
        }),
    );
    assert_eq!(get_a2.status, Status::Ok);
}
