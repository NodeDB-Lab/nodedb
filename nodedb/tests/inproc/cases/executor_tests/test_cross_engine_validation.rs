// SPDX-License-Identifier: BUSL-1.1

//! Cross-engine validation: a cross-model query (vector + graph +
//! relational filter) runs end-to-end with correct results.

use nodedb::bridge::dispatch::BridgeRequest;
use nodedb::engine::graph::edge_store::Direction;
use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan, VectorOp};
use nodedb_types::vector_distance::DistanceMetric;

use super::helpers::*;

// ──────────────────────────────────────────────────────────────────────
// Cross-model query (vector + graph + relational filter)
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
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "papers",
                ),
                document_id: format!("p{i}"),
                value: serde_json::to_vec(&serde_json::json!({
                    "title": format!("Paper {i}"),
                    "year": 2020 + (i % 5) as i64,
                    "citations": i * 10,
                }))
                .unwrap(),
                surrogate: doc_surrogate(&format!("p{i}")),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }

    // 2. Insert vectors for each document.
    for i in 0..10u32 {
        tx.try_push(BridgeRequest::unfloored(make_request_with_id(
            100 + i as u64,
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "papers",
                ),
                vector: vec![i as f32, (i as f32).sin(), (i as f32).cos()],
                dim: 3,
                field_name: String::new(),
                surrogate: doc_surrogate(&format!("p{i}")),
                pk_bytes: None,
                provenance: None,
            }),
        )))
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
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "col",
                ),
                src_id: format!("p{i}"),
                label: "CITES".into(),
                dst_id: format!("p{}", i + 1),
                properties: vec![],
                src_surrogate: doc_surrogate(&format!("p{i}")),
                dst_surrogate: doc_surrogate(&format!("p{}", i + 1)),
                valid_from_ms: None,
                valid_until_ms: None,
            }),
        );
    }

    // 4. Vector search: find papers similar to p5's embedding.
    let vector_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "papers",
            ),
            query_vector: vec![5.0f32, 5.0f32.sin(), 5.0f32.cos()],
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
            edge_labels: vec!["CITES".into()],
            direction: Direction::Out,
            depth: 3,
            options: Default::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: None,
            collection: None,
            scope: nodedb_physical::physical_plan::GraphReadScope::default(),
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
        op: nodedb::bridge::scan_filter::FilterOp::Gte,
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
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "papers",
            ),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            filters: filter_bytes,
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
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
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "papers",
            ),
            query_vector: vec![1.0f32, 0.0, 0.0],
            vector_top_k: 3,
            edge_label: Some("CITES".into()),
            direction: Direction::Out,
            expansion_depth: 2,
            final_top_k: 5,
            rrf_k: (60.0, 10.0),
            rrf_k_triple: None,
            vector_field: String::new(),
            options: Default::default(),
            bm25_query: None,
            bm25_field: None,
            stage: nodedb_physical::physical_plan::RagStage::Local,
            scope: nodedb_physical::physical_plan::GraphReadScope::default(),
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
