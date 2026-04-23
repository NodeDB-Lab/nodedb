//! Integration tests for graph engine operations.

use nodedb::bridge::dispatch::BridgeRequest;
use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::{GraphOp, VectorOp};
use nodedb::engine::graph::edge_store::Direction;

use crate::helpers::*;

#[test]
fn edge_put_and_graph_neighbors() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    for dst in &["bob", "carol"] {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: "col".into(),
                src_id: "alice".into(),
                label: "KNOWS".into(),
                dst_id: dst.to_string(),
                properties: vec![],
            }),
        );
    }

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "alice".into(),
            edge_label: Some("KNOWS".into()),
            direction: Direction::Out,
            rls_filters: Vec::new(),
        }),
    );
    let json = payload_json(&payload);
    assert!(json.contains("bob"), "payload: {json}");
    assert!(json.contains("carol"), "payload: {json}");
}

#[test]
fn graph_hop_traversal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    for (s, d) in &[("a", "b"), ("b", "c")] {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: "col".into(),
                src_id: s.to_string(),
                label: "NEXT".into(),
                dst_id: d.to_string(),
                properties: vec![],
            }),
        );
    }

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["a".into()],
            edge_label: Some("NEXT".into()),
            direction: Direction::Out,
            depth: 2,
            options: Default::default(),
            rls_filters: Vec::new(),
        }),
    );
    let nodes: Vec<String> = serde_json::from_value(payload_value(&payload)).unwrap();
    assert!(nodes.contains(&"a".to_string()));
    assert!(nodes.contains(&"b".to_string()));
    assert!(nodes.contains(&"c".to_string()));
}

#[test]
fn graph_path_and_subgraph() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    for (s, d) in &[("a", "b"), ("b", "c")] {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: "col".into(),
                src_id: s.to_string(),
                label: "L".into(),
                dst_id: d.to_string(),
                properties: vec![],
            }),
        );
    }

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Path {
            src: "a".into(),
            dst: "c".into(),
            edge_label: Some("L".into()),
            max_depth: 5,
            options: Default::default(),
            rls_filters: Vec::new(),
        }),
    );
    let path: Vec<String> = serde_json::from_value(payload_value(&payload)).unwrap();
    assert_eq!(path, vec!["a", "b", "c"]);

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Subgraph {
            start_nodes: vec!["a".into()],
            edge_label: None,
            depth: 2,
            options: Default::default(),
            rls_filters: Vec::new(),
        }),
    );
    let edges: Vec<serde_json::Value> = serde_json::from_value(payload_value(&payload)).unwrap();
    assert_eq!(edges.len(), 2);
}

#[test]
fn edge_delete_updates_csr() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: "col".into(),
            src_id: "x".into(),
            label: "R".into(),
            dst_id: "y".into(),
            properties: vec![],
        }),
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection: "col".into(),
            src_id: "x".into(),
            label: "R".into(),
            dst_id: "y".into(),
        }),
    );

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "x".into(),
            edge_label: None,
            direction: Direction::Out,
            rls_filters: Vec::new(),
        }),
    );
    let neighbors: Vec<serde_json::Value> =
        serde_json::from_value(payload_value(&payload)).unwrap();
    assert!(neighbors.is_empty());
}

#[test]
fn graph_rag_fusion_pipeline() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert vectors.
    for i in 0..10u32 {
        tx.try_push(BridgeRequest {
            inner: make_request_with_id(
                100 + i as u64,
                PhysicalPlan::Vector(VectorOp::Insert {
                    collection: "docs".into(),
                    vector: vec![i as f32, 0.0, 0.0],
                    dim: 3,
                    field_name: String::new(),
                    doc_id: None,
                }),
            ),
        })
        .unwrap();
    }
    core.tick();
    for _ in 0..10 {
        rx.try_pop().unwrap();
    }

    // Insert edges.
    for (s, d) in &[("0", "1"), ("1", "2"), ("2", "3")] {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: "col".into(),
                src_id: s.to_string(),
                label: "CITES".into(),
                dst_id: d.to_string(),
                properties: vec![],
            }),
        );
    }

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::RagFusion {
            collection: "docs".into(),
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

    let body = payload_value(&payload);
    let results = body["results"].as_array().expect("results array");
    let metadata = body.get("metadata").expect("metadata");

    assert!(!results.is_empty());
    assert!(results[0].get("rrf_score").is_some());
    assert!(results[0].get("node_id").is_some());
    assert!(metadata.get("vector_candidates").is_some());
    assert!(metadata.get("graph_expanded").is_some());
    assert_eq!(metadata["truncated"], false);
}
