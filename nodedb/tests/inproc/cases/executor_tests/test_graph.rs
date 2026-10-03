// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for graph engine operations.

use nodedb::bridge::dispatch::BridgeRequest;
use nodedb::engine::graph::edge_store::Direction;
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan, VectorOp};

use super::helpers::*;

#[test]
fn edge_put_and_graph_neighbors() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    for dst in &["bob", "carol"] {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "col",
                ),
                src_id: "alice".into(),
                label: "KNOWS".into(),
                dst_id: dst.to_string(),
                properties: vec![],
                src_surrogate: doc_surrogate("alice"),
                dst_surrogate: doc_surrogate(dst),
            }),
        );
    }

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "alice".into(),
            edge_labels: vec!["KNOWS".into()],
            direction: Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
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
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "col",
                ),
                src_id: s.to_string(),
                label: "NEXT".into(),
                dst_id: d.to_string(),
                properties: vec![],
                src_surrogate: doc_surrogate(s),
                dst_surrogate: doc_surrogate(d),
            }),
        );
    }

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["a".into()],
            edge_labels: vec!["NEXT".into()],
            direction: Direction::Out,
            depth: 2,
            options: Default::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: None,
            collection: None,
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
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "col",
                ),
                src_id: s.to_string(),
                label: "L".into(),
                dst_id: d.to_string(),
                properties: vec![],
                src_surrogate: doc_surrogate(s),
                dst_surrogate: doc_surrogate(d),
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
            edge_labels: vec!["L".into()],
            max_depth: 5,
            options: Default::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: None,
            collection: None,
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
            edge_labels: Vec::new(),
            depth: 2,
            options: Default::default(),
            rls_filters: Vec::new(),
            collection: None,
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
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "col",
            ),
            src_id: "x".into(),
            label: "R".into(),
            dst_id: "y".into(),
            properties: vec![],
            src_surrogate: doc_surrogate("x"),
            dst_surrogate: doc_surrogate("y"),
        }),
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "col",
            ),
            src_id: "x".into(),
            label: "R".into(),
            dst_id: "y".into(),
            src_surrogate: doc_surrogate("x"),
            dst_surrogate: doc_surrogate("y"),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }),
    );

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "x".into(),
            edge_labels: Vec::new(),
            direction: Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
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
        tx.try_push(BridgeRequest::unfloored(make_request_with_id(
            100 + i as u64,
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "docs",
                ),
                vector: vec![i as f32, 0.0, 0.0],
                dim: 3,
                field_name: String::new(),
                // The vector binds the surrogate of graph node `i`, so a hit
                // seeds the expansion from that node.
                surrogate: doc_surrogate(&i.to_string()),
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

    // Insert edges.
    for (s, d) in &[("0", "1"), ("1", "2"), ("2", "3")] {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "col",
                ),
                src_id: s.to_string(),
                label: "CITES".into(),
                dst_id: d.to_string(),
                properties: vec![],
                src_surrogate: doc_surrogate(s),
                dst_surrogate: doc_surrogate(d),
            }),
        );
    }

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::RagFusion {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
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

/// `a -KNOWS-> b`, `a -WORKS-> c`, `a -LIKES-> d`, `b -WORKS-> e`,
/// `c -LIKES-> f`.
fn label_set_graph() -> (
    nodedb::data::executor::core_loop::CoreLoop,
    nodedb_bridge::buffer::Producer<BridgeRequest>,
    nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    tempfile::TempDir,
) {
    let (mut core, mut tx, mut rx, dir) = make_core();
    for (s, label, d) in [
        ("a", "KNOWS", "b"),
        ("a", "WORKS", "c"),
        ("a", "LIKES", "d"),
        ("b", "WORKS", "e"),
        ("c", "LIKES", "f"),
    ] {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "col",
                ),
                src_id: s.into(),
                label: label.into(),
                dst_id: d.into(),
                properties: vec![],
                src_surrogate: doc_surrogate(s),
                dst_surrogate: doc_surrogate(d),
            }),
        );
    }
    (core, tx, rx, dir)
}

fn knows_or_works() -> Vec<String> {
    vec!["KNOWS".into(), "WORKS".into()]
}

#[test]
fn hop_follows_every_listed_label() {
    let (mut core, mut tx, mut rx, _dir) = label_set_graph();
    let hop = |labels: Vec<String>| {
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["a".into()],
            edge_labels: labels,
            direction: Direction::Out,
            depth: 2,
            options: Default::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: None,
            collection: None,
        })
    };

    let payload = send_ok(&mut core, &mut tx, &mut rx, hop(knows_or_works()));
    let mut nodes: Vec<String> = serde_json::from_value(payload_value(&payload)).unwrap();
    nodes.sort();
    assert_eq!(nodes, vec!["a", "b", "c", "e"]);

    // An unknown label adds nothing to the set it joins.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        hop(vec!["KNOWS".into(), "ABSENT".into()]),
    );
    let mut nodes: Vec<String> = serde_json::from_value(payload_value(&payload)).unwrap();
    nodes.sort();
    assert_eq!(nodes, vec!["a", "b"]);

    // A set of only unknown labels keeps no edge.
    let payload = send_ok(&mut core, &mut tx, &mut rx, hop(vec!["ABSENT".into()]));
    let nodes: Vec<String> = serde_json::from_value(payload_value(&payload)).unwrap();
    assert!(
        !nodes.iter().any(|n| n != "a"),
        "no edge may be followed: {nodes:?}"
    );
}

#[test]
fn neighbors_multi_follows_every_listed_label() {
    let (mut core, mut tx, mut rx, _dir) = label_set_graph();
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::NeighborsMulti {
            node_ids: vec!["a".into(), "b".into()],
            edge_labels: knows_or_works(),
            direction: Direction::Out,
            max_results: 0,
            rls_filters: Vec::new(),
            collection: None,
            edge_predicate: Vec::new(),
            with_properties: false,
        }),
    );
    let rows: Vec<serde_json::Value> = serde_json::from_value(payload_value(&payload)).unwrap();
    let mut triples: Vec<(String, String, String)> = rows
        .iter()
        .map(|row| {
            (
                row["src"].as_str().unwrap().to_string(),
                row["label"].as_str().unwrap().to_string(),
                row["node"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    triples.sort();
    assert_eq!(
        triples,
        vec![
            ("a".into(), "KNOWS".into(), "b".into()),
            ("a".into(), "WORKS".into(), "c".into()),
            ("b".into(), "WORKS".into(), "e".into()),
        ]
    );
}

#[test]
fn path_and_subgraph_follow_every_listed_label() {
    let (mut core, mut tx, mut rx, _dir) = label_set_graph();
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Path {
            src: "a".into(),
            dst: "e".into(),
            edge_labels: knows_or_works(),
            max_depth: 5,
            options: Default::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: None,
            collection: None,
        }),
    );
    let path: Vec<String> = serde_json::from_value(payload_value(&payload)).unwrap();
    assert_eq!(path, vec!["a", "b", "e"]);

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Subgraph {
            start_nodes: vec!["a".into()],
            edge_labels: knows_or_works(),
            depth: 2,
            options: Default::default(),
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    let edges: Vec<serde_json::Value> = serde_json::from_value(payload_value(&payload)).unwrap();
    let mut labels: Vec<String> = edges
        .iter()
        .map(|edge| edge["label"].as_str().unwrap().to_string())
        .collect();
    labels.sort();
    assert_eq!(labels, vec!["KNOWS", "WORKS", "WORKS"]);
}
