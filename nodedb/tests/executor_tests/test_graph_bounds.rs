//! Graph traversal boundary tests.
//!
//! Verifies that BFS, shortest path, and subgraph materialization
//! respect bounded depth and fan-out limits under adversarial queries.

use nodedb::bridge::envelope::{ErrorCode, PhysicalPlan};
use nodedb::bridge::physical_plan::GraphOp;
use nodedb::engine::graph::edge_store::Direction;

use crate::helpers::*;

#[test]
fn graph_traversal_bounded_under_adversarial_queries() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Create a supernode: node "hub" connected to 200 other nodes.
    for i in 0..200u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: "col".into(),
                src_id: "hub".into(),
                label: "LINKS".into(),
                dst_id: format!("n{i}"),
                properties: vec![],
                src_surrogate: nodedb_types::Surrogate::ZERO,
                dst_surrogate: nodedb_types::Surrogate::ZERO,
            }),
        );
    }

    // Create a long chain: c0→c1→c2→...→c50.
    for i in 0..50u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: "col".into(),
                src_id: format!("c{i}"),
                label: "NEXT".into(),
                dst_id: format!("c{}", i + 1),
                properties: vec![],
                src_surrogate: nodedb_types::Surrogate::ZERO,
                dst_surrogate: nodedb_types::Surrogate::ZERO,
            }),
        );
    }

    // Test 1: BFS with depth limit = 1 from supernode.
    // Should return hub + 200 neighbors = 201 nodes max.
    let hop1 = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["hub".into()],
            edge_label: None,
            direction: Direction::Out,
            depth: 1,
            options: Default::default(),
            rls_filters: Vec::new(),
        }),
    );
    let hop1_nodes: Vec<String> = serde_json::from_value(payload_value(&hop1)).unwrap();
    assert!(
        hop1_nodes.len() <= 201,
        "depth=1 should cap at hub+neighbors: got {}",
        hop1_nodes.len()
    );
    assert!(hop1_nodes.contains(&"hub".to_string()));

    // Test 2: BFS with depth limit = 0 should return only start nodes.
    let hop0 = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["hub".into()],
            edge_label: None,
            direction: Direction::Out,
            depth: 0,
            options: Default::default(),
            rls_filters: Vec::new(),
        }),
    );
    let hop0_nodes: Vec<String> = serde_json::from_value(payload_value(&hop0)).unwrap();
    assert_eq!(hop0_nodes.len(), 1, "depth=0 should return only start node");

    // Test 3: Chain traversal with limited depth.
    // c0→c1→...→c50, depth=5 should reach c0-c5 only.
    let chain = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["c0".into()],
            edge_label: Some("NEXT".into()),
            direction: Direction::Out,
            depth: 5,
            options: Default::default(),
            rls_filters: Vec::new(),
        }),
    );
    let chain_nodes: Vec<String> = serde_json::from_value(payload_value(&chain)).unwrap();
    assert!(
        chain_nodes.len() <= 6,
        "depth=5 from c0 should reach at most 6 nodes: got {}",
        chain_nodes.len()
    );
    assert!(chain_nodes.contains(&"c0".to_string()));
    assert!(chain_nodes.contains(&"c5".to_string()));

    // Test 4: Shortest path with depth limit prevents deep search.
    let path_short = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Path {
            src: "c0".into(),
            dst: "c50".into(),
            edge_label: Some("NEXT".into()),
            max_depth: 3,
            options: Default::default(),
            rls_filters: Vec::new(),
        }),
    );
    // c0→c50 is 50 hops, depth limit 3 should fail.
    assert_eq!(
        path_short.error_code,
        Some(ErrorCode::NotFound),
        "50-hop path should not be found with max_depth=3"
    );

    // Test 5: Subgraph materialization bounded by depth.
    let subgraph = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Subgraph {
            start_nodes: vec!["c0".into()],
            edge_label: Some("NEXT".into()),
            depth: 3,
            options: Default::default(),
            rls_filters: Vec::new(),
        }),
    );
    let edges: Vec<serde_json::Value> = serde_json::from_value(payload_value(&subgraph)).unwrap();
    // depth=3 from c0: edges c0→c1, c1→c2, c2→c3 = 3 edges.
    assert_eq!(
        edges.len(),
        3,
        "depth=3 subgraph should have 3 edges: got {}",
        edges.len()
    );
}
