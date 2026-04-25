//! Cross-tenant isolation: Graph engine.
//!
//! Tenant A inserts edges. Tenant B queries neighbors — must get zero results.

use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::GraphOp;

use crate::helpers::*;

const TENANT_A: u32 = 10;
const TENANT_B: u32 = 20;

#[test]
fn graph_neighbors_isolated() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A inserts edges.
    for i in 0..5u32 {
        send_ok_as_tenant(
            &mut core,
            &mut tx,
            &mut rx,
            TENANT_A,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: "col".into(),
                src_id: format!("node_{i}"),
                label: "FOLLOWS".into(),
                dst_id: format!("node_{}", i + 1),
                properties: vec![],
                src_surrogate: nodedb_types::Surrogate::ZERO,
                dst_surrogate: nodedb_types::Surrogate::ZERO,
            }),
        );
    }

    // Tenant A can query neighbors.
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "node_2".into(),
            edge_label: Some("FOLLOWS".into()),
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);
    let json_a = payload_json(&resp_a.payload);
    assert!(
        json_a.contains("node_3"),
        "Tenant A should see node_2→node_3"
    );

    // Tenant B queries the same node — must get zero results.
    let resp_b = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "node_2".into(),
            edge_label: Some("FOLLOWS".into()),
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp_b.status, Status::Ok);
    let json_b = payload_json(&resp_b.payload);
    let val: serde_json::Value =
        serde_json::from_str(&json_b).unwrap_or(serde_json::Value::Array(vec![]));
    let empty = vec![];
    let arr = val.as_array().unwrap_or(&empty);
    assert!(
        arr.is_empty(),
        "Tenant B graph query must return 0 results, got {}: {json_b}",
        arr.len()
    );
}
