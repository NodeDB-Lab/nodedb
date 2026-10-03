// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: Graph engine.
//!
//! Tenant A inserts edges. Tenant B queries neighbors — must get zero results.

use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::helpers::*;

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
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "col"),
                src_id: format!("node_{i}"),
                label: "FOLLOWS".into(),
                dst_id: format!("node_{}", i + 1),
                properties: vec![],
                src_surrogate: doc_surrogate(&format!("node_{i}")),
                dst_surrogate: doc_surrogate(&format!("node_{}", i + 1)),
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
            edge_labels: vec!["FOLLOWS".into()],
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
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
            edge_labels: vec!["FOLLOWS".into()],
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
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
