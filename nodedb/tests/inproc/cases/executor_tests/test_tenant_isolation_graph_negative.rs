// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: Graph engine — negative (write-collision) cases.
//!
//! Verifies that Tenant B inserting edges with the same node IDs as Tenant A
//! cannot contaminate Tenant A's neighbor results.  After Tenant B's inserts,
//! Tenant A's neighbor query must return exactly its own edges.

use nodedb::bridge::envelope::Status;
use nodedb::engine::graph::edge_store::Direction;
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::helpers::*;

/// Tenant B inserting edges with the same node IDs must not appear in Tenant A's
/// neighbor results.
#[test]
fn graph_cross_tenant_insert_does_not_contaminate_neighbors() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A creates a single edge: node_1 → node_2.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "social"),
            src_id: "node_1".into(),
            label: "FOLLOWS".into(),
            dst_id: "node_2".into(),
            properties: vec![],
            src_surrogate: doc_surrogate("node_1"),
            dst_surrogate: doc_surrogate("node_2"),
        }),
    );

    // Establish baseline: Tenant A sees exactly 1 neighbor for node_1.
    let resp_baseline = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "node_1".into(),
            edge_labels: vec!["FOLLOWS".into()],
            direction: Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    assert_eq!(resp_baseline.status, Status::Ok);
    let json_baseline = payload_json(&resp_baseline.payload);
    let baseline_count = serde_json::from_str::<serde_json::Value>(&json_baseline)
        .unwrap_or(serde_json::Value::Array(vec![]))
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert!(
        baseline_count >= 1,
        "Tenant A should have at least 1 neighbor before B's inserts"
    );

    // Tenant B inserts 10 edges from the same node_1 to various destinations.
    for i in 0..10u32 {
        send_ok_as_tenant(
            &mut core,
            &mut tx,
            &mut rx,
            TENANT_B,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "social"),
                src_id: "node_1".into(),
                label: "FOLLOWS".into(),
                dst_id: format!("b_node_{i}"),
                properties: vec![],
                src_surrogate: doc_surrogate("node_1"),
                dst_surrogate: doc_surrogate(&format!("b_node_{i}")),
            }),
        );
    }

    // Tenant A's neighbor query must return the same result as baseline.
    let resp_after = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "node_1".into(),
            edge_labels: vec!["FOLLOWS".into()],
            direction: Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    assert_eq!(resp_after.status, Status::Ok);
    let json_after = payload_json(&resp_after.payload);
    let after_count = serde_json::from_str::<serde_json::Value>(&json_after)
        .unwrap_or(serde_json::Value::Array(vec![]))
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    assert_eq!(
        after_count, baseline_count,
        "Tenant B's graph inserts must not contaminate Tenant A's neighbor results \
         (baseline={baseline_count}, after={after_count}); json={json_after}"
    );

    // Verify Tenant B's destination nodes are not visible in Tenant A's results.
    assert!(
        !json_after.contains("b_node_"),
        "Tenant B's destination nodes must NOT appear in Tenant A's neighbor results; got: {json_after}"
    );
}

/// Tenant B deleting an edge from node_1 must not affect Tenant A's graph.
#[test]
fn graph_cross_tenant_edge_delete_does_not_affect_owner() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A creates an edge.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "social"),
            src_id: "alpha".into(),
            label: "CONNECTED".into(),
            dst_id: "beta".into(),
            properties: vec![],
            src_surrogate: doc_surrogate("alpha"),
            dst_surrogate: doc_surrogate("beta"),
        }),
    );

    // Tenant B attempts to delete the same edge from its own (empty) namespace.
    let resp_del = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "social"),
            src_id: "alpha".into(),
            label: "CONNECTED".into(),
            dst_id: "beta".into(),
            src_surrogate: doc_surrogate("alpha"),
            dst_surrogate: doc_surrogate("beta"),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }),
    );
    // Ok (edge not present in B's namespace) or Error — both are acceptable.
    let _ = resp_del; // any response is fine; we only care that A is unaffected

    // Tenant A's neighbors must still include beta.
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "alpha".into(),
            edge_labels: vec!["CONNECTED".into()],
            direction: Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);
    let json_a = payload_json(&resp_a.payload);
    assert!(
        json_a.contains("beta"),
        "Tenant A's edge must survive Tenant B's cross-tenant delete; got: {json_a}"
    );
}
