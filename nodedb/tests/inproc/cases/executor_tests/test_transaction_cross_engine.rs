// SPDX-License-Identifier: BUSL-1.1

//! Committed transactions spanning more than one engine.
//!
//! A graph edge written inside a transaction must commit with the document
//! writes beside it, and a refused transaction must commit none of them — a
//! surviving edge after a refused transaction is state no read path can
//! account for.

use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan};
use nodedb_test_support::tx_batch_helpers::{commit_plans, with_unique_refusal};

use super::helpers::*;

#[test]
fn transaction_edge_put_committed() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-insert source and destination nodes.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "nodes",
            ),
            document_id: "alice".into(),
            value: b"{\"name\":\"alice\"}".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "nodes",
            ),
            document_id: "bob".into(),
            value: b"{\"name\":\"bob\"}".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Transaction: insert doc + edge.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "nodes",
                ),
                document_id: "carol".into(),
                value: b"{\"name\":\"carol\"}".to_vec(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "col",
                ),
                src_id: "alice".into(),
                label: "KNOWS".into(),
                dst_id: "bob".into(),
                properties: Vec::new(),
                src_surrogate: nodedb_types::Surrogate::ZERO,
                dst_surrogate: nodedb_types::Surrogate::ZERO,
            }),
        ],
        10,
    );
    assert_eq!(resp.status, Status::Ok);

    // Verify edge exists via Neighbors.
    let n = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "alice".into(),
            edge_label: Some("KNOWS".into()),
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    assert_eq!(n.status, Status::Ok);
    assert!(!n.payload.is_empty());
}

#[test]
fn a_refused_transaction_leaves_no_edge() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-insert nodes.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "nodes",
            ),
            document_id: "alice".into(),
            value: b"{\"name\":\"alice\"}".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "nodes",
            ),
            document_id: "bob".into(),
            value: b"{\"name\":\"bob\"}".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Transaction: edge put, then a refused insert.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "col",
            ),
            src_id: "alice".into(),
            label: "KNOWS".into(),
            dst_id: "bob".into(),
            properties: Vec::new(),
            src_surrogate: nodedb_types::Surrogate::ZERO,
            dst_surrogate: nodedb_types::Surrogate::ZERO,
        })]),
        20,
    );
    assert_eq!(resp.status, Status::Error);

    // The edge never landed: neighbors are empty.
    let n = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "alice".into(),
            edge_label: Some("KNOWS".into()),
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    assert_eq!(n.status, Status::Ok);
    // Payload should be empty array (no neighbors).
    let payload = &*n.payload;
    // Deserialize: either empty msgpack array or empty JSON array.
    // Empty result = msgpack empty array [0x90] or very short payload.
    assert!(
        payload.len() <= 3,
        "the edge must not land, but payload len: {}",
        payload.len()
    );
}

#[test]
fn a_refused_transaction_leaves_neither_doc_nor_edge() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-insert nodes.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "nodes",
            ),
            document_id: "n1".into(),
            value: b"original_n1".to_vec(),
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: b"n1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "nodes",
            ),
            document_id: "n2".into(),
            value: b"original_n2".to_vec(),
            surrogate: nodedb_types::Surrogate::new(2),
            pk_bytes: b"n2".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Transaction: doc update + edge put, then a refused insert. Neither lands.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "nodes",
                ),
                document_id: "n1".into(),
                value: b"modified_n1".to_vec(),
                surrogate: nodedb_types::Surrogate::new(1),
                pk_bytes: b"n1".to_vec(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "col",
                ),
                src_id: "n1".into(),
                label: "LINKED".into(),
                dst_id: "n2".into(),
                properties: Vec::new(),
                src_surrogate: nodedb_types::Surrogate::ZERO,
                dst_surrogate: nodedb_types::Surrogate::ZERO,
            }),
        ]),
        30,
    );
    assert_eq!(resp.status, Status::Error);

    // The document keeps its original value.
    let r = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "nodes",
            ),
            document_id: "n1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: b"n1".to_vec(),
        }),
    );
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"original_n1");

    // The edge never landed (no neighbors).
    let n = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "n1".into(),
            edge_label: Some("LINKED".into()),
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    assert_eq!(n.status, Status::Ok);
    // Empty result = msgpack empty array [0x90] or very short payload.
    assert!(n.payload.len() <= 3, "the edge must not land");
}
