// SPDX-License-Identifier: BUSL-1.1

//! Committed transactions over the document engine: atomic commit, the
//! refusal of a plan batch, and a refused transaction that commits nothing.
//!
//! Transactions that span the graph and vector engines live in
//! `test_transaction_cross_engine`.

use nodedb::bridge::envelope::ErrorCode;
use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{DocumentOp, MetaOp, PhysicalPlan};
use nodedb_test_support::tx_batch_helpers::{commit_plans, with_unique_refusal};

use super::helpers::*;

#[test]
fn a_transaction_commits_atomically() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "docs",
                ),
                document_id: "d1".into(),
                value: b"{\"name\":\"alice\"}".to_vec(),
                surrogate: nodedb_types::Surrogate::new(1),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "docs",
                ),
                document_id: "d2".into(),
                value: b"{\"name\":\"bob\"}".to_vec(),
                surrogate: nodedb_types::Surrogate::new(2),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        ],
        10,
    );
    assert_eq!(resp.status, Status::Ok);

    // Verify both documents exist via PointGet.
    let r1 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(r1.status, Status::Ok);

    let r2 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d2".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::new(2),
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(r2.status, Status::Ok);
}

/// A plan batch carries no redo record restart replay reads, so an Origin
/// core refuses it, answering under the request's own id.
#[test]
fn a_transaction_batch_is_refused_on_an_origin_core() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    tx.try_push(nodedb::bridge::dispatch::BridgeRequest::unfloored(
        make_request_with_id(
            42,
            PhysicalPlan::Meta(MetaOp::TransactionBatch {
                txn_id: None,
                plans: vec![PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: nodedb_types::QualifiedCollection::new(
                        nodedb_types::DatabaseId::DEFAULT,
                        "docs",
                    ),
                    document_id: "d1".into(),
                    value: b"{\"name\":\"alice\"}".to_vec(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                })],
            }),
        ),
    ))
    .unwrap();
    core.tick();

    let resp = rx.try_pop().unwrap().inner;
    assert_eq!(resp.status, Status::Error);
    assert!(matches!(
        resp.error_code.as_deref(),
        Some(ErrorCode::Unsupported { .. })
    ));
    assert_eq!(resp.request_id, nodedb::types::RequestId::new(42));
}

#[test]
fn a_refused_transaction_commits_nothing() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-insert d1 via SPSC.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".into(),
            value: b"original".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Transaction: overwrite d1, then a refused insert.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".into(),
            value: b"{\"name\":\"modified\"}".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        })]),
        20,
    );
    assert_eq!(resp.status, Status::Error);

    // d1 keeps its original value.
    let r = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "d1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"original");
}
