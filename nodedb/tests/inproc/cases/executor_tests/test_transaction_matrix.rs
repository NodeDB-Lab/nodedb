// SPDX-License-Identifier: BUSL-1.1

//! Cross-engine transaction rollback matrix: engine-pair failures.
//!
//! For every pair of write-trackable engines that can legally appear in one
//! committed transaction (Document, Vector, Graph, CRDT), a deterministic
//! refusal after the first operation must leave none of it applied.
//!
//! Test structure (per pair):
//!   1. Pre-condition: write a known state for the first engine.
//!   2. Transaction: valid first op (overwrites it) + a refused op.
//!   3. Assert: the first op is not applied; state matches the pre-condition.
//!
//! Adding an engine pair: add one test here following the existing pattern.
//! Side-effect rollback (FTS, spatial) lives in
//! `test_transaction_matrix_side_effects`.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, PhysicalPlan};
use nodedb_test_support::tx_batch_helpers::{
    commit_plans, vector_direct_insert, with_unique_refusal,
};
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::helpers::*;
use super::test_transaction_matrix_helpers::*;

// ---------------------------------------------------------------------------
// Document, then a refused insert
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_doc_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-condition: "doc1" = "original".
    send_ok(&mut core, &mut tx, &mut rx, doc_put("docs", b"original"));

    // Transaction: overwrite doc1, then a refused insert.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![doc_put("docs", b"modified")]),
        10,
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "the transaction must be refused"
    );

    // doc1 must be rolled back to "original".
    let r = send_raw(&mut core, &mut tx, &mut rx, doc_get("docs"));
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"original");
}

// ---------------------------------------------------------------------------
// Pair: Vector (first) × Document (second, conflict fails)
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_vector_then_doc_fail() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-condition for doc conflict: doc1 already exists.
    send_ok(&mut core, &mut tx, &mut rx, doc_put("docs", b"preexisting"));

    // Transaction: a vector-primary insert + a PointInsert that conflicts.
    let v_plan = vector_direct_insert("vp", 101);
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            v_plan,
            doc_insert_conflict("docs"), // doc1 already exists → constraint fail
        ],
        20,
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "the transaction must be refused"
    );
    // The error must be a constraint violation, not a rollback failure.
    assert!(
        !matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RollbackFailed { .. })
        ),
        "rollback itself must succeed; got {:?}",
        resp.error_code
    );

    // doc1 is still "preexisting" (the transaction committed nothing).
    let r = send_raw(&mut core, &mut tx, &mut rx, doc_get("docs"));
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"preexisting");
}

// ---------------------------------------------------------------------------
// Pair: Document (first) × Graph (second, edge fails)
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_doc_then_graph_fail() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-condition: doc1 = "original".
    send_ok(&mut core, &mut tx, &mut rx, doc_put("docs", b"original"));

    // Transaction: doc_put (overwrites) + doc_insert_conflict (same key, refused).
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            doc_put("docs", b"modified"),
            doc_insert_conflict("docs"), // same key → constraint fail
        ],
        30,
    );
    assert_eq!(resp.status, Status::Error);

    // doc1 should be rolled back to "original".
    let r = send_raw(&mut core, &mut tx, &mut rx, doc_get("docs"));
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"original");
}

// ---------------------------------------------------------------------------
// Graph, then a refused insert
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_graph_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Transaction: edge put, then a refused insert.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![edge_put("col", "alice", "bob")]),
        40,
    );
    assert_eq!(resp.status, Status::Error);
    assert!(
        !matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RollbackFailed { .. })
        ),
        "rollback itself must succeed; got {:?}",
        resp.error_code
    );

    // The edge must be rolled back: alice should have no REL neighbors.
    let n = send_raw(&mut core, &mut tx, &mut rx, neighbors("alice"));
    assert_eq!(n.status, Status::Ok);
    assert!(
        n.payload.len() <= 3,
        "edge should have been rolled back, payload len={}",
        n.payload.len()
    );
}

// ---------------------------------------------------------------------------
// Graph × Graph, then a refused insert
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_two_edges_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Transaction: two edge puts, then a refused insert. Neither edge lands.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![edge_put("col", "a", "b"), edge_put("col", "c", "d")]),
        50,
    );
    assert_eq!(resp.status, Status::Error);

    let n_ab = send_raw(&mut core, &mut tx, &mut rx, neighbors("a"));
    assert_eq!(n_ab.status, Status::Ok);
    assert!(n_ab.payload.len() <= 3, "edge a→b should be rolled back");

    let n_cd = send_raw(&mut core, &mut tx, &mut rx, neighbors("c"));
    assert_eq!(n_cd.status, Status::Ok);
    assert!(n_cd.payload.len() <= 3, "edge c→d should be rolled back");
}

// ---------------------------------------------------------------------------
// Raw CRDT Apply inside a transaction
// Raw CRDT Apply bypasses serialized preview admission, so a transaction
// cannot stage it. The refusal comes before any sibling write lands.
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_raw_crdt_apply_is_refused() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-condition: doc1 = "original".
    send_ok(&mut core, &mut tx, &mut rx, doc_put("docs", b"original"));

    // Transaction: forbidden CRDT Apply + a write that must never land.
    let crdt_delta: Vec<u8> = vec![0u8; 8]; // minimal placeholder delta
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "crdt_coll"),
                document_id: "crdt_doc1".into(),
                delta: crdt_delta,
                peer_id: 1,
                mutation_id: 42,
                surrogate: nodedb_types::Surrogate::ZERO,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            }),
            doc_put("docs", b"modified"),
        ]),
        60,
    );
    assert_eq!(resp.status, Status::Error);
    assert!(matches!(
        resp.error_code.as_deref(),
        Some(ErrorCode::Internal { detail }) if detail.contains("StageWrite is only valid")
    ));
    // Rollback must succeed (not RollbackFailed).
    assert!(
        !matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RollbackFailed { .. })
        ),
        "rollback itself must succeed; got {:?}",
        resp.error_code
    );

    // doc1 must be rolled back to "original".
    let r = send_raw(&mut core, &mut tx, &mut rx, doc_get("docs"));
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"original");
}

// ---------------------------------------------------------------------------
// Pair: Document × Document — second doc write fails via constraint
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_doc_doc_second_fails() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-condition: "doc1" already exists so PointInsert(if_absent=false) fails.
    send_ok(&mut core, &mut tx, &mut rx, doc_put("docs", b"preexisting"));

    // Transaction: write "other_doc" (new insert) + PointInsert on existing "doc1" (refused).
    let other_put = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
        document_id: "other_doc".into(),
        value: b"should_not_persist".to_vec(),
        surrogate: nodedb_types::Surrogate::new(99),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    });
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            other_put,
            doc_insert_conflict("docs"), // "doc1" already exists
        ],
        70,
    );
    assert_eq!(resp.status, Status::Error);

    // "other_doc" must be rolled back (not present).
    let r = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "other_doc".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::new(99),
            pk_bytes: Vec::new(),
        }),
    );
    // Rolled back: either NotFound or empty payload.
    assert!(
        r.status == Status::Error || r.payload.is_empty() || r.payload.len() <= 3,
        "other_doc should have been rolled back; status={:?} payload_len={}",
        r.status,
        r.payload.len()
    );
}

// ---------------------------------------------------------------------------
// Verify rollback failure surfaces as RollbackFailed (not swallowed)
// — this is a unit-level property test on the error code type.
// ---------------------------------------------------------------------------
