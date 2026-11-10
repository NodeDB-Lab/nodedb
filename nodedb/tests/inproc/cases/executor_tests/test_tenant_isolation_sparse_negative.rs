// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: Document (sparse) engine — negative (write-collision) cases.
//!
//! Verifies that Tenant B writing to the same collection + document_id as Tenant A
//! cannot overwrite or delete Tenant A's document.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

use super::helpers::*;

/// Tenant B PointPut on the same doc_id must not overwrite Tenant A's document.
#[test]
fn sparse_cross_tenant_put_does_not_overwrite() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A inserts a document.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "profiles",
            ),
            document_id: "user_1".into(),
            value: b"{\"name\":\"alice\",\"secret\":\"tenant_a_secret\"}".to_vec(),
            surrogate: doc_surrogate("user_1"),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }),
    );

    // Tenant B puts a document with the same collection + doc_id.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "profiles",
            ),
            document_id: "user_1".into(),
            value: b"{\"name\":\"bob\",\"secret\":\"tenant_b_secret\"}".to_vec(),
            surrogate: doc_surrogate("user_1"),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }),
    );

    // Tenant A's PointGet must still return the original document.
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "profiles",
            ),
            document_id: "user_1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: Some(doc_surrogate("user_1")),
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);
    let json_a = payload_json(&resp_a.payload);
    assert!(
        json_a.contains("tenant_a_secret"),
        "Tenant A's document must be intact after Tenant B's cross-tenant Put; got: {json_a}"
    );
    assert!(
        !json_a.contains("tenant_b_secret"),
        "Tenant B's data must NOT appear in Tenant A's document; got: {json_a}"
    );
}

/// Tenant B PointDelete on a doc_id that Tenant A owns must not affect Tenant A.
#[test]
fn sparse_cross_tenant_delete_does_not_affect_owner() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A inserts a document.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "records",
            ),
            document_id: "rec_42".into(),
            value: b"{\"data\":\"confidential\"}".to_vec(),
            surrogate: doc_surrogate("rec_42"),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }),
    );

    // Tenant B attempts to delete the same document.  In B's namespace
    // the document doesn't exist, so the engine must either return Ok
    // (0 rows deleted) or NotFound.
    let resp_del = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "records",
            ),
            document_id: "rec_42".into(),
            surrogate: Some(doc_surrogate("rec_42")),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        }),
    );
    let ok_or_not_found = resp_del.status == Status::Ok
        || resp_del.error_code.as_deref() == Some(&ErrorCode::NotFound);
    assert!(
        ok_or_not_found,
        "Cross-tenant document delete must be Ok or NotFound, got {:?}",
        resp_del.error_code
    );

    // Tenant A's document must still be present.
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "records",
            ),
            document_id: "rec_42".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: Some(doc_surrogate("rec_42")),
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);
    let json_a = payload_json(&resp_a.payload);
    assert!(
        json_a.contains("confidential"),
        "Tenant A's document must survive Tenant B's cross-tenant delete; got: {json_a}"
    );
}
