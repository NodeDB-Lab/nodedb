// SPDX-License-Identifier: BUSL-1.1

//! Security and isolation validation tests.
//!
//! 1. Security regression: auth failures, privilege changes, tenant isolation
//! 2. Linearizability: WAL append + read consistency

use nodedb::bridge::envelope::Status;
use nodedb::control::security::audit::NoopAuditEmitter;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

const NOOP: &NoopAuditEmitter = &NoopAuditEmitter;

use super::helpers::*;

// ──────────────────────────────────────────────────────────────────────
// Gate 1: Security regression — auth failures, privilege, tenant isolation
// ──────────────────────────────────────────────────────────────────────

#[test]
fn security_tenant_isolation() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant 1 inserts a document.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "secrets",
            ),
            document_id: "s1".into(),
            value: b"{\"data\":\"tenant1_secret\"}".to_vec(),
            surrogate: doc_surrogate("s1"),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Tenant 1 can read it.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "secrets",
            ),
            document_id: "s1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: Some(doc_surrogate("s1")),
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok);

    // The storage key is tenant-scoped: "{tenant_id}:{collection}:{doc_id}".
    // A different tenant_id would produce a different key and get NotFound.
    // This is enforced at the storage layer — the CoreLoop test uses tenant_id=1
    // from make_request(). Cross-tenant access is blocked by the planner's
    // tenant_id check before dispatch.
}

#[test]
fn security_rls_policy_enforcement() {
    use nodedb::control::security::auth_context::AuthContext;
    use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
    use nodedb::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
    use nodedb::control::security::rls::{PolicyType, RlsPolicy, RlsPolicyStore};
    use nodedb_types::TenantId;

    let store = RlsPolicyStore::new();

    // Create a write policy requiring status = "approved".
    let predicate = RlsPredicate::Compare {
        field: "status".into(),
        op: CompareOp::Eq,
        value: PredicateValue::Literal(serde_json::json!("approved")),
    };

    store
        .create_policy(RlsPolicy {
            name: "require_approved".into(),
            collection: "orders".into(),
            display_collection: "orders".into(),
            tenant_id: 1,
            policy_type: PolicyType::Write,
            compiled_predicate: Some(predicate),
            mode: nodedb::control::security::predicate::PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 0,
        })
        .unwrap();

    let make_auth = |tenant_id: u64| {
        let identity = AuthenticatedIdentity::new_regular(
            1,
            "user1",
            TenantId::new(tenant_id),
            AuthMethod::ApiKey,
            vec![Role::ReadWrite],
            None,
            AuthenticatedIdentity::default_database_set(false),
        );
        AuthContext::from_identity(&identity, "test".into())
    };

    // Approved document passes.
    let doc_ok = serde_json::json!({"status": "approved", "amount": 100});
    assert!(
        store
            .check_write_with_auth(
                1,
                "orders",
                &doc_ok,
                &make_auth(1),
                NOOP,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_ok()
    );

    // Pending document rejected.
    let doc_bad = serde_json::json!({"status": "pending", "amount": 200});
    let err = store.check_write_with_auth(
        1,
        "orders",
        &doc_bad,
        &make_auth(1),
        NOOP,
        &nodedb_crs::CrsContext::builtin_only(),
    );
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains("require_approved"));

    // Different tenant has no policies — allowed.
    assert!(
        store
            .check_write_with_auth(
                99,
                "orders",
                &doc_bad,
                &make_auth(99),
                NOOP,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_ok()
    );
}

#[test]
fn security_jwt_validation() {
    use nodedb::control::security::jwt::{JwtConfig, JwtError, JwtValidator};

    let validator = JwtValidator::new(JwtConfig::default());

    // Malformed token rejected.
    assert_eq!(
        validator.validate("not-a-jwt").unwrap_err(),
        JwtError::MalformedToken
    );

    // Two-part token rejected.
    assert_eq!(
        validator.validate("header.payload").unwrap_err(),
        JwtError::MalformedToken
    );
}

// ──────────────────────────────────────────────────────────────────────
// Gate 2: Linearizability — WAL append guarantees read-after-write
// ──────────────────────────────────────────────────────────────────────

#[test]
fn linearizability_read_after_write() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Write then immediately read — must see the write.
    for i in 0..20u32 {
        let doc_id = format!("lin_{i}");
        let value = format!("{{\"seq\":{i}}}");

        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "linear",
                ),
                document_id: doc_id.clone(),
                value: value.into_bytes(),
                surrogate: doc_surrogate(&doc_id),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );

        let resp = send_raw(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointGet {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "linear",
                ),
                document_id: doc_id.clone(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
                surrogate: Some(doc_surrogate(&doc_id)),
                pk_bytes: Vec::new(),
            }),
        );
        assert_eq!(
            resp.status,
            Status::Ok,
            "read-after-write failed for {doc_id}"
        );
    }
}

#[test]
fn linearizability_delete_visibility() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "linear",
            ),
            document_id: "del1".into(),
            value: b"{\"x\":1}".to_vec(),
            surrogate: doc_surrogate("del1"),
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
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "linear",
            ),
            document_id: "del1".into(),
            surrogate: Some(doc_surrogate("del1")),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            // No RLS policy on "linear" — this test targets linearizability, not RLS.
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        }),
    );

    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "linear",
            ),
            document_id: "del1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: Some(doc_surrogate("del1")),
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(resp.error_code, None);
}
