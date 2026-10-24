// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: RLS policies.
//!
//! Tenant A's RLS policies must be invisible to Tenant B.
//! RLS policies are scoped by `(tenant_id, collection)` by construction.

use super::helpers::{TENANT_A, TENANT_B};
use nodedb::control::security::audit::NoopAuditEmitter;
use nodedb::control::security::auth_context::AuthContext;
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use nodedb::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
use nodedb::control::security::rls::{PolicyType, RlsPolicy, RlsPolicyStore};
use nodedb_types::TenantId;

const NOOP: &NoopAuditEmitter = &NoopAuditEmitter;

fn make_auth(tenant_id: u64) -> AuthContext {
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
}

#[test]
fn rls_policies_isolated_between_tenants() {
    let store = RlsPolicyStore::new();

    // Create a restrictive write policy for Tenant A on "orders".
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
            tenant_id: TENANT_A,
            policy_type: PolicyType::Write,
            compiled_predicate: Some(predicate),
            mode: nodedb::control::security::predicate::PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 0,
        })
        .unwrap();

    // Tenant A's write on "orders" with status=pending → BLOCKED by RLS.
    let pending_doc = serde_json::json!({"status": "pending", "amount": 100});
    let result_a = store.check_write_with_auth(
        TENANT_A,
        "orders",
        &pending_doc,
        &make_auth(TENANT_A),
        NOOP,
        &nodedb_crs::CrsContext::builtin_only(),
    );
    assert!(
        result_a.is_err(),
        "Tenant A's RLS should block pending writes"
    );

    // Tenant B's write on "orders" with status=pending → ALLOWED (no policy for Tenant B).
    let result_b = store.check_write_with_auth(
        TENANT_B,
        "orders",
        &pending_doc,
        &make_auth(TENANT_B),
        NOOP,
        &nodedb_crs::CrsContext::builtin_only(),
    );
    assert!(
        result_b.is_ok(),
        "Tenant B has no RLS policy — write should be allowed"
    );

    // Tenant A's approved write → allowed.
    let approved_doc = serde_json::json!({"status": "approved", "amount": 200});
    assert!(
        store
            .check_write_with_auth(
                TENANT_A,
                "orders",
                &approved_doc,
                &make_auth(TENANT_A),
                NOOP,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_ok()
    );
}

#[test]
fn rls_policy_listing_scoped() {
    let store = RlsPolicyStore::new();

    // Create policies for different tenants.
    for (tid, name) in [(TENANT_A, "policy_a"), (TENANT_B, "policy_b")] {
        let predicate = RlsPredicate::Compare {
            field: "role".into(),
            op: CompareOp::Eq,
            value: PredicateValue::Literal(serde_json::json!("admin")),
        };
        store
            .create_policy(RlsPolicy {
                name: name.into(),
                collection: "users".into(),
                display_collection: "users".into(),
                tenant_id: tid,
                policy_type: PolicyType::Read,
                compiled_predicate: Some(predicate),
                mode: nodedb::control::security::predicate::PolicyMode::default(),
                on_deny: Default::default(),
                enabled: true,
                created_by: "admin".into(),
                created_at: 0,
            })
            .unwrap();
    }

    // List policies for Tenant A — should only see Tenant A's policy.
    let policies_a = store.all_policies(TENANT_A, "users");
    assert_eq!(policies_a.len(), 1);
    assert_eq!(policies_a[0].name, "policy_a");

    // List policies for Tenant B — should only see Tenant B's policy.
    let policies_b = store.all_policies(TENANT_B, "users");
    assert_eq!(policies_b.len(), 1);
    assert_eq!(policies_b[0].name, "policy_b");
}
