// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: RLS — negative (cross-tenant manipulation) cases.
//!
//! Verifies that Tenant B cannot read, delete, or override Tenant A's RLS policies.
//! RLS policies are scoped by `(tenant_id, collection)` and must be completely
//! invisible to other tenants.

use super::helpers::{TENANT_A, TENANT_B};
use nodedb::control::security::audit::NoopAuditEmitter;
use nodedb::control::security::auth_context::AuthContext;
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use nodedb::control::security::predicate::{CompareOp, PolicyMode, PredicateValue, RlsPredicate};
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

fn status_policy(tenant_id: u64, name: &str) -> RlsPolicy {
    RlsPolicy {
        name: name.into(),
        collection: "orders".into(),
        display_collection: "orders".into(),
        tenant_id,
        policy_type: PolicyType::Write,
        compiled_predicate: Some(RlsPredicate::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: PredicateValue::Literal(serde_json::json!("approved")),
        }),
        mode: PolicyMode::default(),
        on_deny: Default::default(),
        enabled: true,
        created_by: "admin".into(),
        created_at: 0,
    }
}

/// Tenant B cannot see Tenant A's policies through listing.
#[test]
fn rls_tenant_b_cannot_list_tenant_a_policies() {
    let store = RlsPolicyStore::new();

    store
        .create_policy(status_policy(TENANT_A, "a_policy"))
        .unwrap();

    let policies_b = store.all_policies(TENANT_B, "orders");
    assert!(
        policies_b.is_empty(),
        "Tenant B must not see Tenant A's RLS policies; got {} policies",
        policies_b.len()
    );
}

/// Tenant B creating a policy with the same name as Tenant A's policy must be
/// scoped to B only — it must not overwrite A's policy.
#[test]
fn rls_same_name_policy_in_different_tenants_is_isolated() {
    let store = RlsPolicyStore::new();

    // Both tenants create a policy with the identical name.
    store
        .create_policy(status_policy(TENANT_A, "shared_name"))
        .unwrap();
    store
        .create_policy(RlsPolicy {
            name: "shared_name".into(),
            collection: "orders".into(),
            display_collection: "orders".into(),
            tenant_id: TENANT_B,
            policy_type: PolicyType::Read,
            compiled_predicate: None,
            mode: PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 0,
        })
        .unwrap();

    // Tenant A's policy must still be of type Write.
    let policies_a = store.all_policies(TENANT_A, "orders");
    assert_eq!(policies_a.len(), 1);
    assert_eq!(policies_a[0].policy_type, PolicyType::Write);

    // Tenant B's policy must be of type Read.
    let policies_b = store.all_policies(TENANT_B, "orders");
    assert_eq!(policies_b.len(), 1);
    assert_eq!(policies_b[0].policy_type, PolicyType::Read);
}

/// Tenant A's policy blocks Tenant A's writes; Tenant B is unaffected (no policy).
/// Then a new policy is created for Tenant B.  After that, each tenant must be
/// independently controlled by its own policy.
#[test]
fn rls_policies_enforce_independently_per_tenant() {
    let store = RlsPolicyStore::new();

    // Tenant A: requires status=approved.
    store
        .create_policy(status_policy(TENANT_A, "a_gate"))
        .unwrap();

    let pending = serde_json::json!({"status": "pending", "amount": 50});
    let approved = serde_json::json!({"status": "approved", "amount": 50});

    // Tenant A blocked on pending.
    assert!(
        store
            .check_write_with_auth(
                TENANT_A,
                "orders",
                &pending,
                &make_auth(TENANT_A),
                NOOP,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_err(),
        "Tenant A's RLS must block pending write"
    );

    // Tenant B has no policy yet — write allowed regardless of status.
    assert!(
        store
            .check_write_with_auth(
                TENANT_B,
                "orders",
                &pending,
                &make_auth(TENANT_B),
                NOOP,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_ok(),
        "Tenant B must be unrestricted before its own policy is created"
    );

    // Now add a policy for Tenant B that requires status=approved too.
    store
        .create_policy(status_policy(TENANT_B, "b_gate"))
        .unwrap();

    // Tenant B is now blocked on pending.
    assert!(
        store
            .check_write_with_auth(
                TENANT_B,
                "orders",
                &pending,
                &make_auth(TENANT_B),
                NOOP,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_err(),
        "Tenant B's own RLS must block pending write after b_gate is created"
    );

    // Tenant A is still blocked on pending (unchanged).
    assert!(
        store
            .check_write_with_auth(
                TENANT_A,
                "orders",
                &pending,
                &make_auth(TENANT_A),
                NOOP,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_err(),
        "Tenant A's RLS must still block pending write"
    );

    // Both tenants allow approved writes.
    assert!(
        store
            .check_write_with_auth(
                TENANT_A,
                "orders",
                &approved,
                &make_auth(TENANT_A),
                NOOP,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_ok()
    );
    assert!(
        store
            .check_write_with_auth(
                TENANT_B,
                "orders",
                &approved,
                &make_auth(TENANT_B),
                NOOP,
                &nodedb_crs::CrsContext::builtin_only()
            )
            .is_ok()
    );
}
