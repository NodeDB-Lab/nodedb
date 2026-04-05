//! Cross-tenant isolation: RLS policies.
//!
//! Tenant A's RLS policies must be invisible to Tenant B.
//! RLS policies are scoped by `(tenant_id, collection)` by construction.

use nodedb::control::security::rls::{PolicyType, RlsPolicy, RlsPolicyStore};

const TENANT_A: u32 = 10;
const TENANT_B: u32 = 20;

#[test]
fn rls_policies_isolated_between_tenants() {
    let store = RlsPolicyStore::new();

    // Create a restrictive write policy for Tenant A on "orders".
    let filter = nodedb::bridge::scan_filter::ScanFilter {
        field: "status".into(),
        op: "eq".into(),
        value: serde_json::json!("approved"),
        clauses: Vec::new(),
    };
    let predicate = zerompk::to_msgpack_vec(&vec![filter]).unwrap();

    store
        .create_policy(RlsPolicy {
            name: "require_approved".into(),
            collection: "orders".into(),
            tenant_id: TENANT_A,
            policy_type: PolicyType::Write,
            predicate,
            compiled_predicate: None,
            mode: nodedb::control::security::predicate::PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 0,
        })
        .unwrap();

    // Tenant A's write on "orders" with status=pending → BLOCKED by RLS.
    let pending_doc = serde_json::json!({"status": "pending", "amount": 100});
    let result_a = store.check_write(TENANT_A, "orders", &pending_doc, "user1");
    assert!(
        result_a.is_err(),
        "Tenant A's RLS should block pending writes"
    );

    // Tenant B's write on "orders" with status=pending → ALLOWED (no policy for Tenant B).
    let result_b = store.check_write(TENANT_B, "orders", &pending_doc, "user1");
    assert!(
        result_b.is_ok(),
        "Tenant B has no RLS policy — write should be allowed"
    );

    // Tenant A's approved write → allowed.
    let approved_doc = serde_json::json!({"status": "approved", "amount": 200});
    assert!(
        store
            .check_write(TENANT_A, "orders", &approved_doc, "user1")
            .is_ok()
    );
}

#[test]
fn rls_policy_listing_scoped() {
    let store = RlsPolicyStore::new();

    // Create policies for different tenants.
    for (tid, name) in [(TENANT_A, "policy_a"), (TENANT_B, "policy_b")] {
        let filter = nodedb::bridge::scan_filter::ScanFilter {
            field: "role".into(),
            op: "eq".into(),
            value: serde_json::json!("admin"),
            clauses: Vec::new(),
        };
        store
            .create_policy(RlsPolicy {
                name: name.into(),
                collection: "users".into(),
                tenant_id: tid,
                policy_type: PolicyType::Read,
                predicate: zerompk::to_msgpack_vec(&vec![filter]).unwrap(),
                compiled_predicate: None,
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
