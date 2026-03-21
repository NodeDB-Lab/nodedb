//! Row-Level Security (RLS) policies.
//!
//! RLS adds per-row access control: predicates injected into physical
//! plans as mandatory filters. Not bypassable by application code.
//!
//! ```sql
//! CREATE POLICY read_own ON users USING (doc_get('$.user_id') = current_user());
//! CREATE POLICY write_own ON users FOR INSERT WITH CHECK (doc_get('$.tenant_id') = current_tenant());
//! ```
//!
//! **Read path**: RLS predicates are injected into DocumentScan filters
//! before execution. The predicate is evaluated on every row — rows that
//! don't match are excluded from results.
//!
//! **Write path**: RLS write policies are checked before WAL append.
//! Rejected writes return `REJECTED_AUTHZ` with an audit trail.

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// A single RLS policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RlsPolicy {
    /// Policy name (unique per collection).
    pub name: String,
    /// Collection this policy applies to.
    pub collection: String,
    /// Tenant scope.
    pub tenant_id: u32,
    /// Policy type: read, write, or both.
    pub policy_type: PolicyType,
    /// Predicate expression as a serialized `ScanFilter`.
    /// Applied to each row during scan (read) or before WAL (write).
    ///
    /// The predicate can reference:
    /// - Document fields via `doc_get('$.field')`
    /// - Session variables via `current_user()`, `current_tenant()`
    /// - Literal values
    pub predicate: Vec<u8>,
    /// Whether this policy is enabled.
    pub enabled: bool,
    /// Creator username (for audit).
    pub created_by: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
}

/// Policy type: when the policy is evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyType {
    /// Applied on SELECT/read — filters rows the user can see.
    Read,
    /// Applied on INSERT/UPDATE/DELETE — blocks disallowed writes.
    Write,
    /// Applied on both read and write paths.
    All,
}

/// RLS policy store: manages policies per tenant+collection.
pub struct RlsPolicyStore {
    /// Key: `"{tenant_id}:{collection}"` → list of policies.
    policies: RwLock<HashMap<String, Vec<RlsPolicy>>>,
}

impl Default for RlsPolicyStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RlsPolicyStore {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
        }
    }

    /// Create or replace an RLS policy.
    pub fn create_policy(&self, policy: RlsPolicy) -> Result<(), String> {
        let key = format!("{}:{}", policy.tenant_id, policy.collection);
        let mut policies = self.policies.write().unwrap_or_else(|p| p.into_inner());
        let list = policies.entry(key).or_default();

        // Replace existing policy with same name, or add new.
        if let Some(existing) = list.iter_mut().find(|p| p.name == policy.name) {
            *existing = policy;
        } else {
            list.push(policy);
        }
        Ok(())
    }

    /// Drop an RLS policy.
    pub fn drop_policy(&self, tenant_id: u32, collection: &str, policy_name: &str) -> bool {
        let key = format!("{tenant_id}:{collection}");
        let mut policies = self.policies.write().unwrap_or_else(|p| p.into_inner());
        if let Some(list) = policies.get_mut(&key) {
            let before = list.len();
            list.retain(|p| p.name != policy_name);
            list.len() < before
        } else {
            false
        }
    }

    /// Get all enabled read policies for a tenant+collection.
    ///
    /// These predicates must be injected into DocumentScan filters
    /// before execution on the Data Plane.
    pub fn read_policies(&self, tenant_id: u32, collection: &str) -> Vec<RlsPolicy> {
        let key = format!("{tenant_id}:{collection}");
        let policies = self.policies.read().unwrap_or_else(|p| p.into_inner());
        policies
            .get(&key)
            .map(|list| {
                list.iter()
                    .filter(|p| {
                        p.enabled && matches!(p.policy_type, PolicyType::Read | PolicyType::All)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all enabled write policies for a tenant+collection.
    ///
    /// These predicates must be checked before WAL append for
    /// INSERT/UPDATE/DELETE operations.
    pub fn write_policies(&self, tenant_id: u32, collection: &str) -> Vec<RlsPolicy> {
        let key = format!("{tenant_id}:{collection}");
        let policies = self.policies.read().unwrap_or_else(|p| p.into_inner());
        policies
            .get(&key)
            .map(|list| {
                list.iter()
                    .filter(|p| {
                        p.enabled && matches!(p.policy_type, PolicyType::Write | PolicyType::All)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if a document passes all write policies for a collection.
    ///
    /// Returns `Ok(())` if the write is allowed, or `Err(reason)` with
    /// the policy name that rejected the write.
    pub fn check_write(
        &self,
        tenant_id: u32,
        collection: &str,
        document: &serde_json::Value,
        username: &str,
    ) -> Result<(), String> {
        let policies = self.write_policies(tenant_id, collection);
        if policies.is_empty() {
            return Ok(()); // No write policies → allow.
        }

        for policy in &policies {
            if !policy.predicate.is_empty() {
                // Deserialize predicate and evaluate against the document.
                let filters: Vec<crate::bridge::scan_filter::ScanFilter> =
                    match rmp_serde::from_slice(&policy.predicate) {
                        Ok(f) => f,
                        Err(e) => {
                            warn!(
                                policy = %policy.name,
                                error = %e,
                                "RLS write policy predicate deserialization failed"
                            );
                            continue;
                        }
                    };

                // Evaluate each filter against the document.
                let passes = filters.iter().all(|f| f.matches(document));
                if !passes {
                    info!(
                        policy = %policy.name,
                        %username,
                        %collection,
                        "RLS write policy rejected"
                    );
                    return Err(format!(
                        "write rejected by RLS policy '{}' on collection '{}'",
                        policy.name, collection
                    ));
                }
            }
        }

        Ok(())
    }

    /// Get the combined read predicate bytes for a tenant+collection.
    ///
    /// Returns the serialized filters to inject into DocumentScan.
    /// Multiple policies are AND-combined (all must pass).
    pub fn combined_read_predicate(&self, tenant_id: u32, collection: &str) -> Vec<u8> {
        let policies = self.read_policies(tenant_id, collection);
        if policies.is_empty() {
            return Vec::new();
        }

        // Collect all predicates from all policies.
        let mut all_filters: Vec<crate::bridge::scan_filter::ScanFilter> = Vec::new();
        for policy in &policies {
            if !policy.predicate.is_empty()
                && let Ok(filters) = rmp_serde::from_slice::<
                    Vec<crate::bridge::scan_filter::ScanFilter>,
                >(&policy.predicate)
            {
                all_filters.extend(filters);
            }
        }

        if all_filters.is_empty() {
            Vec::new()
        } else {
            rmp_serde::to_vec_named(&all_filters).unwrap_or_default()
        }
    }

    /// Total policies across all collections.
    pub fn policy_count(&self) -> usize {
        self.policies
            .read()
            .unwrap()
            .values()
            .map(|v| v.len())
            .sum()
    }
}

/// Namespace-scoped authorization: check permissions at tenant + namespace level.
///
/// Extends collection-level grants with namespace scoping:
/// `GRANT READ ON namespace.collection TO role`
///
/// Namespaces are dot-separated prefixes in collection names.
/// Checks: direct collection grant → namespace prefix grants → wildcard grant.
pub fn check_namespace_authz(
    identity: &super::identity::AuthenticatedIdentity,
    collection: &str,
    required_permission: super::identity::Permission,
    permission_store: &super::permission::PermissionStore,
    role_store: &super::role::RoleStore,
) -> bool {
    // Superusers bypass all checks.
    if identity.is_superuser {
        return true;
    }

    // Check direct collection-level grant via the existing permission system.
    if permission_store.check(identity, required_permission, collection, role_store) {
        return true;
    }

    // Check namespace-level grant: if collection is "ns.sub.table",
    // check grants on "ns.sub", "ns", and wildcard "*".
    let parts: Vec<&str> = collection.split('.').collect();
    for i in (0..parts.len()).rev() {
        let namespace = parts[..i].join(".");
        if !namespace.is_empty()
            && permission_store.check(identity, required_permission, &namespace, role_store)
        {
            return true;
        }
    }

    // Check wildcard grant.
    permission_store.check(identity, required_permission, "*", role_store)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_policy(name: &str, collection: &str, policy_type: PolicyType) -> RlsPolicy {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        RlsPolicy {
            name: name.into(),
            collection: collection.into(),
            tenant_id: 1,
            policy_type,
            predicate: Vec::new(),
            enabled: true,
            created_by: "admin".into(),
            created_at: now,
        }
    }

    #[test]
    fn create_and_query_policy() {
        let store = RlsPolicyStore::new();
        store
            .create_policy(make_policy("read_own", "users", PolicyType::Read))
            .unwrap();

        let read = store.read_policies(1, "users");
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].name, "read_own");

        let write = store.write_policies(1, "users");
        assert!(write.is_empty()); // Read-only policy.
    }

    #[test]
    fn write_policy_enforcement() {
        let store = RlsPolicyStore::new();

        // Create a write policy with a predicate: status must be "active".
        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "status".into(),
            op: "eq".into(),
            value: serde_json::json!("active"),
            clauses: Vec::new(),
        };
        let predicate = rmp_serde::to_vec_named(&vec![filter]).unwrap();
        let mut policy = make_policy("require_active", "orders", PolicyType::Write);
        policy.predicate = predicate;
        store.create_policy(policy).unwrap();

        // Document with status=active → allowed.
        let doc_ok = serde_json::json!({"status": "active", "amount": 100});
        assert!(store.check_write(1, "orders", &doc_ok, "alice").is_ok());

        // Document with status=draft → rejected.
        let doc_bad = serde_json::json!({"status": "draft", "amount": 100});
        assert!(store.check_write(1, "orders", &doc_bad, "alice").is_err());
    }

    #[test]
    fn drop_policy() {
        let store = RlsPolicyStore::new();
        store
            .create_policy(make_policy("p1", "users", PolicyType::Read))
            .unwrap();
        assert_eq!(store.policy_count(), 1);

        assert!(store.drop_policy(1, "users", "p1"));
        assert_eq!(store.policy_count(), 0);
    }

    #[test]
    fn all_policy_type_applies_to_both() {
        let store = RlsPolicyStore::new();
        store
            .create_policy(make_policy("both", "data", PolicyType::All))
            .unwrap();

        assert_eq!(store.read_policies(1, "data").len(), 1);
        assert_eq!(store.write_policies(1, "data").len(), 1);
    }

    #[test]
    fn no_policies_allows_everything() {
        let store = RlsPolicyStore::new();
        let doc = serde_json::json!({"anything": "goes"});
        assert!(store.check_write(1, "whatever", &doc, "anyone").is_ok());
    }
}
