//! RLS predicate evaluation and `$auth.*` substitution.
//!
//! **Read path**: `combined_read_predicate_with_auth` merges all
//! enabled read policies into a single `ScanFilter` list injected
//! into `DocumentScan`.
//!
//! **Write path**: `check_write` / `check_write_with_auth` evaluate
//! each write policy against the document; failures return
//! `Error::RejectedAuthz`.

use tracing::{info, warn};

use crate::control::security::auth_context::AuthContext;
use crate::control::security::predicate::{PolicyMode, RlsPredicate};
use crate::control::security::predicate_eval::{combine_policies, substitute_to_scan_filters};

use super::store::RlsPolicyStore;
use super::types::RlsPolicy;

impl RlsPolicyStore {
    /// Check if a document passes all legacy write policies.
    pub fn check_write(
        &self,
        tenant_id: u32,
        collection: &str,
        document: &serde_json::Value,
        username: &str,
    ) -> crate::Result<()> {
        let policies = self.write_policies(tenant_id, collection);
        if policies.is_empty() {
            return Ok(());
        }

        for policy in &policies {
            if !policy.predicate.is_empty() {
                let filters: Vec<crate::bridge::scan_filter::ScanFilter> =
                    match zerompk::from_msgpack(&policy.predicate) {
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

                let doc_mp = nodedb_types::json_to_msgpack_or_empty(document);
                let passes = filters.iter().all(|f| f.matches_binary(&doc_mp));
                if !passes {
                    info!(
                        policy = %policy.name,
                        %username,
                        %collection,
                        "RLS write policy rejected"
                    );
                    return Err(crate::Error::RejectedAuthz {
                        tenant_id: crate::types::TenantId::new(tenant_id),
                        resource: format!(
                            "RLS policy '{}' on collection '{}'",
                            policy.name, collection
                        ),
                    });
                }
            }
        }

        Ok(())
    }

    /// Legacy (static) combined read predicate — AND-combined filters.
    pub fn combined_read_predicate(&self, tenant_id: u32, collection: &str) -> Vec<u8> {
        let policies = self.read_policies(tenant_id, collection);
        if policies.is_empty() {
            return Vec::new();
        }

        let mut all_filters: Vec<crate::bridge::scan_filter::ScanFilter> = Vec::new();
        for policy in &policies {
            if !policy.predicate.is_empty()
                && let Ok(filters) = zerompk::from_msgpack::<
                    Vec<crate::bridge::scan_filter::ScanFilter>,
                >(&policy.predicate)
            {
                all_filters.extend(filters);
            }
        }

        if all_filters.is_empty() {
            Vec::new()
        } else {
            zerompk::to_msgpack_vec(&all_filters).unwrap_or_default()
        }
    }

    /// Primary read-path RLS method: substitutes `$auth.*` variables
    /// and returns the combined `ScanFilter` bytes.
    ///
    /// Returns `None` when a required `$auth` field is missing
    /// (fail-closed semantics).
    pub fn combined_read_predicate_with_auth(
        &self,
        tenant_id: u32,
        collection: &str,
        auth: &AuthContext,
    ) -> Option<Vec<u8>> {
        if auth.is_superuser() {
            return Some(Vec::new());
        }

        let policies = self.read_policies(tenant_id, collection);
        if policies.is_empty() {
            return Some(Vec::new());
        }

        let mut compiled_policies: Vec<(RlsPredicate, PolicyMode)> = Vec::new();
        let mut legacy_filters: Vec<crate::bridge::scan_filter::ScanFilter> = Vec::new();

        for policy in &policies {
            if let Some(ref compiled) = policy.compiled_predicate {
                compiled_policies.push((compiled.clone(), policy.mode));
            } else if !policy.predicate.is_empty()
                && let Ok(filters) = zerompk::from_msgpack::<
                    Vec<crate::bridge::scan_filter::ScanFilter>,
                >(&policy.predicate)
            {
                legacy_filters.extend(filters);
            }
        }

        let mut all_filters = if !compiled_policies.is_empty() {
            combine_policies(&compiled_policies, auth)?
        } else {
            Vec::new()
        };

        all_filters.extend(legacy_filters);

        if all_filters.is_empty() {
            Some(Vec::new())
        } else {
            Some(zerompk::to_msgpack_vec(&all_filters).unwrap_or_default())
        }
    }

    /// Write-path RLS check with `$auth.*` support. Evaluates both
    /// compiled and legacy write policies; fail-closed on unresolved
    /// auth references.
    pub fn check_write_with_auth(
        &self,
        tenant_id: u32,
        collection: &str,
        document: &serde_json::Value,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        if auth.is_superuser() {
            return Ok(());
        }

        let policies = self.write_policies(tenant_id, collection);
        if policies.is_empty() {
            return Ok(());
        }

        for policy in &policies {
            if let Some(ref compiled) = policy.compiled_predicate {
                check_compiled_write(policy, compiled, document, auth, tenant_id, collection)?;
            } else if !policy.predicate.is_empty() {
                check_legacy_write(policy, document, auth, tenant_id, collection)?;
            }
        }

        Ok(())
    }
}

fn check_compiled_write(
    policy: &RlsPolicy,
    compiled: &RlsPredicate,
    document: &serde_json::Value,
    auth: &AuthContext,
    tenant_id: u32,
    collection: &str,
) -> crate::Result<()> {
    let filters = match substitute_to_scan_filters(compiled, auth) {
        Some(f) => f,
        None => {
            info!(
                policy = %policy.name,
                username = %auth.username,
                %collection,
                "RLS write policy: unresolved $auth reference → denied"
            );
            return Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(tenant_id),
                resource: format!(
                    "RLS policy '{}' on '{}': unresolved session variable",
                    policy.name, collection
                ),
            });
        }
    };

    let doc_mp = nodedb_types::json_to_msgpack_or_empty(document);
    if !filters.iter().all(|f| f.matches_binary(&doc_mp)) {
        info!(
            policy = %policy.name,
            username = %auth.username,
            %collection,
            "RLS write policy rejected (compiled)"
        );
        return Err(crate::Error::RejectedAuthz {
            tenant_id: crate::types::TenantId::new(tenant_id),
            resource: format!(
                "RLS policy '{}' on collection '{}'",
                policy.name, collection
            ),
        });
    }
    Ok(())
}

fn check_legacy_write(
    policy: &RlsPolicy,
    document: &serde_json::Value,
    auth: &AuthContext,
    tenant_id: u32,
    collection: &str,
) -> crate::Result<()> {
    let filters: Vec<crate::bridge::scan_filter::ScanFilter> =
        match zerompk::from_msgpack(&policy.predicate) {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    policy = %policy.name,
                    error = %e,
                    "RLS write policy predicate deserialization failed"
                );
                return Ok(());
            }
        };

    let doc_mp = nodedb_types::json_to_msgpack_or_empty(document);
    if !filters.iter().all(|f| f.matches_binary(&doc_mp)) {
        info!(
            policy = %policy.name,
            username = %auth.username,
            %collection,
            "RLS write policy rejected (legacy)"
        );
        return Err(crate::Error::RejectedAuthz {
            tenant_id: crate::types::TenantId::new(tenant_id),
            resource: format!(
                "RLS policy '{}' on collection '{}'",
                policy.name, collection
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::rls::types::PolicyType;

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
            compiled_predicate: None,
            mode: PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: now,
        }
    }

    #[test]
    fn write_policy_enforcement() {
        let store = RlsPolicyStore::new();

        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "status".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("active".into()),
            clauses: Vec::new(),
            expr: None,
        };
        let predicate = zerompk::to_msgpack_vec(&vec![filter]).unwrap();
        let mut policy = make_policy("require_active", "orders", PolicyType::Write);
        policy.predicate = predicate;
        store.create_policy(policy).unwrap();

        let doc_ok = serde_json::json!({"status": "active", "amount": 100});
        assert!(store.check_write(1, "orders", &doc_ok, "alice").is_ok());

        let doc_bad = serde_json::json!({"status": "draft", "amount": 100});
        assert!(store.check_write(1, "orders", &doc_bad, "alice").is_err());
    }

    #[test]
    fn no_policies_allows_everything() {
        let store = RlsPolicyStore::new();
        let doc = serde_json::json!({"anything": "goes"});
        assert!(store.check_write(1, "whatever", &doc, "anyone").is_ok());
    }
}
