//! redb cross-table referential integrity checks.
//!
//! redb transactions are atomic per-write but NOT across
//! tables. A crash mid-apply (or a code bug in the applier)
//! can leave any of the following invariants broken:
//!
//! - Every `StoredCollection` has a matching `StoredOwner`
//!   with `object_type = "collection"`.
//! - Every `StoredOwner.owner_username` resolves to a
//!   `StoredUser`.
//! - Every `StoredPermission.grantee` resolves to either a
//!   `StoredUser` (when prefixed `"user:"`) or a
//!   `StoredRole`.
//! - Every `StoredTrigger.collection` exists as a
//!   `StoredCollection` row.
//! - Every `StoredRlsPolicy.collection` exists as a
//!   `StoredCollection` row.
//!
//! None of these are auto-repaired. Redb is not the source of
//! truth — the raft log is — and the safe recovery for any
//! redb corruption is "re-run the applier from the log",
//! which is the operator's job. The integrity check reports
//! every violation and the sanity-check wrapper aborts
//! startup on any non-empty violation list.

use std::collections::HashSet;

use crate::control::security::catalog::SystemCatalog;

use super::divergence::{Divergence, DivergenceKind};

/// Run every cross-table integrity invariant against the
/// current redb state and return every violation found.
/// Never panics, never writes.
pub fn verify_redb_integrity(catalog: &SystemCatalog) -> Vec<Divergence> {
    let mut violations: Vec<Divergence> = Vec::new();

    // Fetch every table once up front. If a table load fails
    // it's logged and skipped — we can't cross-check what we
    // can't read, but we can still report the load error via
    // tracing and move on.
    let collections = match catalog.load_all_collections() {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "integrity: failed to load collections");
            return violations;
        }
    };
    let owners = match catalog.load_all_owners() {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "integrity: failed to load owners");
            Vec::new()
        }
    };
    let users = match catalog.load_all_users() {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "integrity: failed to load users");
            Vec::new()
        }
    };
    let roles = match catalog.load_all_roles() {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "integrity: failed to load roles");
            Vec::new()
        }
    };
    let permissions = match catalog.load_all_permissions() {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "integrity: failed to load permissions");
            Vec::new()
        }
    };
    let triggers = match catalog.load_all_triggers() {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "integrity: failed to load triggers");
            Vec::new()
        }
    };
    let rls = match catalog.load_all_rls_policies() {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "integrity: failed to load rls policies");
            Vec::new()
        }
    };

    // Build lookup sets once — every referential check is a
    // HashSet membership probe.
    let collection_keys: HashSet<(u32, String)> = collections
        .iter()
        .map(|c| (c.tenant_id, c.name.clone()))
        .collect();
    let user_names: HashSet<String> = users.iter().map(|u| u.username.clone()).collect();
    let role_names: HashSet<String> = roles.iter().map(|r| r.name.clone()).collect();
    let owner_keys: HashSet<(String, u32, String)> = owners
        .iter()
        .map(|o| (o.object_type.clone(), o.tenant_id, o.object_name.clone()))
        .collect();

    // ── Check 1: every collection has an owner. ──
    for c in &collections {
        let key = ("collection".to_string(), c.tenant_id, c.name.clone());
        if !owner_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::OrphanRow {
                kind: "collection",
                key: format!("{}:{}", c.tenant_id, c.name),
                expected_parent_kind: "owner",
            }));
        }
    }

    // ── Check 2: every owner.owner_username resolves to a user. ──
    for o in &owners {
        if !user_names.contains(&o.owner_username) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "owner",
                from_key: format!("{}:{}:{}", o.object_type, o.tenant_id, o.object_name),
                to_kind: "user",
                to_key: o.owner_username.clone(),
            }));
        }
    }

    // ── Check 3: every permission.grantee resolves. ──
    for p in &permissions {
        // `grantee` is either `"user:<name>"` or `"<role>"`.
        if let Some(username) = p.grantee.strip_prefix("user:") {
            if !user_names.contains(username) {
                violations.push(Divergence::new(DivergenceKind::DanglingReference {
                    from_kind: "permission",
                    from_key: format!("{}:{}", p.target, p.grantee),
                    to_kind: "user",
                    to_key: username.to_string(),
                }));
            }
        } else {
            // Role grantee — check role exists. Built-in
            // roles ("admin", "readonly", etc.) are NOT in the
            // StoredRole table (they live in the identity
            // module), so we only flag unknown custom names
            // that contain no built-in marker.
            if !role_names.contains(&p.grantee) && !is_builtin_role(&p.grantee) {
                violations.push(Divergence::new(DivergenceKind::DanglingReference {
                    from_kind: "permission",
                    from_key: format!("{}:{}", p.target, p.grantee),
                    to_kind: "role",
                    to_key: p.grantee.clone(),
                }));
            }
        }
    }

    // ── Check 4: every trigger.collection exists. ──
    for t in &triggers {
        let key = (t.tenant_id, t.collection.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "trigger",
                from_key: format!("{}:{}", t.tenant_id, t.name),
                to_kind: "collection",
                to_key: format!("{}:{}", t.tenant_id, t.collection),
            }));
        }
    }

    // ── Check 5: every rls_policy.collection exists. ──
    for p in &rls {
        let key = (p.tenant_id, p.collection.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "rls_policy",
                from_key: format!("{}:{}", p.tenant_id, p.name),
                to_kind: "collection",
                to_key: format!("{}:{}", p.tenant_id, p.collection),
            }));
        }
    }

    violations
}

/// Built-in role names that exist outside the `StoredRole`
/// table. These must match the set in
/// `security::identity::Role`.
fn is_builtin_role(name: &str) -> bool {
    matches!(
        name,
        "superuser" | "tenant_admin" | "readwrite" | "readonly" | "monitor"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_role_detection() {
        assert!(is_builtin_role("superuser"));
        assert!(is_builtin_role("readonly"));
        assert!(is_builtin_role("monitor"));
        assert!(!is_builtin_role("admin"));
        assert!(!is_builtin_role("custom_auditor"));
    }
}
