//! EXPLAIN PERMISSION: full permission resolution trace + policy linting.

use super::auth_context::AuthContext;
use super::identity::AuthenticatedIdentity;

/// Result of EXPLAIN PERMISSION resolution.
#[derive(Debug, Clone)]
pub struct PermissionExplanation {
    pub collection: String,
    pub permission: String,
    pub auth_user_id: String,
    /// Whether the final result is ALLOW or DENY.
    pub allowed: bool,
    /// Resolution steps (in order of evaluation).
    pub steps: Vec<ExplainStep>,
}

/// A single step in the permission resolution chain.
#[derive(Debug, Clone)]
pub struct ExplainStep {
    pub check: String,
    pub result: String,
    pub source: String,
}

/// Resolve full permission chain for EXPLAIN PERMISSION.
pub fn explain_permission(
    permission: &str,
    collection: &str,
    identity: &AuthenticatedIdentity,
    auth_ctx: &AuthContext,
    state: &crate::control::state::SharedState,
) -> PermissionExplanation {
    let mut steps = Vec::new();
    let user_id = identity.user_id.to_string();

    // 1. Account status.
    let status_ok = auth_ctx.check_status().is_ok();
    steps.push(ExplainStep {
        check: "account_status".into(),
        result: if status_ok { "PASS" } else { "DENY" }.into(),
        source: format!("status={}", auth_ctx.status),
    });
    if !status_ok {
        return PermissionExplanation {
            collection: collection.into(),
            permission: permission.into(),
            auth_user_id: user_id,
            allowed: false,
            steps,
        };
    }

    // 2. Superuser check.
    if identity.is_superuser {
        steps.push(ExplainStep {
            check: "superuser".into(),
            result: "PASS (bypass all)".into(),
            source: "identity.is_superuser=true".into(),
        });
        return PermissionExplanation {
            collection: collection.into(),
            permission: permission.into(),
            auth_user_id: user_id,
            allowed: true,
            steps,
        };
    }

    // 3. Role-based permission.
    let perm_enum = match permission.to_lowercase().as_str() {
        "read" | "select" => super::identity::Permission::Read,
        "write" | "insert" | "update" | "delete" => super::identity::Permission::Write,
        "create" => super::identity::Permission::Create,
        "drop" => super::identity::Permission::Drop,
        "alter" => super::identity::Permission::Alter,
        "admin" => super::identity::Permission::Admin,
        "monitor" => super::identity::Permission::Monitor,
        _ => super::identity::Permission::Read,
    };
    let role_ok = super::identity::role_grants_permission(
        &identity
            .roles
            .first()
            .cloned()
            .unwrap_or(super::identity::Role::ReadOnly),
        perm_enum,
    );
    steps.push(ExplainStep {
        check: "role_permission".into(),
        result: if role_ok { "PASS" } else { "DENY" }.into(),
        source: format!("roles={:?}", identity.roles),
    });

    // 4. Scope check.
    let org_ids = state.orgs.orgs_for_user(&user_id);
    let effective_scopes = state.scope_grants.effective_scopes(&user_id, &org_ids);
    let scope_ok = !effective_scopes.is_empty();
    steps.push(ExplainStep {
        check: "effective_scopes".into(),
        result: if scope_ok { "PASS" } else { "NONE" }.into(),
        source: format!("scopes={:?}", effective_scopes),
    });

    // 5. RLS policies.
    let rls_policies = state
        .rls
        .read_policies(identity.tenant_id.as_u32(), collection);
    steps.push(ExplainStep {
        check: "rls_policies".into(),
        result: format!("{} policies", rls_policies.len()),
        source: rls_policies
            .iter()
            .map(|p| p.name.clone())
            .collect::<Vec<_>>()
            .join(", "),
    });

    // 6. Rate limit status.
    let rl_result = state.rate_limiter.check(
        &user_id,
        &org_ids,
        auth_ctx.metadata.get("plan").map(|s| s.as_str()),
        "point_get",
    );
    steps.push(ExplainStep {
        check: "rate_limit".into(),
        result: if rl_result.allowed { "PASS" } else { "DENY" }.into(),
        source: format!("remaining={}/{}", rl_result.remaining, rl_result.limit),
    });

    // 7. Blacklist check.
    let blacklisted = state.blacklist.check_user(&user_id).is_some();
    steps.push(ExplainStep {
        check: "blacklist".into(),
        result: if blacklisted { "DENY" } else { "PASS" }.into(),
        source: if blacklisted {
            "user blacklisted"
        } else {
            "not blacklisted"
        }
        .into(),
    });

    let allowed = status_ok && role_ok && !blacklisted && rl_result.allowed;

    PermissionExplanation {
        collection: collection.into(),
        permission: permission.into(),
        auth_user_id: user_id,
        allowed,
        steps,
    }
}

/// Lint an RLS predicate for common issues.
///
/// Returns a list of warnings (empty = clean).
pub fn lint_predicate(predicate: &super::predicate::RlsPredicate) -> Vec<String> {
    let mut warnings = Vec::new();

    match predicate {
        super::predicate::RlsPredicate::AlwaysTrue => {
            warnings.push("tautology: predicate is always true (no filtering)".into());
        }
        super::predicate::RlsPredicate::AlwaysFalse => {
            warnings.push("contradiction: predicate is always false (blocks everything)".into());
        }
        super::predicate::RlsPredicate::Compare { value, .. }
            if !value.is_auth_ref()
                && matches!(value, super::predicate::PredicateValue::Literal(_)) =>
        {
            warnings
                .push("static predicate: no $auth reference — same result for all users".into());
        }
        super::predicate::RlsPredicate::And(children)
        | super::predicate::RlsPredicate::Or(children) => {
            for child in children {
                warnings.extend(lint_predicate(child));
            }
        }
        super::predicate::RlsPredicate::Not(inner) => {
            warnings.extend(lint_predicate(inner));
        }
        _ => {}
    }

    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};

    #[test]
    fn lint_always_true() {
        let warnings = lint_predicate(&RlsPredicate::AlwaysTrue);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("tautology"));
    }

    #[test]
    fn lint_static_predicate() {
        let pred = RlsPredicate::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: PredicateValue::Literal(serde_json::json!("active")),
        };
        let warnings = lint_predicate(&pred);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("static predicate"));
    }

    #[test]
    fn lint_auth_ref_clean() {
        let pred = RlsPredicate::Compare {
            field: "user_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("id".into()),
        };
        let warnings = lint_predicate(&pred);
        assert!(warnings.is_empty());
    }
}
