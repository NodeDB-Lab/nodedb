// SPDX-License-Identifier: BUSL-1.1

//! Scope grant lifecycle: `GRANT SCOPE`, `REVOKE SCOPE`, `RENEW SCOPE`, and
//! `SHOW SCOPE GRANTS`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::catalog::StoredScopeGrant;
use crate::control::security::conditional::{parse_conditions, render_conditions};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::scope::grant::replication::{propose_grant, propose_revoke};
use crate::control::security::scope::grant::{RenewOutcome, ScopeGrantParams};
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{err, status};

/// Replicate a scope-grant upsert (`GRANT SCOPE`, and `RENEW SCOPE`, which
/// is the same upsert with a later expiry), surfacing failures as SQL errors.
///
/// The replication itself lives with the rest of the scope-grant apply path so
/// the expiry sweep and the DDL handlers cannot drift apart; this wrapper only
/// translates the error into what pgwire reports.
fn propose_scope_grant(state: &SharedState, stored: &StoredScopeGrant) -> Result<(), DdlError> {
    propose_grant(state, stored).map_err(|e| err("XX000", e.to_string()))
}

/// Replicate a scope-grant removal. Same dual path as [`propose_scope_grant`].
fn propose_scope_revoke(
    state: &SharedState,
    scope_name: &str,
    grantee_type: &str,
    grantee_id: &str,
) -> Result<(), DdlError> {
    propose_revoke(state, scope_name, grantee_type, grantee_id)
        .map_err(|e| err("XX000", e.to_string()))
}

/// GRANT SCOPE '<scope>' TO <ORG|USER|ROLE> '<id>'
///     [EXPIRES '<unix ts>'] [GRACE PERIOD <duration>] [ON EXPIRE <action>]
///     [WHEN BETWEEN '<start>' AND '<end>' [ON WEEKDAYS|WEEKENDS|ALL]]
///     [REQUIRE MFA] [REQUIRE IP IN ('<cidr>', ...)]
///     [REQUIRE STEP_UP [<seconds>]] [REQUIRE DEVICE_TRUST]
///
/// The expiry clauses retire the grant on a wall clock; the condition
/// clauses leave it granted but decide, per request, whether it applies.
pub fn grant_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    // GRANT SCOPE '<scope>' TO <type> '<id>'
    if parts.len() < 6 {
        return Err(err(
            "42601",
            "syntax: GRANT SCOPE '<scope>' TO <ORG|USER|ROLE> '<id>'",
        ));
    }
    let scope_name = parts[2].trim_matches('\'');
    let grantee_type = parts[4].to_lowercase();
    let grantee_id = parts[5].trim_matches('\'');

    if !matches!(grantee_type.as_str(), "org" | "user" | "role" | "team") {
        return Err(err(
            "42601",
            "grantee type must be ORG, USER, ROLE, or TEAM",
        ));
    }

    // Parse optional EXPIRES, GRACE PERIOD, ON EXPIRE clauses.
    let expires_at = parse_expires(parts);
    let grace_period_secs = parse_grace_period(parts);
    let on_expire_action = parse_on_expire(parts);
    // Conditions come from the clause tail, after the grantee. A malformed
    // condition is a syntax error, never a silently unconditional grant.
    let conditions = parse_conditions(&parts[6..]).map_err(|e| err("42601", e.to_string()))?;
    let rendered_conditions = render_conditions(&conditions);

    let stored = state
        .scope_grants
        .prepare_grant(ScopeGrantParams {
            scope_name,
            grantee_type: &grantee_type,
            grantee_id,
            granted_by: &identity.username,
            expires_at,
            grace_period_secs,
            on_expire_action: &on_expire_action,
            conditions,
        })
        .map_err(|e| err("XX000", e.to_string()))?;
    propose_scope_grant(state, &stored)?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "granted scope '{scope_name}' to {grantee_type} '{grantee_id}' \
             with conditions: {rendered_conditions}"
        ),
    );

    Ok(status("GRANT SCOPE"))
}

/// REVOKE SCOPE '<scope>' FROM <ORG|USER|ROLE> '<id>'
pub fn revoke_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    if parts.len() < 6 {
        return Err(err(
            "42601",
            "syntax: REVOKE SCOPE '<scope>' FROM <ORG|USER|ROLE> '<id>'",
        ));
    }
    let scope_name = parts[2].trim_matches('\'');
    let grantee_type = parts[4].to_lowercase();
    let grantee_id = parts[5].trim_matches('\'');

    propose_scope_revoke(state, scope_name, &grantee_type, grantee_id)?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked scope '{scope_name}' from {grantee_type} '{grantee_id}'"),
    );

    Ok(status("REVOKE SCOPE"))
}

/// RENEW SCOPE '<scope>' FOR <ORG|USER> '<id>' EXTEND BY <duration>
pub fn renew_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    // RENEW SCOPE '<scope>' FOR <type> '<id>' EXTEND BY <duration>
    if parts.len() < 8 {
        return Err(err(
            "42601",
            "syntax: RENEW SCOPE '<scope>' FOR <ORG|USER> '<id>' EXTEND BY <duration>",
        ));
    }
    let scope_name = parts[2].trim_matches('\'');
    let grantee_type = parts[4].to_lowercase();
    let grantee_id = parts[5].trim_matches('\'');
    let duration_str = parts[7];
    let extend_secs =
        crate::control::server::shared::ddl::neutral::auth_user::parse_duration_public(
            duration_str,
        )
        .ok_or_else(|| err("42601", format!("invalid duration: '{duration_str}'")))?;

    let outcome = state
        .scope_grants
        .prepare_renew(scope_name, &grantee_type, grantee_id, extend_secs)
        .map_err(|e| err("XX000", e.to_string()))?;
    match outcome {
        RenewOutcome::NotFound => return Err(err("42704", "scope grant not found")),
        // Nothing to move: a permanent grant has no deadline to extend.
        RenewOutcome::AlreadyPermanent => {}
        RenewOutcome::Extend(stored) => propose_scope_grant(state, &stored)?,
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "renewed scope '{scope_name}' for {grantee_type} '{grantee_id}' by {duration_str}"
        ),
    );

    Ok(status("RENEW SCOPE"))
}

/// SHOW SCOPE GRANTS [EXPIRING WITHIN <duration>]
pub fn show_scope_grants(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let grants = if let Some(within_idx) = parts.iter().position(|p| p.to_uppercase() == "WITHIN") {
        let dur_str = parts.get(within_idx + 1).unwrap_or(&"7d");
        let secs =
            crate::control::server::shared::ddl::neutral::auth_user::parse_duration_public(dur_str)
                .unwrap_or(7 * 86_400);
        state.scope_grants.expiring_within(secs)
    } else {
        state.scope_grants.list(None)
    };

    let columns = vec![
        "scope".to_string(),
        "grantee_type".to_string(),
        "grantee_id".to_string(),
        "status".to_string(),
        "expires_at".to_string(),
        "conditions".to_string(),
        "granted_by".to_string(),
    ];

    let rows: Vec<_> = grants
        .iter()
        .map(|g| {
            let mut row = Map::new();
            row.insert("scope".to_string(), JsonValue::String(g.scope_name.clone()));
            row.insert(
                "grantee_type".to_string(),
                JsonValue::String(g.grantee_type.clone()),
            );
            row.insert(
                "grantee_id".to_string(),
                JsonValue::String(g.grantee_id.clone()),
            );
            row.insert(
                "status".to_string(),
                JsonValue::String(g.status().to_string()),
            );
            row.insert(
                "expires_at".to_string(),
                JsonValue::String(if g.expires_at == 0 {
                    "permanent".to_string()
                } else {
                    g.expires_at.to_string()
                }),
            );
            // An operator debugging "why isn't this grant applying?" needs
            // to see the conditions attached to it, not just its expiry.
            row.insert(
                "conditions".to_string(),
                JsonValue::String(render_conditions(&g.conditions)),
            );
            row.insert(
                "granted_by".to_string(),
                JsonValue::String(g.granted_by.clone()),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

// ── Parse helpers for time-bound GRANT SCOPE syntax ────────────────

/// Parse EXPIRES '<timestamp>' from parts. Returns 0 if not present.
fn parse_expires(parts: &[&str]) -> u64 {
    parts
        .iter()
        .position(|p| p.to_uppercase() == "EXPIRES")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.trim_matches('\'').parse::<u64>().ok())
        .unwrap_or(0)
}

/// Parse GRACE PERIOD <duration> from parts. Returns 0 if not present.
fn parse_grace_period(parts: &[&str]) -> u64 {
    parts
        .iter()
        .position(|p| p.to_uppercase() == "GRACE")
        .and_then(|i| {
            // GRACE PERIOD <duration>
            if parts.get(i + 1).map(|s| s.to_uppercase()) == Some("PERIOD".into()) {
                parts.get(i + 2)
            } else {
                None
            }
        })
        .and_then(|s| {
            crate::control::server::shared::ddl::neutral::auth_user::parse_duration_public(s)
        })
        .unwrap_or(0)
}

/// Parse ON EXPIRE action from parts.
fn parse_on_expire(parts: &[&str]) -> String {
    let idx = parts.iter().position(|p| p.to_uppercase() == "EXPIRE");
    let Some(i) = idx else {
        return String::new();
    };
    // Check previous token is "ON".
    if i == 0 || parts[i - 1].to_uppercase() != "ON" {
        return String::new();
    }
    // ON EXPIRE GRANT SCOPE '<name>' → "grant:<name>"
    // ON EXPIRE REVOKE ALL → "revoke_all"
    let action = parts
        .get(i + 1)
        .map(|s| s.to_uppercase())
        .unwrap_or_default();
    match action.as_str() {
        "GRANT" => {
            // ON EXPIRE GRANT SCOPE '<name>'
            let scope = parts.get(i + 3).unwrap_or(&"");
            format!("grant:{}", scope.trim_matches('\''))
        }
        "REVOKE" => "revoke_all".into(),
        _ => String::new(),
    }
}
