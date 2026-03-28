//! Scope management DDL commands.
//!
//! ```sql
//! DEFINE SCOPE 'profile:read' AS READ ON user_profiles, READ ON user_settings
//! DEFINE SCOPE 'customer' AS INCLUDE 'profile:read', INCLUDE 'orders:write'
//! DROP SCOPE 'profile:read'
//! GRANT SCOPE 'pro:all' TO ORG 'acme'
//! GRANT SCOPE 'profile:read' TO USER 'user_42'
//! REVOKE SCOPE 'pro:all' FROM ORG 'acme'
//! SHOW SCOPES
//! SHOW SCOPE 'profile:read'
//! SHOW MY SCOPES
//! SHOW SCOPES FOR USER 'user_42'
//! SHOW SCOPES FOR ORG 'acme'
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// DEFINE SCOPE '<name>' AS <perm> ON <coll> [, <perm> ON <coll>] [INCLUDE '<scope>']
pub fn define_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    // DEFINE SCOPE '<name>' AS ...
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DEFINE SCOPE '<name>' AS <perm> ON <coll> [, ...] [INCLUDE '<scope>']",
        ));
    }

    let scope_name = parts[2].trim_matches('\'');
    // Everything after "AS" is the definition.
    let as_idx = parts
        .iter()
        .position(|p| p.to_uppercase() == "AS")
        .ok_or_else(|| sqlstate_error("42601", "missing AS keyword"))?;

    let def_parts = &parts[as_idx + 1..];

    let mut grants = Vec::new();
    let mut includes = Vec::new();

    let mut i = 0;
    while i < def_parts.len() {
        let token = def_parts[i].to_uppercase();
        match token.as_str() {
            "INCLUDE" => {
                if i + 1 < def_parts.len() {
                    let inc = def_parts[i + 1].trim_matches('\'').trim_end_matches(',');
                    includes.push(inc.to_string());
                    i += 2;
                } else {
                    return Err(sqlstate_error("42601", "INCLUDE requires a scope name"));
                }
            }
            "READ" | "WRITE" | "CREATE" | "DROP" | "ALTER" | "ADMIN" => {
                // <perm> ON <collection>
                if i + 2 < def_parts.len() && def_parts[i + 1].to_uppercase() == "ON" {
                    let coll = def_parts[i + 2].trim_matches('\'').trim_end_matches(',');
                    grants.push((token.to_lowercase(), coll.to_string()));
                    i += 3;
                } else {
                    return Err(sqlstate_error("42601", "expected <perm> ON <collection>"));
                }
            }
            _ => {
                // Skip commas and unknown tokens.
                i += 1;
            }
        }
    }

    state
        .scope_defs
        .define(scope_name, grants, includes, &identity.username)
        .map_err(|e| sqlstate_error("42601", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("defined scope '{scope_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DEFINE SCOPE"))])
}

/// DROP SCOPE '<name>'
pub fn drop_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP SCOPE '<name>'"));
    }
    let name = parts[2].trim_matches('\'');

    let found = state
        .scope_defs
        .drop_scope(name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if !found {
        return Err(sqlstate_error(
            "42704",
            &format!("scope '{name}' not found"),
        ));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("dropped scope '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP SCOPE"))])
}

/// GRANT SCOPE '<scope>' TO <ORG|USER|ROLE> '<id>'
pub fn grant_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    // GRANT SCOPE '<scope>' TO <type> '<id>'
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: GRANT SCOPE '<scope>' TO <ORG|USER|ROLE> '<id>'",
        ));
    }
    let scope_name = parts[2].trim_matches('\'');
    let grantee_type = parts[4].to_lowercase();
    let grantee_id = parts[5].trim_matches('\'');

    if !matches!(grantee_type.as_str(), "org" | "user" | "role" | "team") {
        return Err(sqlstate_error(
            "42601",
            "grantee type must be ORG, USER, ROLE, or TEAM",
        ));
    }

    // Parse optional EXPIRES, GRACE PERIOD, ON EXPIRE clauses.
    let expires_at = parse_expires(parts);
    let grace_period_secs = parse_grace_period(parts);
    let on_expire_action = parse_on_expire(parts);

    state
        .scope_grants
        .grant(
            scope_name,
            &grantee_type,
            grantee_id,
            &identity.username,
            expires_at,
            grace_period_secs,
            &on_expire_action,
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("granted scope '{scope_name}' to {grantee_type} '{grantee_id}'"),
    );

    Ok(vec![Response::Execution(Tag::new("GRANT SCOPE"))])
}

/// REVOKE SCOPE '<scope>' FROM <ORG|USER|ROLE> '<id>'
pub fn revoke_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REVOKE SCOPE '<scope>' FROM <ORG|USER|ROLE> '<id>'",
        ));
    }
    let scope_name = parts[2].trim_matches('\'');
    let grantee_type = parts[4].to_lowercase();
    let grantee_id = parts[5].trim_matches('\'');

    state
        .scope_grants
        .revoke(scope_name, &grantee_type, grantee_id)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked scope '{scope_name}' from {grantee_type} '{grantee_id}'"),
    );

    Ok(vec![Response::Execution(Tag::new("REVOKE SCOPE"))])
}

/// SHOW SCOPES / SHOW SCOPE '<name>' / SHOW SCOPES FOR <type> '<id>'
pub fn show_scopes(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // SHOW SCOPE '<name>' — resolve a single scope.
    if parts.len() >= 3 && parts[1].to_uppercase() == "SCOPE" && parts[2].to_uppercase() != "GRANTS"
    {
        let name = parts[2].trim_matches('\'');
        let resolved = state.scope_defs.resolve(name);
        let schema = Arc::new(vec![text_field("permission"), text_field("collection")]);
        let rows: Vec<_> = resolved
            .iter()
            .map(|(perm, coll)| {
                let mut enc = DataRowEncoder::new(schema.clone());
                let _ = enc.encode_field(perm);
                let _ = enc.encode_field(coll);
                Ok(enc.take_row())
            })
            .collect();
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter(rows),
        ))]);
    }

    // SHOW SCOPES — list all scope definitions.
    let scopes = state.scope_defs.list();
    let schema = Arc::new(vec![
        text_field("name"),
        text_field("grants"),
        text_field("includes"),
        text_field("created_by"),
    ]);

    let rows: Vec<_> = scopes
        .iter()
        .map(|s| {
            let grants_str: Vec<String> = s
                .grants
                .iter()
                .map(|(p, c)| format!("{p} ON {c}"))
                .collect();
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&s.name);
            let _ = enc.encode_field(&grants_str.join(", "));
            let _ = enc.encode_field(&s.includes.join(", "));
            let _ = enc.encode_field(&s.created_by);
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// RENEW SCOPE '<scope>' FOR <ORG|USER> '<id>' EXTEND BY <duration>
pub fn renew_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    // RENEW SCOPE '<scope>' FOR <type> '<id>' EXTEND BY <duration>
    if parts.len() < 8 {
        return Err(sqlstate_error(
            "42601",
            "syntax: RENEW SCOPE '<scope>' FOR <ORG|USER> '<id>' EXTEND BY <duration>",
        ));
    }
    let scope_name = parts[2].trim_matches('\'');
    let grantee_type = parts[4].to_lowercase();
    let grantee_id = parts[5].trim_matches('\'');
    let duration_str = parts[7];
    let extend_secs =
        crate::control::server::pgwire::ddl::auth_user_ddl::parse_duration_public(duration_str)
            .ok_or_else(|| {
                sqlstate_error("42601", &format!("invalid duration: '{duration_str}'"))
            })?;

    let found = state
        .scope_grants
        .renew(scope_name, &grantee_type, grantee_id, extend_secs)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if !found {
        return Err(sqlstate_error("42704", "scope grant not found"));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "renewed scope '{scope_name}' for {grantee_type} '{grantee_id}' by {duration_str}"
        ),
    );

    Ok(vec![Response::Execution(Tag::new("RENEW SCOPE"))])
}

/// SHOW SCOPE GRANTS [EXPIRING WITHIN <duration>]
pub fn show_scope_grants(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let grants = if let Some(within_idx) = parts.iter().position(|p| p.to_uppercase() == "WITHIN") {
        let dur_str = parts.get(within_idx + 1).unwrap_or(&"7d");
        let secs =
            crate::control::server::pgwire::ddl::auth_user_ddl::parse_duration_public(dur_str)
                .unwrap_or(7 * 86_400);
        state.scope_grants.expiring_within(secs)
    } else {
        state.scope_grants.list(None)
    };

    let schema = Arc::new(vec![
        text_field("scope"),
        text_field("grantee_type"),
        text_field("grantee_id"),
        text_field("status"),
        text_field("expires_at"),
        text_field("granted_by"),
    ]);

    let rows: Vec<_> = grants
        .iter()
        .map(|g| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&g.scope_name);
            let _ = enc.encode_field(&g.grantee_type);
            let _ = enc.encode_field(&g.grantee_id);
            let _ = enc.encode_field(&g.status().to_string());
            let _ = enc.encode_field(&if g.expires_at == 0 {
                "permanent".to_string()
            } else {
                g.expires_at.to_string()
            });
            let _ = enc.encode_field(&g.granted_by);
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
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
        .and_then(|s| crate::control::server::pgwire::ddl::auth_user_ddl::parse_duration_public(s))
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
