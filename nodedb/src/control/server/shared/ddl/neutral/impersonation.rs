// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral impersonation & delegation DDL commands.
//!
//! ```sql
//! IMPERSONATE AUTH USER 'user_42'
//! STOP IMPERSONATION
//! DELEGATE AUTH USER 'bob' AS AUTH USER 'alice' SCOPES 'profile:read' EXPIRES 7d REASON 'vacation'
//! REVOKE DELEGATION FROM 'bob' AS 'alice'
//! SHOW DELEGATIONS
//! ```
//!
//! Ported from the pgwire `ddl::impersonation_ddl` handlers. All five mutate
//! or read the GLOBAL `state.impersonation` registry (keyed by user_id, not
//! by connection) plus the audit log — not the current connection's identity
//! — so they carry no per-connection state. The superuser / delegator gates,
//! the token parsing (`AS` / `SCOPES` / `EXPIRES` / `REASON` extraction), the
//! registry calls, and the audit records are preserved verbatim; only the
//! result construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// IMPERSONATE AUTH USER '<target_user_id>'
pub fn impersonate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    if parts.len() < 4 {
        return Err(err("42601", "syntax: IMPERSONATE AUTH USER '<user_id>'"));
    }
    let target_id = parts[3].trim_matches('\'');

    state
        .impersonation
        .start_impersonation(
            &identity.user_id.to_string(),
            &identity.username,
            target_id,
            target_id,
        )
        .map_err(|e| err("42000", e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("{} impersonating {target_id}", identity.username),
    );

    Ok(status("IMPERSONATE"))
}

/// STOP IMPERSONATION
pub fn stop_impersonation(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let stopped = state
        .impersonation
        .stop_impersonation(&identity.user_id.to_string());
    if !stopped {
        return Err(err("42000", "not currently impersonating anyone"));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        "impersonation stopped",
    );

    Ok(status("STOP IMPERSONATION"))
}

/// DELEGATE AUTH USER '<delegate>' AS AUTH USER '<delegator>' SCOPES '...' EXPIRES <dur> REASON '...'
pub fn delegate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // DELEGATE AUTH USER '<b>' AS AUTH USER '<a>' SCOPES '...' EXPIRES <d> REASON '...'
    if parts.len() < 8 {
        return Err(err(
            "42601",
            "syntax: DELEGATE AUTH USER '<delegate>' AS AUTH USER '<delegator>' SCOPES '...' [EXPIRES <dur>] [REASON '...']",
        ));
    }
    let delegate_id = parts[3].trim_matches('\'');
    // Find AS keyword.
    let as_idx = parts
        .iter()
        .position(|p| p.to_uppercase() == "AS")
        .ok_or_else(|| err("42601", "missing AS keyword"))?;
    let delegator_id = parts
        .get(as_idx + 3)
        .map(|s| s.trim_matches('\''))
        .unwrap_or("");

    // Only the delegator or a superuser can create delegations.
    if identity.user_id.to_string() != delegator_id && !identity.is_superuser {
        return Err(err("42501", "can only delegate your own scopes"));
    }

    let scopes: Vec<String> = parts
        .iter()
        .position(|p| p.to_uppercase() == "SCOPES")
        .map(|i| {
            parts[i + 1..]
                .iter()
                .take_while(|p| {
                    let u = p.to_uppercase();
                    u != "EXPIRES" && u != "REASON"
                })
                .map(|s| s.trim_matches('\'').trim_end_matches(',').to_string())
                .collect()
        })
        .unwrap_or_default();

    let expires_secs = parts
        .iter()
        .position(|p| p.to_uppercase() == "EXPIRES")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| {
            crate::control::server::shared::ddl::neutral::auth_user::parse_duration_public(s)
        })
        .unwrap_or(86_400); // Default 1 day.

    let reason = parts
        .iter()
        .position(|p| p.to_uppercase() == "REASON")
        .map(|i| parts[i + 1..].join(" ").trim_matches('\'').to_string())
        .unwrap_or_default();

    state
        .impersonation
        .delegate(delegator_id, delegate_id, scopes, expires_secs, &reason)
        .map_err(|e| err("42000", e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("delegated scopes from '{delegator_id}' to '{delegate_id}': {reason}"),
    );

    Ok(status("DELEGATE"))
}

/// REVOKE DELEGATION FROM '<delegate>' AS '<delegator>'
pub fn revoke_delegation(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 5 {
        return Err(err(
            "42601",
            "syntax: REVOKE DELEGATION FROM '<delegate>' AS '<delegator>'",
        ));
    }
    let delegate_id = parts.get(3).map(|s| s.trim_matches('\'')).unwrap_or("");
    let delegator_id = parts.get(5).map(|s| s.trim_matches('\'')).unwrap_or("");

    state
        .impersonation
        .revoke_delegation(delegator_id, delegate_id);

    state.audit_record(
        crate::control::security::audit::AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked delegation from '{delegator_id}' to '{delegate_id}'"),
    );

    Ok(status("REVOKE DELEGATION"))
}

/// SHOW DELEGATIONS
pub fn show_delegations(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let delegations = state.impersonation.list_delegations();

    let columns = vec![
        "delegator".to_string(),
        "delegate".to_string(),
        "scopes".to_string(),
        "expires_at".to_string(),
        "reason".to_string(),
    ];

    let rows: Vec<Map<String, JsonValue>> = delegations
        .iter()
        .map(|d| {
            let mut row = Map::new();
            row.insert(
                "delegator".to_string(),
                JsonValue::String(d.delegator_user_id.clone()),
            );
            row.insert(
                "delegate".to_string(),
                JsonValue::String(d.delegate_user_id.clone()),
            );
            row.insert("scopes".to_string(), JsonValue::String(d.scopes.join(", ")));
            row.insert(
                "expires_at".to_string(),
                JsonValue::String(d.expires_at.to_string()),
            );
            row.insert("reason".to_string(), JsonValue::String(d.reason.clone()));
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
