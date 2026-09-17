// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral auth user management DDL commands (JIT-provisioned users).
//!
//! ```sql
//! ALTER AUTH USER 'user_42' SET STATUS active|suspended|banned|restricted|read_only
//! DEACTIVATE AUTH USER 'user_42'
//! PURGE AUTH USERS INACTIVE FOR 90d
//! SHOW AUTH USERS
//! ```
//!
//! Ported from the pgwire `ddl::auth_user_ddl` handlers. The superuser gate,
//! status parsing, auth-user store mutations, and `audit_record` side effects
//! are preserved verbatim; only the result construction changed from pgwire
//! `Response` / `QueryResponse` / `Tag` to the protocol-neutral [`DdlResult`]
//! over [`ShapedRows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::auth_context::AuthStatus;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`], preserving the exact SQLSTATE codes and messages
/// the pgwire handlers produced.
fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Build a single-tag status result.
fn status(command: impl Into<String>) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.into(),
        rows_affected: None,
    }]
}

/// Handle ALTER AUTH USER or DEACTIVATE AUTH USER commands.
pub fn handle_auth_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    let upper0 = parts.first().map(|s| s.to_uppercase()).unwrap_or_default();
    match upper0.as_str() {
        "DEACTIVATE" => deactivate_auth_user(state, identity, parts),
        "ALTER" => alter_auth_user_status(state, identity, parts),
        _ => Err(err(
            "42601",
            "expected ALTER AUTH USER or DEACTIVATE AUTH USER",
        )),
    }
}

/// DEACTIVATE AUTH USER '<user_id>'
fn deactivate_auth_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // DEACTIVATE AUTH USER '<id>'
    if parts.len() < 4 {
        return Err(err("42601", "syntax: DEACTIVATE AUTH USER '<user_id>'"));
    }

    let user_id = parts[3].trim_matches('\'');

    let found = state
        .auth_users
        .deactivate(user_id)
        .map_err(|e| err("XX000", e.to_string()))?;

    if !found {
        return Err(err("42704", format!("auth user '{user_id}' not found")));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("deactivated auth user '{user_id}'"),
    );

    Ok(status("DEACTIVATE"))
}

/// ALTER AUTH USER '<user_id>' SET STATUS <status>
fn alter_auth_user_status(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // ALTER AUTH USER '<id>' SET STATUS <status>
    if parts.len() < 7 {
        return Err(err(
            "42601",
            "syntax: ALTER AUTH USER '<user_id>' SET STATUS <active|suspended|banned|restricted|read_only>",
        ));
    }

    let user_id = parts[3].trim_matches('\'');
    let status_str = parts[6].to_lowercase();
    let status_val: AuthStatus = status_str.parse().map_err(|e: String| err("42601", e))?;

    let found = state
        .auth_users
        .set_status(user_id, status_val)
        .map_err(|e| err("XX000", e.to_string()))?;

    if !found {
        return Err(err("42704", format!("auth user '{user_id}' not found")));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("auth user '{user_id}' status set to {status_val}"),
    );

    Ok(status("ALTER AUTH USER"))
}

/// PURGE AUTH USERS INACTIVE FOR <duration>
///
/// Duration format: `90d` (days), `24h` (hours).
pub fn purge_auth_users(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    // PURGE AUTH USERS INACTIVE FOR <duration>
    if parts.len() < 6 {
        return Err(err(
            "42601",
            "syntax: PURGE AUTH USERS INACTIVE FOR <duration> (e.g., 90d, 24h)",
        ));
    }

    let duration_str = parts[5];
    let threshold_secs = parse_duration_secs(duration_str).ok_or_else(|| {
        err(
            "42601",
            format!("invalid duration: '{duration_str}'. Use 90d or 24h"),
        )
    })?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let cutoff = now.saturating_sub(threshold_secs);
    let purged = state
        .auth_users
        .purge_inactive(cutoff)
        .map_err(|e| err("XX000", e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("purged {purged} inactive auth users (older than {duration_str})"),
    );

    Ok(status(format!("PURGE {purged}")))
}

/// SHOW AUTH USERS
pub fn show_auth_users(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    let users = state.auth_users.list(false);

    let columns = vec![
        "id".to_string(),
        "username".to_string(),
        "email".to_string(),
        "tenant_id".to_string(),
        "provider".to_string(),
        "status".to_string(),
        "is_active".to_string(),
        "last_seen".to_string(),
    ];

    let rows: Vec<_> = users
        .iter()
        .map(|u| {
            let mut row = Map::new();
            row.insert("id".to_string(), JsonValue::String(u.id.clone()));
            row.insert(
                "username".to_string(),
                JsonValue::String(u.username.clone()),
            );
            row.insert("email".to_string(), JsonValue::String(u.email.clone()));
            row.insert(
                "tenant_id".to_string(),
                JsonValue::String(u.tenant_id.to_string()),
            );
            row.insert(
                "provider".to_string(),
                JsonValue::String(u.provider.clone()),
            );
            row.insert(
                "status".to_string(),
                JsonValue::String(u.status.to_string()),
            );
            row.insert(
                "is_active".to_string(),
                JsonValue::String(u.is_active.to_string()),
            );
            row.insert(
                "last_seen".to_string(),
                JsonValue::String(u.last_seen.to_string()),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// Public re-export of duration parser for use by other DDL modules.
pub fn parse_duration_public(s: &str) -> Option<u64> {
    parse_duration_secs(s)
}

/// Parse a duration string like "90d", "24h", "3600s" to seconds.
fn parse_duration_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('d') {
        let n: u64 = n.parse().ok()?;
        Some(n * 86_400)
    } else if let Some(n) = s.strip_suffix('h') {
        let n: u64 = n.parse().ok()?;
        Some(n * 3_600)
    } else if let Some(n) = s.strip_suffix('s') {
        n.parse().ok()
    } else {
        s.parse().ok()
    }
}
