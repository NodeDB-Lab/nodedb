// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral auth-scoped API key DDL commands.
//!
//! ```sql
//! CREATE AUTH KEY FOR AUTH USER 'x' WITH SCOPES 'profile:read' [RATE_LIMIT 100] [EXPIRES 30d]
//! ROTATE AUTH KEY '<key_id>' [OVERLAP 24h]
//! LIST AUTH KEYS [FOR AUTH USER 'x']
//! ```
//!
//! Ported from the pgwire `ddl::auth_key_ddl` handlers. The superuser gate,
//! token creation / rotation, listing, and `audit_record` side effects are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `QueryResponse` to the protocol-neutral [`DdlResult`] over
//! [`ShapedRows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`], preserving the exact SQLSTATE codes and messages
/// the pgwire handlers produced.
fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// CREATE AUTH KEY FOR AUTH USER '<id>' WITH SCOPES '...' [RATE_LIMIT N] [EXPIRES Nd]
pub fn create_auth_key(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    // CREATE AUTH KEY FOR AUTH USER '<id>' ...
    if parts.len() < 6 {
        return Err(err(
            "42601",
            "syntax: CREATE AUTH KEY FOR AUTH USER '<id>' [WITH SCOPES '...'] [RATE_LIMIT N] [EXPIRES Nd]",
        ));
    }
    let auth_user_id = parts[5].trim_matches('\'');

    // Parse scopes.
    let scopes: Vec<String> = parts
        .iter()
        .position(|p| p.to_uppercase() == "SCOPES")
        .map(|i| {
            parts[i + 1..]
                .iter()
                .take_while(|p| {
                    let u = p.to_uppercase();
                    u != "RATE_LIMIT" && u != "EXPIRES"
                })
                .map(|s| s.trim_matches('\'').trim_end_matches(',').to_string())
                .collect()
        })
        .unwrap_or_default();

    // Parse rate limit.
    let rate_limit = parts
        .iter()
        .position(|p| p.to_uppercase() == "RATE_LIMIT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Parse expires.
    let expires_days = parts
        .iter()
        .position(|p| p.to_uppercase() == "EXPIRES")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| {
            let s = s.trim_end_matches('d');
            s.parse::<u64>().ok()
        })
        .unwrap_or(0);

    let token = state.auth_api_keys.create_key(
        auth_user_id,
        identity.tenant_id.as_u64(),
        scopes,
        rate_limit,
        0, // burst = use default
        expires_days,
    );

    state.audit_record(
        crate::control::security::audit::AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("created auth API key for user '{auth_user_id}'"),
    );

    let mut row = Map::new();
    row.insert("auth_api_key".to_string(), JsonValue::String(token));
    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["auth_api_key".to_string()],
        vec![row],
    ))])
}

/// ROTATE AUTH KEY '<key_id>' [OVERLAP 24h]
pub fn rotate_auth_key(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    if parts.len() < 4 {
        return Err(err(
            "42601",
            "syntax: ROTATE AUTH KEY '<key_id>' [OVERLAP 24h]",
        ));
    }
    let key_id = parts[3].trim_matches('\'');
    let overlap_hours = parts
        .iter()
        .position(|p| p.to_uppercase() == "OVERLAP")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.trim_end_matches('h').parse::<u64>().ok())
        .unwrap_or(24);

    let new_token = state
        .auth_api_keys
        .rotate(key_id, overlap_hours)
        .ok_or_else(|| err("42704", format!("auth key '{key_id}' not found")))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("rotated auth key '{key_id}' (overlap {overlap_hours}h)"),
    );

    let mut row = Map::new();
    row.insert("new_auth_api_key".to_string(), JsonValue::String(new_token));
    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["new_auth_api_key".to_string()],
        vec![row],
    ))])
}

/// LIST AUTH KEYS [FOR AUTH USER '<id>']
pub fn list_auth_keys(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let user_filter = parts
        .iter()
        .position(|p| p.to_uppercase() == "USER")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.trim_matches('\''));

    let keys = if let Some(uid) = user_filter {
        state.auth_api_keys.list_for_user(uid)
    } else {
        state.auth_api_keys.list_all()
    };

    let columns = vec![
        "key_id".to_string(),
        "auth_user_id".to_string(),
        "scopes".to_string(),
        "rate_limit".to_string(),
        "expires_at".to_string(),
        "last_used_at".to_string(),
        "last_used_ip".to_string(),
    ];

    let rows: Vec<_> = keys
        .iter()
        .map(|k| {
            let mut row = Map::new();
            row.insert("key_id".to_string(), JsonValue::String(k.key_id.clone()));
            row.insert(
                "auth_user_id".to_string(),
                JsonValue::String(k.auth_user_id.clone()),
            );
            row.insert("scopes".to_string(), JsonValue::String(k.scopes.join(", ")));
            row.insert(
                "rate_limit".to_string(),
                JsonValue::String(k.rate_limit_qps.to_string()),
            );
            row.insert(
                "expires_at".to_string(),
                JsonValue::String(if k.expires_at == 0 {
                    "never".into()
                } else {
                    k.expires_at.to_string()
                }),
            );
            row.insert(
                "last_used_at".to_string(),
                JsonValue::String(if k.last_used_at == 0 {
                    "never".into()
                } else {
                    k.last_used_at.to_string()
                }),
            );
            row.insert(
                "last_used_ip".to_string(),
                JsonValue::String(k.last_used_ip.clone()),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
