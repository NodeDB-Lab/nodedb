//! Auth user management DDL commands (JIT-provisioned users).
//!
//! ```sql
//! ALTER AUTH USER 'user_42' SET STATUS active|suspended|banned|restricted|read_only
//! DEACTIVATE AUTH USER 'user_42'
//! PURGE AUTH USERS INACTIVE FOR 90d
//! SHOW AUTH USERS
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::auth_context::AuthStatus;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// Handle ALTER AUTH USER or DEACTIVATE AUTH USER commands.
pub fn handle_auth_user(
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

    let upper0 = parts[0].to_uppercase();
    match upper0.as_str() {
        "DEACTIVATE" => deactivate_auth_user(state, identity, parts),
        "ALTER" => alter_auth_user_status(state, identity, parts),
        _ => Err(sqlstate_error(
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
) -> PgWireResult<Vec<Response>> {
    // DEACTIVATE AUTH USER '<id>'
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DEACTIVATE AUTH USER '<user_id>'",
        ));
    }

    let user_id = parts[3].trim_matches('\'');

    let found = state
        .auth_users
        .deactivate(user_id)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    if !found {
        return Err(sqlstate_error(
            "42704",
            &format!("auth user '{user_id}' not found"),
        ));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("deactivated auth user '{user_id}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DEACTIVATE"))])
}

/// ALTER AUTH USER '<user_id>' SET STATUS <status>
fn alter_auth_user_status(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // ALTER AUTH USER '<id>' SET STATUS <status>
    if parts.len() < 7 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER AUTH USER '<user_id>' SET STATUS <active|suspended|banned|restricted|read_only>",
        ));
    }

    let user_id = parts[3].trim_matches('\'');
    let status_str = parts[6].to_lowercase();
    let status: AuthStatus = status_str
        .parse()
        .map_err(|e: String| sqlstate_error("42601", &e))?;

    let found = state
        .auth_users
        .set_status(user_id, status)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    if !found {
        return Err(sqlstate_error(
            "42704",
            &format!("auth user '{user_id}' not found"),
        ));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("auth user '{user_id}' status set to {status}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER AUTH USER"))])
}

/// PURGE AUTH USERS INACTIVE FOR <duration>
///
/// Duration format: `90d` (days), `24h` (hours).
pub fn purge_auth_users(
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

    // PURGE AUTH USERS INACTIVE FOR <duration>
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: PURGE AUTH USERS INACTIVE FOR <duration> (e.g., 90d, 24h)",
        ));
    }

    let duration_str = parts[5];
    let threshold_secs = parse_duration_secs(duration_str).ok_or_else(|| {
        sqlstate_error(
            "42601",
            &format!("invalid duration: '{duration_str}'. Use 90d or 24h"),
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
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("purged {purged} inactive auth users (older than {duration_str})"),
    );

    Ok(vec![Response::Execution(Tag::new(&format!(
        "PURGE {purged}"
    )))])
}

/// SHOW AUTH USERS
pub fn show_auth_users(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }

    let users = state.auth_users.list(false);

    let schema = Arc::new(vec![
        text_field("id"),
        text_field("username"),
        text_field("email"),
        text_field("tenant_id"),
        text_field("provider"),
        text_field("status"),
        text_field("is_active"),
        text_field("last_seen"),
    ]);

    let rows: Vec<_> = users
        .iter()
        .map(|u| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&u.id);
            let _ = enc.encode_field(&u.username);
            let _ = enc.encode_field(&u.email);
            let _ = enc.encode_field(&u.tenant_id.to_string());
            let _ = enc.encode_field(&u.provider);
            let _ = enc.encode_field(&u.status.to_string());
            let _ = enc.encode_field(&u.is_active.to_string());
            let _ = enc.encode_field(&u.last_seen.to_string());
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
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
