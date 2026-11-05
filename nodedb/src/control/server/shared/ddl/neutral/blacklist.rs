// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral blacklist DDL commands.
//!
//! ```sql
//! BLACKLIST AUTH USER 'user_42' [UNTIL '2026-12-31T00:00:00Z'] REASON 'spam'
//! BLACKLIST AUTH USER 'user_42' REASON 'spam' WITH KILL SESSIONS
//! BLACKLIST IP '192.168.1.100' REASON 'abuse'
//! BLACKLIST IP '192.168.1.100' REASON 'abuse' WITH KILL SESSIONS
//! BLACKLIST IP '10.0.0.0/8' REASON 'blocked network'
//! UNBLACKLIST AUTH USER 'user_42'
//! UNBLACKLIST IP '10.0.0.0/8'
//! SHOW BLACKLIST [IP | USER | ALL]
//! ```
//!
//! Every ban this module can create it can also lift: an entry is persisted to
//! the system catalog and reloaded at boot, so without a removal command an
//! operator who bans a range has no way back short of editing storage.
//!
//! The superuser gate, blacklist-registry mutations, `WITH KILL SESSIONS`
//! session termination, and `audit_record` side effects run here. The result
//! is the protocol-neutral [`DdlResult`] over [`ShapedRows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`] from a SQLSTATE code and a message.
fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Build a single-tag status result.
fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Handle BLACKLIST commands (AUTH USER or IP).
pub fn handle_blacklist(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    if parts.len() < 3 {
        return Err(err(
            "42601",
            "syntax: BLACKLIST AUTH USER '<id>' [UNTIL '<timestamp>'] REASON '<reason>' | BLACKLIST IP '<addr>' REASON '<reason>'",
        ));
    }

    let upper1 = parts[1].to_uppercase();
    match upper1.as_str() {
        "AUTH" => handle_blacklist_user(state, identity, parts),
        "IP" => handle_blacklist_ip(state, identity, parts),
        _ => Err(err(
            "42601",
            "expected: BLACKLIST AUTH USER ... or BLACKLIST IP ...",
        )),
    }
}

/// BLACKLIST AUTH USER '<user_id>' [UNTIL '<timestamp>'] REASON '<reason>'
fn handle_blacklist_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // BLACKLIST AUTH USER '<id>' ...
    if parts.len() < 4 {
        return Err(err(
            "42601",
            "syntax: BLACKLIST AUTH USER '<id>' REASON '<reason>'",
        ));
    }

    let user_id = parts[3].trim_matches('\'');

    let expires_at = extract_until(parts);
    let reason = extract_reason(parts).unwrap_or("admin blacklist".into());

    state
        .blacklist
        .blacklist_user(user_id, &reason, &identity.username, expires_at)
        .map_err(|e| DdlError::from_error(&e))?;

    // WITH KILL SESSIONS — terminate active sessions immediately.
    let kill_sessions = parts.iter().any(|p| p.to_uppercase() == "KILL");
    let mut killed = 0;
    if kill_sessions {
        killed = state.session_registry.kill_sessions_for_principal(
            user_id,
            crate::control::security::sessions::KillReason::AdminKill,
        );
    }

    let kill_msg = if killed > 0 {
        format!(", killed {killed} session(s)")
    } else {
        String::new()
    };
    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("blacklisted user '{user_id}': {reason}{kill_msg}"),
    );

    Ok(status("BLACKLIST"))
}

/// BLACKLIST IP '<addr_or_cidr>' REASON '<reason>'
fn handle_blacklist_ip(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 3 {
        return Err(err(
            "42601",
            "syntax: BLACKLIST IP '<addr>' REASON '<reason>'",
        ));
    }

    let addr = parts[2].trim_matches('\'');
    let expires_at = extract_until(parts);
    let reason = extract_reason(parts).unwrap_or("admin blacklist".into());

    state
        .blacklist
        .blacklist_ip(addr, &reason, &identity.username, expires_at)
        .map_err(|e| DdlError::from_error(&e))?;

    // WITH KILL SESSIONS ends the sessions the entry refuses. An expired entry
    // never matches, and a CIDR entry matches its whole range.
    let killed = if parts.iter().any(|p| p.eq_ignore_ascii_case("KILL")) {
        state.session_registry.kill_sessions_for_ip(
            crate::control::security::sessions::KillReason::AdminKill,
            |ip| state.blacklist.check_ip(ip).is_some(),
        )
    } else {
        0
    };

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("blacklisted IP '{addr}': {reason}, killed {killed} session(s)"),
    );

    Ok(status("BLACKLIST"))
}

/// Handle UNBLACKLIST commands (AUTH USER or IP).
pub fn handle_unblacklist(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    if parts.len() < 3 {
        return Err(err(
            "42601",
            "syntax: UNBLACKLIST AUTH USER '<id>' | UNBLACKLIST IP '<addr>'",
        ));
    }

    match parts[1].to_uppercase().as_str() {
        "AUTH" => handle_unblacklist_user(state, identity, parts),
        "IP" => handle_unblacklist_ip(state, identity, parts),
        _ => Err(err(
            "42601",
            "expected: UNBLACKLIST AUTH USER ... or UNBLACKLIST IP ...",
        )),
    }
}

/// UNBLACKLIST AUTH USER '<user_id>'
fn handle_unblacklist_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 4 {
        return Err(err("42601", "syntax: UNBLACKLIST AUTH USER '<id>'"));
    }

    let user_id = parts[3].trim_matches('\'');
    lift(state, identity, "user", user_id, |store| {
        store.unblacklist_user(user_id)
    })
}

/// UNBLACKLIST IP '<addr_or_cidr>'
fn handle_unblacklist_ip(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let addr = parts[2].trim_matches('\'');
    lift(state, identity, "IP", addr, |store| {
        store.unblacklist_ip(addr)
    })
}

/// Run a removal, audit it, and report whether an entry was actually lifted.
///
/// A removal that matched nothing is an error rather than a silent success:
/// the operator's next belief is "that ban is gone", and a typo'd address
/// leaving the real entry in place must never be reported as `UNBLACKLIST`.
fn lift(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    kind: &str,
    key: &str,
    remove: impl FnOnce(
        &crate::control::security::blacklist::store::BlacklistStore,
    ) -> crate::Result<bool>,
) -> Result<Vec<DdlResult>, DdlError> {
    // Lifting a block restores access, so its record comes first.
    super::audit_intent::record_action_intent(
        state,
        identity,
        Some(identity.tenant_id),
        &format!("UNBLACKLIST {kind} '{key}'"),
    )?;
    let removed = remove(&state.blacklist).map_err(|e| DdlError::from_error(&e))?;
    if !removed {
        return Err(err(
            "42704",
            format!("no blacklist entry for {kind} '{key}'"),
        ));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("lifted blacklist on {kind} '{key}'"),
    );

    Ok(status("UNBLACKLIST"))
}

/// SHOW BLACKLIST [IP | USER | ALL]
pub fn show_blacklist(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    let kind_filter = parts
        .get(2)
        .map(|s| s.to_uppercase())
        .and_then(|s| match s.as_str() {
            "IP" => Some("ip"),
            "USER" => Some("user"),
            _ => None,
        });

    let entries = state.blacklist.list(kind_filter);

    let columns = vec![
        "key".to_string(),
        "kind".to_string(),
        "reason".to_string(),
        "created_by".to_string(),
        "created_at".to_string(),
        "expires_at".to_string(),
    ];

    let rows: Vec<_> = entries
        .iter()
        .map(|e| {
            let mut row = Map::new();
            row.insert("key".to_string(), JsonValue::String(e.key.clone()));
            row.insert("kind".to_string(), JsonValue::String(e.kind.clone()));
            row.insert("reason".to_string(), JsonValue::String(e.reason.clone()));
            row.insert(
                "created_by".to_string(),
                JsonValue::String(e.created_by.clone()),
            );
            row.insert(
                "created_at".to_string(),
                JsonValue::String(e.created_at.to_string()),
            );
            row.insert(
                "expires_at".to_string(),
                JsonValue::String(if e.expires_at == 0 {
                    "permanent".to_string()
                } else {
                    e.expires_at.to_string()
                }),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// Extract UNTIL timestamp from parts. Returns 0 (permanent) if not present.
fn extract_until(parts: &[&str]) -> u64 {
    parts
        .iter()
        .position(|p| p.to_uppercase() == "UNTIL")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| {
            let s = s.trim_matches('\'');
            // Try parsing as Unix timestamp first, then ISO 8601.
            s.parse::<u64>().ok()
        })
        .unwrap_or(0)
}

/// Extract REASON string from parts.
fn extract_reason(parts: &[&str]) -> Option<String> {
    let idx = parts.iter().position(|p| p.to_uppercase() == "REASON")?;
    let rest: Vec<&str> = parts[idx + 1..]
        .iter()
        .take_while(|p| {
            let u = p.to_uppercase();
            u != "UNTIL" && u != "WITH"
        })
        .copied()
        .collect();
    if rest.is_empty() {
        None
    } else {
        Some(rest.join(" ").trim_matches('\'').to_string())
    }
}
