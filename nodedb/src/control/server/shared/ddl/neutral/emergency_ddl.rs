// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral emergency & incident response DDL commands.
//!
//! ```sql
//! EMERGENCY LOCKDOWN REASON 'security incident'
//! EMERGENCY UNLOCK
//! BLACKLIST AUTH USERS WHERE email LIKE '%@compromised.com' WITH KILL SESSIONS
//! ```
//!
//! The superuser gates, two-party approval check, emergency-state mutation,
//! blacklist / session side effects, and audit records run here. The result
//! is the protocol-neutral [`DdlResult`] with its SQLSTATE codes, messages,
//! and command tags.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`] from a SQLSTATE code and a message.
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

/// EMERGENCY LOCKDOWN REASON '...'
pub fn emergency_lockdown(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    // Check two-party authorization if configured.
    if state.emergency.requires_two_party("EMERGENCY LOCKDOWN")
        && state
            .emergency
            .submit_two_party_approval("EMERGENCY LOCKDOWN", &identity.username)
    {
        return Err(err(
            "42000",
            "two-party authorization required: waiting for second admin approval",
        ));
    }

    let reason = parts
        .iter()
        .position(|p| p.to_uppercase() == "REASON")
        .map(|i| parts[i + 1..].join(" ").trim_matches('\'').to_string())
        .unwrap_or_else(|| "no reason provided".into());

    state.emergency.lockdown(&reason);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("EMERGENCY LOCKDOWN: {reason}"),
    );

    Ok(status("EMERGENCY LOCKDOWN"))
}

/// EMERGENCY UNLOCK
pub fn emergency_unlock(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    // An unlock restores access, so its record comes first.
    // A lockdown removes access, so it runs even when the audit log is down.
    super::audit_intent::record_action_intent(
        state,
        identity,
        Some(identity.tenant_id),
        "EMERGENCY UNLOCK",
    )?;
    state.emergency.unlock();

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        "EMERGENCY UNLOCK",
    );

    Ok(status("EMERGENCY UNLOCK"))
}

/// BLACKLIST AUTH USERS WHERE email LIKE '%@compromised.com' [WITH KILL SESSIONS]
pub fn bulk_blacklist(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    // Parse: BLACKLIST AUTH USERS WHERE email LIKE '<pattern>' [WITH KILL SESSIONS]
    let like_idx = parts
        .iter()
        .position(|p| p.to_uppercase() == "LIKE")
        .ok_or_else(|| err("42601", "missing LIKE clause"))?;
    let pattern = parts
        .get(like_idx + 1)
        .map(|s| s.trim_matches('\''))
        .unwrap_or("");

    let kill_sessions = parts.iter().any(|p| p.to_uppercase() == "KILL");

    // Find matching auth users.
    let all_users = state.auth_users.list(false);
    let mut blacklisted_count = 0u32;
    let mut killed_count = 0usize;

    for user in &all_users {
        let matches = crate::bridge::scan_filter::sql_like_match(&user.email, pattern, false)
            || crate::bridge::scan_filter::sql_like_match(&user.username, pattern, false);
        if matches {
            let _ = state.blacklist.blacklist_user(
                &user.id,
                &format!("bulk blacklist: pattern '{pattern}'"),
                &identity.username,
                0, // Permanent.
            );
            blacklisted_count += 1;

            if kill_sessions {
                killed_count += state.session_registry.kill_sessions_for_principal(
                    &user.id,
                    crate::control::security::sessions::KillReason::AdminKill,
                );
            }
        }
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "bulk blacklist: pattern '{pattern}', {blacklisted_count} users, {killed_count} sessions killed"
        ),
    );

    Ok(status(format!("BLACKLIST {blacklisted_count}")))
}
