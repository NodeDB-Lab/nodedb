// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral session management DDL commands.
//!
//! ```sql
//! SHOW SESSIONS
//! KILL SESSION '<session_id>'
//! KILL USER SESSIONS '<auth_user_id>'
//! VERIFY AUDIT CHAIN
//! ```
//!
//! Ported from the pgwire `ddl::session_ddl` handlers. All four read or
//! mutate the GLOBAL `state.session_registry` / `state.audit` — not any
//! per-connection session state — so they carry no per-connection state. The
//! superuser / cluster_admin / database_owner gates, the race-condition
//! handling in `kill_session` (the disappeared-session audit branch), and the
//! audit records are preserved verbatim; only the result construction changed
//! from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].
//!
//! Named `session_admin` (not `session`) to avoid collision with the
//! unrelated `shared::session` module (per-connection `SessionStore`).
//!
//! `VERIFY AUDIT CHAIN` (this file, space-separated, full-chain) is distinct
//! from the `SELECT VERIFY_AUDIT_CHAIN(from_seq, to_seq)` query function in
//! `neutral::query_functions` (underscore-separated, ranged) — the two never
//! overlap textually. Both read the node-wide audit log and both require
//! superuser.

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

/// SHOW SESSIONS
pub fn show_sessions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    let sessions = state.session_registry.list_all();

    let columns = vec![
        "session_id".to_string(),
        "user_id".to_string(),
        "db_user".to_string(),
        "auth_method".to_string(),
        "connected_at".to_string(),
        "last_active".to_string(),
        "idle_seconds".to_string(),
        "client_ip".to_string(),
        "protocol".to_string(),
        "current_database".to_string(),
        "bytes_in".to_string(),
        "bytes_out".to_string(),
        "current_statement".to_string(),
        "token_expires_in_secs".to_string(),
    ];

    let rows: Vec<Map<String, JsonValue>> = sessions
        .iter()
        .map(|s| {
            let mut row = Map::new();
            row.insert(
                "session_id".to_string(),
                JsonValue::String(s.session_id.clone()),
            );
            row.insert(
                "user_id".to_string(),
                JsonValue::String(s.user_id.to_string()),
            );
            row.insert("db_user".to_string(), JsonValue::String(s.db_user.clone()));
            row.insert(
                "auth_method".to_string(),
                JsonValue::String(s.auth_method.clone()),
            );
            row.insert(
                "connected_at".to_string(),
                JsonValue::String(s.connected_at.to_string()),
            );
            row.insert(
                "last_active".to_string(),
                JsonValue::String(s.last_active.to_string()),
            );
            row.insert(
                "idle_seconds".to_string(),
                JsonValue::String(s.idle_seconds.to_string()),
            );
            row.insert(
                "client_ip".to_string(),
                JsonValue::String(s.client_ip.clone()),
            );
            row.insert(
                "protocol".to_string(),
                JsonValue::String(s.protocol.clone()),
            );
            row.insert(
                "current_database".to_string(),
                JsonValue::String(s.current_database.as_u64().to_string()),
            );
            row.insert(
                "bytes_in".to_string(),
                JsonValue::String(s.bytes_in.to_string()),
            );
            row.insert(
                "bytes_out".to_string(),
                JsonValue::String(s.bytes_out.to_string()),
            );
            let current_stmt = s
                .current_statement_digest
                .as_deref()
                .unwrap_or("")
                .to_string();
            row.insert(
                "current_statement".to_string(),
                JsonValue::String(current_stmt),
            );
            let token_exp = s
                .token_expires_in_seconds
                .map(|v| v.to_string())
                .unwrap_or_default();
            row.insert(
                "token_expires_in_secs".to_string(),
                JsonValue::String(token_exp),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// KILL SESSION '<session_id>'
pub fn kill_session(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 3 {
        return Err(err("42601", "syntax: KILL SESSION '<session_id>'"));
    }
    let session_id = parts[2].trim_matches('\'');

    // Resolve target's bound database WITHOUT killing — needed for the
    // DatabaseOwner authority branch and to surface a precise audit row.
    let target_db = match state.session_registry.lookup_session_database(session_id) {
        Some(db) => db,
        None => {
            return Err(err("42704", format!("session '{session_id}' not found")));
        }
    };

    // Permission: Superuser, ClusterAdmin, or DatabaseOwner of the session's db.
    let authorized = identity.is_superuser
        || identity.has_cluster_admin()
        || identity.is_database_owner(target_db);
    if !authorized {
        state.audit_record_with_db(
            crate::control::security::audit::AuditEvent::PermissionDenied,
            Some(identity.tenant_id),
            Some(target_db),
            &identity.username,
            &format!("KILL SESSION '{session_id}'"),
        );
        return Err(err(
            "42501",
            "permission denied: KILL SESSION requires superuser, cluster_admin, or database_owner of the session's database",
        ));
    }

    // Now signal the kill. The session may have disconnected between the
    // permission check above and this call; `kill_session_by_id` returns
    // `None` in that race, in which case no kill signal was sent and we
    // must not record `SessionRevoked` (which would be a false audit).
    match state.session_registry.kill_session_by_id(
        session_id,
        crate::control::security::sessions::KillReason::AdminKill,
    ) {
        Some(_db) => {
            state.audit_record_with_db(
                crate::control::security::audit::AuditEvent::SessionRevoked,
                Some(identity.tenant_id),
                Some(target_db),
                &identity.username,
                &format!("killed session '{session_id}' by {}", identity.username),
            );
            Ok(status("KILL SESSION"))
        }
        None => {
            // Session disappeared between authority check and kill — return
            // a precise error to the client and a separate audit record so
            // operators can distinguish "kill applied" from "kill raced".
            state.audit_record_with_db(
                crate::control::security::audit::AuditEvent::AdminAction,
                Some(identity.tenant_id),
                Some(target_db),
                &identity.username,
                &format!(
                    "KILL SESSION '{session_id}' raced — session disconnected before kill applied"
                ),
            );
            Err(err(
                "42704",
                format!("session '{session_id}' disconnected before KILL applied"),
            ))
        }
    }
}

/// KILL USER SESSIONS '<auth_user_id>'
pub fn kill_user_sessions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    if parts.len() < 4 {
        return Err(err("42601", "syntax: KILL USER SESSIONS '<auth_user_id>'"));
    }
    let user_id_str = parts[3].trim_matches('\'');
    let user_id: u64 = user_id_str.parse().map_err(|_| {
        err(
            "22003",
            format!("invalid user_id '{user_id_str}': must be numeric"),
        )
    })?;

    let killed = state.session_registry.kill_sessions_for_user(
        user_id,
        crate::control::security::sessions::KillReason::AdminKill,
    );

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("killed {killed} sessions for user_id={user_id}"),
    );

    Ok(status(&format!("KILL {killed}")))
}

/// VERIFY AUDIT CHAIN
pub fn verify_audit_chain(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }

    let audit = state.audit.lock().unwrap_or_else(|p| p.into_inner());
    match audit.verify_chain() {
        Ok(()) => {
            let columns = vec!["status".to_string(), "entries".to_string()];
            let mut row = Map::new();
            row.insert("status".to_string(), JsonValue::String("VALID".to_string()));
            row.insert(
                "entries".to_string(),
                JsonValue::String(audit.len().to_string()),
            );
            Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
                columns,
                vec![row],
            ))])
        }
        Err(broken_seq) => Err(err(
            "XX001",
            format!("audit chain broken at sequence {broken_seq}"),
        )),
    }
}
