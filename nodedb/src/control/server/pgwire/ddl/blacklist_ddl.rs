//! Blacklist DDL commands.
//!
//! ```sql
//! BLACKLIST AUTH USER 'user_42' [UNTIL '2026-12-31T00:00:00Z'] REASON 'spam'
//! BLACKLIST IP '192.168.1.100' REASON 'abuse'
//! BLACKLIST IP '10.0.0.0/8' REASON 'blocked network'
//! SHOW BLACKLIST [IP | USER | ALL]
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// Handle BLACKLIST commands (AUTH USER or IP).
pub fn handle_blacklist(
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
        return Err(sqlstate_error(
            "42601",
            "syntax: BLACKLIST AUTH USER '<id>' [UNTIL '<timestamp>'] REASON '<reason>' | BLACKLIST IP '<addr>' REASON '<reason>'",
        ));
    }

    let upper1 = parts[1].to_uppercase();
    match upper1.as_str() {
        "AUTH" => handle_blacklist_user(state, identity, parts),
        "IP" => handle_blacklist_ip(state, identity, parts),
        _ => Err(sqlstate_error(
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
) -> PgWireResult<Vec<Response>> {
    // BLACKLIST AUTH USER '<id>' ...
    if parts.len() < 4 {
        return Err(sqlstate_error(
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
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("blacklisted user '{user_id}': {reason}"),
    );

    Ok(vec![Response::Execution(Tag::new("BLACKLIST"))])
}

/// BLACKLIST IP '<addr_or_cidr>' REASON '<reason>'
fn handle_blacklist_ip(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error(
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
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("blacklisted IP '{addr}': {reason}"),
    );

    Ok(vec![Response::Execution(Tag::new("BLACKLIST"))])
}

/// SHOW BLACKLIST [IP | USER | ALL]
pub fn show_blacklist(
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

    let kind_filter = parts
        .get(2)
        .map(|s| s.to_uppercase())
        .and_then(|s| match s.as_str() {
            "IP" => Some("ip"),
            "USER" => Some("user"),
            _ => None,
        });

    let entries = state.blacklist.list(kind_filter);

    let schema = Arc::new(vec![
        text_field("key"),
        text_field("kind"),
        text_field("reason"),
        text_field("created_by"),
        text_field("created_at"),
        text_field("expires_at"),
    ]);

    let rows: Vec<_> = entries
        .iter()
        .map(|e| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&e.key);
            let _ = enc.encode_field(&e.kind);
            let _ = enc.encode_field(&e.reason);
            let _ = enc.encode_field(&e.created_by);
            let _ = enc.encode_field(&e.created_at.to_string());
            let _ = enc.encode_field(&if e.expires_at == 0 {
                "permanent".to_string()
            } else {
                e.expires_at.to_string()
            });
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
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
