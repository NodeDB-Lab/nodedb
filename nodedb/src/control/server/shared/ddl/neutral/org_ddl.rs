// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral organization management DDL commands.
//!
//! ```sql
//! CREATE ORG 'acme' IN TENANT 1
//! ALTER ORG 'acme' SET STATUS suspended
//! DROP ORG 'acme'
//! SHOW ORGS [IN TENANT 1]
//! SHOW MEMBERS OF ORG 'acme'
//! ```
//!
//! Ported from the pgwire `ddl::org_ddl` handlers. The superuser gate, org-store
//! mutations, and `audit_record` side effects are preserved verbatim; only the
//! result construction changed from pgwire `Response` / `QueryResponse` / `Tag`
//! to the protocol-neutral [`DdlResult`] over [`ShapedRows`].

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

/// Build a single-tag status result.
fn status(command: impl Into<String>) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.into(),
        rows_affected: None,
    }]
}

/// Route org DDL commands.
pub fn handle_org(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.is_empty() {
        return Err(err("42601", "empty org command"));
    }
    let cmd = parts[0].to_uppercase();
    match cmd.as_str() {
        "CREATE" => create_org(state, identity, parts),
        "ALTER" => alter_org(state, identity, parts),
        "DROP" => drop_org(state, identity, parts),
        _ => Err(err("42601", "expected CREATE ORG, ALTER ORG, or DROP ORG")),
    }
}

/// CREATE ORG '<org_id>' IN TENANT <id>
fn create_org(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    // CREATE ORG '<name>' IN TENANT <id>
    if parts.len() < 3 {
        return Err(err("42601", "syntax: CREATE ORG '<name>' [IN TENANT <id>]"));
    }
    let org_id = parts[2].trim_matches('\'');
    let tenant_id = parts
        .iter()
        .position(|p| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| identity.tenant_id.as_u64());

    state
        .orgs
        .create_org(org_id, org_id, tenant_id)
        .map_err(|e| err("23505", e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("created org '{org_id}' in tenant {tenant_id}"),
    );

    Ok(status("CREATE ORG"))
}

/// ALTER ORG '<org_id>' SET STATUS <status>
fn alter_org(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    if parts.len() < 6 {
        return Err(err("42601", "syntax: ALTER ORG '<id>' SET STATUS <status>"));
    }
    let org_id = parts[2].trim_matches('\'');
    let status_val = parts[5].to_lowercase();

    let found = state
        .orgs
        .set_status(org_id, &status_val)
        .map_err(|e| err("XX000", e.to_string()))?;
    if !found {
        return Err(err("42704", format!("org '{org_id}' not found")));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("org '{org_id}' status set to {status_val}"),
    );

    Ok(status("ALTER ORG"))
}

/// DROP ORG '<org_id>'
fn drop_org(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    if parts.len() < 3 {
        return Err(err("42601", "syntax: DROP ORG '<org_id>'"));
    }
    let org_id = parts[2].trim_matches('\'');

    let found = state
        .orgs
        .drop_org(org_id)
        .map_err(|e| err("XX000", e.to_string()))?;
    if !found {
        return Err(err("42704", format!("org '{org_id}' not found")));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("dropped org '{org_id}'"),
    );

    Ok(status("DROP ORG"))
}

/// SHOW ORGS [IN TENANT <id>]
pub fn show_orgs(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_filter = parts
        .iter()
        .position(|p| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u64>().ok());

    let orgs = state.orgs.list(tenant_filter);

    let columns = vec![
        "org_id".to_string(),
        "name".to_string(),
        "tenant_id".to_string(),
        "status".to_string(),
    ];

    let rows: Vec<_> = orgs
        .iter()
        .map(|o| {
            let mut row = Map::new();
            row.insert("org_id".to_string(), JsonValue::String(o.org_id.clone()));
            row.insert("name".to_string(), JsonValue::String(o.name.clone()));
            row.insert(
                "tenant_id".to_string(),
                JsonValue::String(o.tenant_id.to_string()),
            );
            row.insert("status".to_string(), JsonValue::String(o.status.clone()));
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// SHOW MEMBERS OF ORG '<org_id>'
pub fn show_members(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // SHOW MEMBERS OF ORG '<id>'
    let org_id = parts
        .iter()
        .position(|p| p.to_uppercase() == "ORG")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.trim_matches('\''))
        .ok_or_else(|| err("42601", "syntax: SHOW MEMBERS OF ORG '<org_id>'"))?;

    let members = state.orgs.members_of(org_id);

    let columns = vec![
        "user_id".to_string(),
        "org_id".to_string(),
        "role".to_string(),
        "joined_at".to_string(),
    ];

    let rows: Vec<_> = members
        .iter()
        .map(|m| {
            let mut row = Map::new();
            row.insert(
                "user_id".to_string(),
                JsonValue::String(m.auth_user_id.clone()),
            );
            row.insert("org_id".to_string(), JsonValue::String(m.org_id.clone()));
            row.insert("role".to_string(), JsonValue::String(m.role.clone()));
            row.insert(
                "joined_at".to_string(),
                JsonValue::String(m.joined_at.to_string()),
            );
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
