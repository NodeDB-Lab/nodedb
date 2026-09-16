// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral scope query DDL commands: ALTER SCOPE, SHOW MY SCOPES,
//! SHOW SCOPES FOR.
//!
//! Ported from the pgwire `ddl::scope_query_ddl` handlers. The superuser gate,
//! `scope_defs` / `scope_grants` / `orgs` catalog reads and mutations, and
//! `audit_record` side effects are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `QueryResponse` / `Tag` to the
//! protocol-neutral [`DdlResult`] over [`ShapedRows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`], preserving the exact SQLSTATE codes and messages
/// the pgwire handlers produced (via `sqlstate_error`).
fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// ALTER SCOPE '<name>' SET GRANTS <perm> ON <coll> [, ...] [INCLUDE '<scope>']
pub fn alter_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    if parts.len() < 5 {
        return Err(err(
            "42601",
            "syntax: ALTER SCOPE '<name>' SET GRANTS <perm> ON <coll> [, ...] [INCLUDE '<scope>']",
        ));
    }

    let scope_name = parts[2].trim_matches('\'');
    let set_idx = parts
        .iter()
        .position(|p| p.to_uppercase() == "SET")
        .ok_or_else(|| err("42601", "missing SET keyword"))?;

    let def_parts = &parts[set_idx + 1..];
    let mut grants = Vec::new();
    let mut includes = Vec::new();
    let mut has_grants = false;

    let mut i = 0;
    while i < def_parts.len() {
        let token = def_parts[i].to_uppercase();
        match token.as_str() {
            "GRANTS" => {
                has_grants = true;
                i += 1;
            }
            "INCLUDE" if i + 1 < def_parts.len() => {
                includes.push(
                    def_parts[i + 1]
                        .trim_matches('\'')
                        .trim_end_matches(',')
                        .to_string(),
                );
                i += 2;
            }
            "READ" | "WRITE" | "CREATE" | "DROP" | "ALTER" | "ADMIN"
                if i + 2 < def_parts.len() && def_parts[i + 1].to_uppercase() == "ON" =>
            {
                grants.push((
                    token.to_lowercase(),
                    def_parts[i + 2]
                        .trim_matches('\'')
                        .trim_end_matches(',')
                        .to_string(),
                ));
                i += 3;
            }
            _ => {
                i += 1;
            }
        }
    }

    let grants_opt = if has_grants || !grants.is_empty() {
        Some(grants)
    } else {
        None
    };
    let includes_opt = if !includes.is_empty() {
        Some(includes)
    } else {
        None
    };

    let found = state
        .scope_defs
        .alter(scope_name, grants_opt, includes_opt)
        .map_err(|e| err("42601", e.to_string()))?;

    if !found {
        return Err(err("42704", format!("scope '{scope_name}' not found")));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("altered scope '{scope_name}'"),
    );

    Ok(vec![DdlResult::Status {
        command: "ALTER SCOPE".to_string(),
        rows_affected: None,
    }])
}

/// SHOW MY SCOPES — show effective scopes for the current user.
pub fn show_my_scopes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let user_id = identity.user_id.to_string();
    let org_ids = state.orgs.orgs_for_user(&user_id);
    let effective = state.scope_grants.effective_scopes(&user_id, &org_ids);

    let columns = vec!["scope".to_string(), "source".to_string()];

    let mut rows = Vec::new();
    for scope_name in &effective {
        let source = if state
            .scope_grants
            .scopes_for("user", &user_id)
            .contains(scope_name)
        {
            "direct"
        } else {
            "org"
        };
        let mut row = Map::new();
        row.insert("scope".to_string(), JsonValue::String(scope_name.clone()));
        row.insert("source".to_string(), JsonValue::String(source.to_string()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// SHOW SCOPES FOR USER '<id>' / SHOW SCOPES FOR ORG '<id>'
pub fn show_scopes_for(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // SHOW SCOPES FOR <USER|ORG> '<id>'
    if parts.len() < 5 {
        return Err(err("42601", "syntax: SHOW SCOPES FOR <USER|ORG> '<id>'"));
    }

    let grantee_type = parts[3].to_lowercase();
    let grantee_id = parts[4].trim_matches('\'');

    let scopes = match grantee_type.as_str() {
        "user" => {
            let org_ids = state.orgs.orgs_for_user(grantee_id);
            state.scope_grants.effective_scopes(grantee_id, &org_ids)
        }
        "org" => state
            .scope_grants
            .scopes_for("org", grantee_id)
            .into_iter()
            .collect(),
        _ => return Err(err("42601", "expected USER or ORG")),
    };

    let columns = vec!["scope".to_string()];
    let rows: Vec<_> = scopes
        .iter()
        .map(|s| {
            let mut row = Map::new();
            row.insert("scope".to_string(), JsonValue::String(s.clone()));
            row
        })
        .collect();

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
