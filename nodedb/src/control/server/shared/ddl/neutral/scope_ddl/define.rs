// SPDX-License-Identifier: BUSL-1.1

//! Scope definition lifecycle: `DEFINE SCOPE`, `DROP SCOPE`.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{err, status};

/// DEFINE SCOPE '<name>' AS <perm> ON <coll> [, <perm> ON <coll>] [INCLUDE '<scope>']
pub fn define_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    // DEFINE SCOPE '<name>' AS ...
    if parts.len() < 4 {
        return Err(err(
            "42601",
            "syntax: DEFINE SCOPE '<name>' AS <perm> ON <coll> [, ...] [INCLUDE '<scope>']",
        ));
    }

    let scope_name = parts[2].trim_matches('\'');
    // Everything after "AS" is the definition.
    let as_idx = parts
        .iter()
        .position(|p| p.to_uppercase() == "AS")
        .ok_or_else(|| err("42601", "missing AS keyword"))?;

    let def_parts = &parts[as_idx + 1..];

    let mut grants = Vec::new();
    let mut includes = Vec::new();

    let mut i = 0;
    while i < def_parts.len() {
        let token = def_parts[i].to_uppercase();
        match token.as_str() {
            "INCLUDE" => {
                if i + 1 < def_parts.len() {
                    let inc = def_parts[i + 1].trim_matches('\'').trim_end_matches(',');
                    includes.push(inc.to_string());
                    i += 2;
                } else {
                    return Err(err("42601", "INCLUDE requires a scope name"));
                }
            }
            "READ" | "WRITE" | "CREATE" | "DROP" | "ALTER" | "ADMIN" | "BACKUP" => {
                // <perm> ON <collection>. `BACKUP ON 'tenant:<id>'` covers
                // the backups and restores of that tenant.
                if i + 2 < def_parts.len() && def_parts[i + 1].to_uppercase() == "ON" {
                    let coll = def_parts[i + 2].trim_matches('\'').trim_end_matches(',');
                    grants.push((token.to_lowercase(), coll.to_string()));
                    i += 3;
                } else {
                    return Err(err("42601", "expected <perm> ON <collection>"));
                }
            }
            _ => {
                // Skip commas and unknown tokens.
                i += 1;
            }
        }
    }

    super::super::audit_intent::record_action_intent(
        state,
        identity,
        Some(identity.tenant_id),
        &format!("DEFINE SCOPE '{scope_name}'"),
    )?;
    state
        .scope_defs
        .define(scope_name, grants, includes, &identity.username)
        .map_err(|e| err("42601", e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("defined scope '{scope_name}'"),
    );

    Ok(status("DEFINE SCOPE"))
}

/// DROP SCOPE '<name>'
pub fn drop_scope(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err("42501", "permission denied: requires superuser"));
    }
    if parts.len() < 3 {
        return Err(err("42601", "syntax: DROP SCOPE '<name>'"));
    }
    let name = parts[2].trim_matches('\'');

    super::super::audit_intent::record_action_intent(
        state,
        identity,
        Some(identity.tenant_id),
        &format!("DROP SCOPE '{name}'"),
    )?;
    let found = state
        .scope_defs
        .drop_scope(name)
        .map_err(|e| DdlError::from_error(&e))?;
    if !found {
        return Err(err("42704", format!("scope '{name}' not found")));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("dropped scope '{name}'"),
    );

    Ok(status("DROP SCOPE"))
}
