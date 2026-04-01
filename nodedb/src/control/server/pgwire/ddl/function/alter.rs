//! `ALTER FUNCTION ... OWNER TO` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `ALTER FUNCTION <name> OWNER TO <new_owner>`
pub fn alter_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "alter functions")?;

    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER FUNCTION <name> OWNER TO <user> | SET (FUEL=N, MEMORY=N)",
        ));
    }

    let name = parts[2].to_lowercase();
    let action = parts[3].to_uppercase();

    // ALTER FUNCTION <name> SET (FUEL = N, MEMORY = N)
    if action == "SET" {
        return alter_function_limits(state, identity, &name, parts);
    }

    // ALTER FUNCTION <name> OWNER TO <new_owner>
    if action != "OWNER" || parts.len() < 6 || !parts[4].eq_ignore_ascii_case("TO") {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER FUNCTION <name> OWNER TO <user> | SET (FUEL=N, MEMORY=N)",
        ));
    }

    let new_owner = parts[5].trim_end_matches(';').to_string();

    let tenant_id = identity.tenant_id.as_u32();
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let mut func = catalog
        .get_function(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42883", &format!("function '{name}' does not exist")))?;

    let old_owner = func.owner.clone();
    func.owner = new_owner.clone();
    catalog
        .put_function(&func)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER FUNCTION {name} OWNER TO {new_owner} (was: {old_owner})"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER FUNCTION"))])
}

/// Handle `ALTER FUNCTION <name> SET (FUEL = N, MEMORY = N)`
fn alter_function_limits(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let mut func = catalog
        .get_function(tenant_id, name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42883", &format!("function '{name}' does not exist")))?;

    // Parse SET (...) from remaining parts.
    let rest = parts[4..].join(" ");
    let rest = rest
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim_end_matches(';');
    for part in rest.split(',') {
        let kv: Vec<&str> = part.split('=').map(str::trim).collect();
        if kv.len() != 2 {
            continue;
        }
        match kv[0].to_uppercase().as_str() {
            "FUEL" => {
                if let Ok(v) = kv[1].parse::<u64>() {
                    func.wasm_fuel = v;
                }
            }
            "MEMORY" => {
                if let Ok(v) = kv[1].parse::<usize>() {
                    func.wasm_memory = v;
                }
            }
            _ => {}
        }
    }

    catalog
        .put_function(&func)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "ALTER FUNCTION {name} SET (FUEL={}, MEMORY={})",
            func.wasm_fuel, func.wasm_memory
        ),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER FUNCTION"))])
}
