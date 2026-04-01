//! `DROP FUNCTION [IF EXISTS]` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};
use super::parse::validate_identifier;

/// Handle `DROP FUNCTION [IF EXISTS] <name>`
///
/// Requires superuser or tenant_admin — same privilege level as CREATE FUNCTION.
pub fn drop_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop functions")?;

    let (name, if_exists) = parse_drop_function(parts)?;
    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    // Check if function exists.
    let func_exists = catalog
        .get_function(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog read: {e}")))?
        .is_some();

    if !func_exists && !if_exists {
        return Err(sqlstate_error(
            "42883",
            &format!("function '{name}' does not exist"),
        ));
    }

    if !func_exists {
        // IF EXISTS and function doesn't exist — no-op.
        return Ok(vec![Response::Execution(Tag::new("DROP FUNCTION"))]);
    }

    // Check dependencies: block DROP if other objects depend on this function.
    let dependents = catalog
        .find_dependents(tenant_id, "function", &name)
        .map_err(|e| sqlstate_error("XX000", &format!("dependency check: {e}")))?;
    if !dependents.is_empty() {
        let dep_list: Vec<String> = dependents
            .iter()
            .map(|(t, n)| format!("{t} '{n}'"))
            .collect();
        return Err(sqlstate_error(
            "2BP01",
            &format!(
                "cannot drop function '{name}': depended on by {}",
                dep_list.join(", ")
            ),
        ));
    }

    // If the function is a WASM function, clean up the stored binary.
    if let Ok(Some(func)) = catalog.get_function(tenant_id, &name)
        && let Some(ref hash) = func.wasm_hash
    {
        let _ = crate::control::planner::wasm::store::delete_wasm_binary(catalog, hash);
    }

    // Delete function definition + dependencies + ownership.
    catalog
        .delete_function(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
    let _ = catalog.delete_dependencies("function", tenant_id, &name);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP FUNCTION {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP FUNCTION"))])
}

/// Parse `DROP FUNCTION [IF EXISTS] <name>`.
fn parse_drop_function(parts: &[&str]) -> PgWireResult<(String, bool)> {
    // parts[0] = "DROP", parts[1] = "FUNCTION", ...
    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP FUNCTION [IF EXISTS] <name>",
        ));
    }

    let mut idx = 2;
    let if_exists = if parts.len() > 4
        && parts[2].eq_ignore_ascii_case("IF")
        && parts[3].eq_ignore_ascii_case("EXISTS")
    {
        idx = 4;
        true
    } else {
        false
    };

    if idx >= parts.len() {
        return Err(sqlstate_error("42601", "function name required"));
    }

    let name = parts[idx].to_lowercase().trim_end_matches(';').to_string();
    validate_identifier(&name)?;
    Ok((name, if_exists))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_drop_basic() {
        let parts: Vec<&str> = "DROP FUNCTION normalize_email".split_whitespace().collect();
        let (name, if_exists) = parse_drop_function(&parts).unwrap();
        assert_eq!(name, "normalize_email");
        assert!(!if_exists);
    }

    #[test]
    fn parse_drop_if_exists() {
        let parts: Vec<&str> = "DROP FUNCTION IF EXISTS myf".split_whitespace().collect();
        let (name, if_exists) = parse_drop_function(&parts).unwrap();
        assert_eq!(name, "myf");
        assert!(if_exists);
    }

    #[test]
    fn parse_drop_with_semicolon() {
        let parts: Vec<&str> = "DROP FUNCTION myf;".split_whitespace().collect();
        let (name, _) = parse_drop_function(&parts).unwrap();
        assert_eq!(name, "myf");
    }

    #[test]
    fn parse_drop_too_short() {
        let parts: Vec<&str> = "DROP FUNCTION".split_whitespace().collect();
        assert!(parse_drop_function(&parts).is_err());
    }
}
