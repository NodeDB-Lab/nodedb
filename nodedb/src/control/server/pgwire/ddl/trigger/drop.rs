//! `DROP TRIGGER` and `ALTER TRIGGER ... ENABLE/DISABLE` DDL handlers.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `DROP TRIGGER [IF EXISTS] <name>`
pub fn drop_trigger(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop triggers")?;

    let (name, if_exists) = parse_drop_trigger(parts)?;
    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    // Check existence before proposing (so `IF EXISTS` + missing
    // trigger returns a clean success without touching raft).
    let exists_before = catalog
        .get_trigger(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog read: {e}")))?
        .is_some();
    if !exists_before && !if_exists {
        return Err(sqlstate_error(
            "42704",
            &format!("trigger '{name}' does not exist"),
        ));
    }
    if !exists_before {
        return Ok(vec![Response::Execution(Tag::new("DROP TRIGGER"))]);
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteTrigger {
        tenant_id,
        name: name.clone(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .delete_trigger(tenant_id, &name)
            .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        state.trigger_registry.unregister(tenant_id, &name);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP TRIGGER {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP TRIGGER"))])
}

/// Handle `ALTER TRIGGER <name> ENABLE|DISABLE|OWNER TO <new_owner>`
pub fn alter_trigger(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "alter triggers")?;

    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER TRIGGER <name> ENABLE|DISABLE|OWNER TO <user>",
        ));
    }

    let name = parts[2].to_lowercase();
    let action = parts[3].to_uppercase();

    // ALTER TRIGGER <name> OWNER TO <new_owner>
    if action == "OWNER" {
        return alter_trigger_owner(state, identity, parts, &name);
    }

    let enabled = match action.as_str() {
        "ENABLE" => true,
        "DISABLE" => false,
        _ => {
            return Err(sqlstate_error(
                "42601",
                &format!("expected ENABLE, DISABLE, or OWNER TO, got '{action}'"),
            ));
        }
    };

    let tenant_id = identity.tenant_id.as_u32();
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let mut trigger = catalog
        .get_trigger(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42704", &format!("trigger '{name}' does not exist")))?;

    trigger.enabled = enabled;
    catalog
        .put_trigger(&trigger)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    // Update in-memory registry.
    state
        .trigger_registry
        .set_enabled(tenant_id, &name, enabled);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER TRIGGER {name} {action}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER TRIGGER"))])
}

/// Handle `ALTER TRIGGER <name> OWNER TO <new_owner>`
fn alter_trigger_owner(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    name: &str,
) -> PgWireResult<Vec<Response>> {
    // ALTER TRIGGER <name> OWNER TO <new_owner>
    if parts.len() < 6 || !parts[4].eq_ignore_ascii_case("TO") {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER TRIGGER <name> OWNER TO <new_owner>",
        ));
    }
    let new_owner = parts[5].trim_end_matches(';').to_string();

    let tenant_id = identity.tenant_id.as_u32();
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let mut trigger = catalog
        .get_trigger(tenant_id, name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42704", &format!("trigger '{name}' does not exist")))?;

    let old_owner = trigger.owner.clone();
    trigger.owner = new_owner.clone();
    catalog
        .put_trigger(&trigger)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    // Re-register with updated owner in the in-memory registry.
    state.trigger_registry.register(trigger);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER TRIGGER {name} OWNER TO {new_owner} (was: {old_owner})"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER TRIGGER"))])
}

fn parse_drop_trigger(parts: &[&str]) -> PgWireResult<(String, bool)> {
    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP TRIGGER [IF EXISTS] <name>",
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
        return Err(sqlstate_error("42601", "trigger name required"));
    }
    let name = parts[idx].to_lowercase().trim_end_matches(';').to_string();
    Ok((name, if_exists))
}
