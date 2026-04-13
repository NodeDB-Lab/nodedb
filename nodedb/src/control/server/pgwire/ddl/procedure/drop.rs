//! `DROP PROCEDURE` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `DROP PROCEDURE [IF EXISTS] <name>`
pub fn drop_procedure(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop procedures")?;

    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP PROCEDURE [IF EXISTS] <name>",
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
        return Err(sqlstate_error("42601", "procedure name required"));
    }
    let name = parts[idx].to_lowercase().trim_end_matches(';').to_string();
    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    // Pre-check existence so `IF EXISTS` on a missing procedure is
    // a clean no-op that never touches raft.
    let exists_before = catalog
        .get_procedure(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog read: {e}")))?
        .is_some();
    if !exists_before && !if_exists {
        return Err(sqlstate_error(
            "42883",
            &format!("procedure '{name}' does not exist"),
        ));
    }
    if !exists_before {
        return Ok(vec![Response::Execution(Tag::new("DROP PROCEDURE"))]);
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteProcedure {
        tenant_id,
        name: name.clone(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        let _ = catalog
            .delete_procedure(tenant_id, &name)
            .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP PROCEDURE {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP PROCEDURE"))])
}
