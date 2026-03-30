//! `DROP CHANGE STREAM` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `DROP CHANGE STREAM [IF EXISTS] <name>`
pub fn drop_change_stream(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop change streams")?;

    // parts: ["DROP", "CHANGE", "STREAM", ...]
    let (if_exists, name) = if parts.len() >= 6
        && parts[3].eq_ignore_ascii_case("IF")
        && parts[4].eq_ignore_ascii_case("EXISTS")
    {
        (true, parts[5].to_lowercase())
    } else if parts.len() >= 4 {
        (false, parts[3].to_lowercase())
    } else {
        return Err(sqlstate_error(
            "42601",
            "expected DROP CHANGE STREAM [IF EXISTS] <name>",
        ));
    };

    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let existed = catalog
        .delete_change_stream(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog delete: {e}")))?;

    if !existed && !if_exists {
        return Err(sqlstate_error(
            "42704",
            &format!("change stream '{name}' does not exist"),
        ));
    }

    state.stream_registry.unregister(tenant_id, &name);

    // Immediately remove the in-memory event buffer so no new events accumulate
    // and memory is freed.
    state.cdc_router.remove_buffer(tenant_id, &name);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP CHANGE STREAM {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP CHANGE STREAM"))])
}
