//! `DROP SCHEDULE` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `DROP SCHEDULE [IF EXISTS] <name>`
pub fn drop_schedule(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop schedules")?;

    // parts: ["DROP", "SCHEDULE", ...]
    let (if_exists, name) = if parts.len() >= 5
        && parts[2].eq_ignore_ascii_case("IF")
        && parts[3].eq_ignore_ascii_case("EXISTS")
    {
        (true, parts[4].to_lowercase())
    } else if parts.len() >= 3 {
        (false, parts[2].to_lowercase())
    } else {
        return Err(sqlstate_error(
            "42601",
            "expected DROP SCHEDULE [IF EXISTS] <name>",
        ));
    };

    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let existed = catalog
        .delete_schedule(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog delete: {e}")))?;

    if !existed && !if_exists {
        return Err(sqlstate_error(
            "42704",
            &format!("schedule '{name}' does not exist"),
        ));
    }

    state.schedule_registry.unregister(tenant_id, &name);

    // Emit tombstone delta for Lite visibility (removes schedule from Lite catalog).
    {
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            collection: "_schedules".into(),
            document_id: name.clone(),
            payload: Vec::new(),
            op: crate::event::crdt_sync::types::DeltaOp::Delete,
            lsn: 0,
            tenant_id,
            peer_id: state.node_id,
            sequence: 0,
        };
        state.crdt_sync_delivery.enqueue(tenant_id, delta);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP SCHEDULE {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP SCHEDULE"))])
}
