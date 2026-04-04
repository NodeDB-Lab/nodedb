//! `DROP RETENTION POLICY` DDL handler.
//!
//! Syntax:
//! ```sql
//! DROP RETENTION POLICY <name> [ON <collection>]
//! ```

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

pub fn drop_retention_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop retention policies")?;

    // DROP RETENTION POLICY <name>
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP RETENTION POLICY <name>",
        ));
    }
    let name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id.as_u32();

    // Verify policy exists.
    if state
        .retention_policy_registry
        .get(tenant_id, &name)
        .is_none()
    {
        return Err(sqlstate_error(
            "42704",
            &format!("retention policy '{name}' does not exist"),
        ));
    }

    // Delete from catalog.
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .delete_retention_policy(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog delete: {e}")))?;

    // Emit CRDT tombstone delta.
    {
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            collection: super::RETENTION_POLICIES_CRDT_COLLECTION.into(),
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

    // Capture collection name before removing from registry.
    let collection = state
        .retention_policy_registry
        .get(tenant_id, &name)
        .map(|p| p.collection.clone())
        .unwrap_or_default();

    // Remove from in-memory registry.
    state.retention_policy_registry.unregister(tenant_id, &name);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP RETENTION POLICY {name}"),
    );

    tracing::info!(name, %collection, "retention policy dropped");

    Ok(vec![Response::Execution(Tag::new("DROP RETENTION POLICY"))])
}
