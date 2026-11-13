// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP ALERT` DDL handler.
//!
//! The removal is replicated: a `DeleteAlertRule` entry drops the row, the
//! registry entry, and the hysteresis state on every node. The `_alert_rules`
//! CRDT-sync tombstone is edge-to-cloud sync, so it stays in the handler.
//!
//! Syntax: `DROP ALERT <name>`

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// CRDT collection name for alert rule sync between Origin and Lite.
const ALERT_RULES_CRDT_COLLECTION: &str =
    crate::event::crdt_sync::types::DefinitionFamily::AlertRules.collection();

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Existence check used by the `DROP ALERT IF EXISTS` short-circuit in the
/// neutral router.
pub fn alert_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state
        .alert_registry
        .get(database_id.as_u64(), tid, name)
        .is_some()
}

pub async fn drop_alert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop alerts")?;

    if parts.len() < 3 {
        return Err(err("42601", "syntax: DROP ALERT <name>".to_string()));
    }
    let name = parse_ident_token(parts[2])?;
    let tenant_id = identity.tenant_id.as_u64();

    if state
        .alert_registry
        .get(database_id.as_u64(), tenant_id, &name)
        .is_none()
    {
        return Err(err("42704", format!("alert '{name}' does not exist")));
    }

    super::replicate::propose_delete(state, database_id.as_u64(), tenant_id, &name).await?;

    // Removal notice for the tenant admins' Lite sessions. Handler-scoped:
    // the apply path runs on every node, so emitting there duplicates it.
    super::super::system_notice::SystemNotice {
        state,
        database_id,
        tenant_id,
        collection: ALERT_RULES_CRDT_COLLECTION,
        document_id: name.clone(),
    }
    .delete();

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP ALERT {name}"),
    );

    tracing::info!(name, "alert rule dropped");

    Ok(status("DROP ALERT"))
}
