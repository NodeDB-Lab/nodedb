// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP SCHEDULE` DDL handler.
//!
//! The `propose_catalog_entry`, the `_schedules` CRDT-sync tombstone delta,
//! and the `audit_record` call run here. The result is the protocol-neutral
//! [`DdlResult`] / [`DdlError`].

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Existence check used by the `DROP SCHEDULE IF EXISTS` short-circuit in the
/// neutral router.
pub fn schedule_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    name: &str,
) -> bool {
    state
        .schedule_registry
        .get(database_id, identity.tenant_id.as_u64(), name)
        .is_some()
}

/// Handle `DROP SCHEDULE [IF EXISTS] <name>`
pub async fn drop_schedule(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop schedules")?;

    // parts: ["DROP", "SCHEDULE", ...]
    let (if_exists, name) = if parts.len() >= 5
        && parts[2].eq_ignore_ascii_case("IF")
        && parts[3].eq_ignore_ascii_case("EXISTS")
    {
        (true, parse_ident_token(parts[4])?)
    } else if parts.len() >= 3 {
        (false, parse_ident_token(parts[2])?)
    } else {
        return Err(DdlError::new(
            "42601",
            "expected DROP SCHEDULE [IF EXISTS] <name>",
        ));
    };

    let tenant_id = identity.tenant_id.as_u64();

    // Pre-check existence: `IF EXISTS` + missing is a no-op that
    // doesn't touch raft. Check via the in-memory registry since
    // `schedules.rs` has no `get_schedule` method today.
    let existed_before = state
        .schedule_registry
        .get(database_id, tenant_id, &name)
        .is_some();
    if !existed_before && !if_exists {
        return Err(DdlError::new(
            "42704",
            format!("schedule '{name}' does not exist"),
        ));
    }
    if !existed_before {
        return Ok(status("DROP SCHEDULE"));
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteSchedule {
        database_id,
        tenant_id,
        name: name.clone(),
    };
    crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;

    // Removal notice for the tenant admins' Lite sessions.
    super::super::system_notice::SystemNotice {
        state,
        database_id,
        tenant_id,
        collection: crate::event::crdt_sync::types::DefinitionFamily::Schedules.collection(),
        document_id: format!("{}:{name}", database_id.as_u64()),
    }
    .delete();

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP SCHEDULE {name}"),
    );

    Ok(status("DROP SCHEDULE"))
}
