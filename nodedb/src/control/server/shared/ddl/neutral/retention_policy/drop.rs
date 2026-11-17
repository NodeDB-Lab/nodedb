// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP RETENTION POLICY` DDL handler.
//!
//! Existence is decided on the leader, before the propose. The removal is a
//! replicated `DeleteRetentionPolicy`, so every node drops the row and its
//! registry entry.
//!
//! Syntax:
//! ```sql
//! DROP RETENTION POLICY <name> [ON <collection>]
//! ```

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::require_tenant_admin;
use super::replicate::propose_delete;

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

pub async fn drop_retention_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop retention policies")?;

    let name = name.to_string();
    let tenant_id = identity.tenant_id.as_u64();

    // Verify policy exists and capture definition for cleanup.
    let policy_def = state
        .retention_policy_registry
        .get(database_id.as_u64(), tenant_id, &name)
        .ok_or_else(|| err("42704", format!("retention policy '{name}' does not exist")))?;

    // Replicated: every node deletes the row and drops its registry entry.
    propose_delete(state, &policy_def).await?;

    // Removal notice for the tenant admins' Lite sessions.
    super::super::system_notice::SystemNotice {
        state,
        database_id,
        tenant_id,
        collection: super::RETENTION_POLICIES_CRDT_COLLECTION,
        document_id: name.clone(),
    }
    .delete();

    // Unregister auto-created continuous aggregates.
    if !policy_def.downsample_tiers().is_empty()
        && let Err(e) = crate::engine::timeseries::retention_policy::autowire::unregister_tiers(
            state,
            &policy_def,
        )
        .await
    {
        crate::diag::retention_autowire_orphaned(
            &e,
            database_id.as_u64(),
            tenant_id,
            &name,
            &policy_def.collection,
        );
        tracing::warn!(
            policy = name,
            error = %e,
            "failed to unregister some auto-wired aggregates (continuing drop)"
        );
    }

    let collection = policy_def.collection.clone();

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP RETENTION POLICY {name}"),
    );

    tracing::info!(name, %collection, "retention policy dropped");

    Ok(vec![DdlResult::Status {
        command: "DROP RETENTION POLICY".to_string(),
        rows_affected: None,
    }])
}
