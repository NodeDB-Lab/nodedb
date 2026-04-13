//! DROP COLLECTION DDL.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

/// DROP COLLECTION <name>
///
/// Marks collection as inactive. Requires owner or admin.
pub fn drop_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP COLLECTION <name>"));
    }

    let name_lower = parts[2].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner("collection", tenant_id, name)
        .as_deref()
        == Some(&identity.username);

    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only owner, superuser, or tenant_admin can drop collections",
        ));
    }

    // Verify the collection exists (read from the legacy redb — the
    // replicated cache observes the same entries via the applier).
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), name) {
            Ok(Some(coll)) if coll.is_active => {}
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{name}' does not exist"),
                ));
            }
        }
    }

    // Propose the drop through the metadata raft group. The applier
    // on every node performs the `get + flip is_active + put` sequence,
    // so the record is preserved for audit / undrop and the legacy
    // `load_collections_for_tenant` filter hides it everywhere.
    let entry = crate::control::metadata_proposer::collection_drop_entry(tenant_id.as_u32(), name);
    let log_index = crate::control::metadata_proposer::propose_metadata_and_wait(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0
        && let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(mut coll)) = catalog.get_collection(tenant_id.as_u32(), name)
    {
        // Single-node / no-cluster fallback: same semantics the
        // applier applies, executed directly here.
        coll.is_active = false;
        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    // Cascade: drop implicit sequences (SERIAL/BIGSERIAL fields create {coll}_{field}_seq).
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(seqs) = catalog.load_sequences_for_tenant(tenant_id.as_u32())
    {
        let prefix = format!("{name}_");
        let suffix = "_seq";
        for seq in &seqs {
            if seq.name.starts_with(&prefix) && seq.name.ends_with(suffix) {
                catalog
                    .delete_sequence(tenant_id.as_u32(), &seq.name)
                    .map_err(|e| {
                        sqlstate_error(
                            "XX000",
                            &format!("failed to drop sequence '{}': {e}", seq.name),
                        )
                    })?;
                // Best-effort: registry removal is non-critical since catalog
                // is the source of truth and the sequence won't be reloaded.
                let _ = state
                    .sequence_registry
                    .remove(tenant_id.as_u32(), &seq.name);
            }
        }
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("dropped collection '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP COLLECTION"))])
}
