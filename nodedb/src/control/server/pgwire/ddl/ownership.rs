use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;

/// ALTER COLLECTION <name> OWNER TO <user>
///
/// Transfer ownership of a collection. Requires:
/// - Current owner, OR
/// - Superuser / tenant_admin
pub fn alter_collection_owner(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // ALTER COLLECTION <name> OWNER TO <user>
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER COLLECTION <name> OWNER TO <user>",
        ));
    }

    let collection = parts[2];
    if !parts[3].eq_ignore_ascii_case("OWNER") || !parts[4].eq_ignore_ascii_case("TO") {
        return Err(sqlstate_error(
            "42601",
            "expected OWNER TO after collection name",
        ));
    }
    let new_owner = parts[5];

    // Check authorization: current owner or admin.
    let current_owner = state
        .permissions
        .get_owner("collection", identity.tenant_id, collection);

    let is_current_owner = current_owner
        .as_ref()
        .is_some_and(|o| o == &identity.username);

    if !is_current_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only the current owner, superuser, or tenant_admin can transfer ownership",
        ));
    }

    // Verify new owner exists.
    if state.credentials.get_user(new_owner).is_none() {
        return Err(sqlstate_error(
            "42704",
            &format!("user '{new_owner}' not found"),
        ));
    }

    // Mutate the parent `StoredCollection` and re-propose it: the
    // `OWNERS` redb table is canonical at boot time
    // (`PermissionStore::load_from`), but the `post_apply` for
    // `PutCollection` rewrites it from `stored.owner` on every
    // node — so the only way to keep an owner change durable
    // through subsequent ALTER COLLECTION calls is to also mutate
    // the parent record. A separate `PutOwner` would be silently
    // overwritten the next time anyone re-proposed the collection.
    let catalog_ref = state.credentials.catalog();
    let catalog = catalog_ref
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "catalog unavailable for ALTER COLLECTION OWNER"))?;
    let mut stored = match catalog.get_collection(identity.tenant_id.as_u32(), collection) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return Err(sqlstate_error(
                "42P01",
                &format!("collection '{collection}' does not exist"),
            ));
        }
        Err(e) => return Err(sqlstate_error("XX000", &format!("catalog read: {e}"))),
    };
    stored.owner = new_owner.to_string();
    let entry = CatalogEntry::PutCollection(Box::new(stored.clone()));
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .put_collection(&stored)
            .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        state.permissions.install_replicated_owner(
            &crate::control::security::catalog::StoredOwner {
                object_type: "collection".into(),
                object_name: stored.name.clone(),
                tenant_id: stored.tenant_id,
                owner_username: stored.owner.clone(),
            },
        );
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "transferred ownership of collection '{collection}' from '{}' to '{new_owner}'",
            current_owner.unwrap_or_else(|| "<none>".to_string())
        ),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}
