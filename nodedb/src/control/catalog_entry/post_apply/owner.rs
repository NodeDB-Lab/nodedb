//! Post-apply side effects for ownership `CatalogEntry` variants
//! (orphan path — see `apply::owner`) plus the shared helper used
//! by every sibling post_apply file that replicates ownership as a
//! side effect of a parent `Stored*` entry.

use std::sync::Arc;

use crate::control::security::catalog::StoredOwner;
use crate::control::state::SharedState;

/// Install an owner record derived from a parent `Stored*` entry.
///
/// Every post_apply file that handles an object with an embedded
/// `owner` field (function, trigger, sequence, schedule,
/// change_stream, procedure, materialized_view, collection) calls
/// this instead of constructing `StoredOwner` inline. Objects that
/// have no parent `Stored*` variant (indexes, spatial indexes, raw
/// `ALTER OBJECT OWNER`) use `CatalogEntry::PutOwner` directly.
pub(super) fn install_from_parent(
    object_type: &'static str,
    tenant_id: u32,
    object_name: &str,
    owner_username: &str,
    shared: &SharedState,
) {
    shared.permissions.install_replicated_owner(&StoredOwner {
        object_type: object_type.to_string(),
        object_name: object_name.to_string(),
        tenant_id,
        owner_username: owner_username.to_string(),
    });
}

pub fn put(stored: StoredOwner, shared: Arc<SharedState>) {
    shared.permissions.install_replicated_owner(&stored);
    tracing::debug!(
        object_type = %stored.object_type,
        tenant = stored.tenant_id,
        object = %stored.object_name,
        owner = %stored.owner_username,
        "post_apply: owner replicated"
    );
}

pub fn delete(object_type: String, tenant_id: u32, object_name: String, shared: Arc<SharedState>) {
    let removed =
        shared
            .permissions
            .install_replicated_remove_owner(&object_type, tenant_id, &object_name);
    tracing::debug!(
        %object_type, tenant = tenant_id, %object_name, removed,
        "post_apply: owner removed"
    );
}
