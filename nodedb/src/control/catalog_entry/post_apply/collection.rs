//! Collection post-apply side effects.

use std::sync::Arc;

use tracing::debug;

use crate::control::security::catalog::{StoredCollection, StoredOwner};
use crate::control::state::SharedState;

/// Synchronous half of `PutCollection` post-apply: install the owner
/// record into the in-memory `PermissionStore`. Called inline by the
/// metadata applier BEFORE the applied-index watcher bump so readers
/// of `applied_index` observe the ownership consistently.
pub fn put_owner_sync(stored: &StoredCollection, shared: Arc<SharedState>) {
    // Replicate the owner record on every node so cluster-wide
    // `is_owner` / `check` evaluations succeed. Handlers no longer
    // call `set_owner` directly — ownership is entirely a side
    // effect of the parent `PutCollection` apply.
    shared.permissions.install_replicated_owner(&StoredOwner {
        object_type: "collection".into(),
        object_name: stored.name.clone(),
        tenant_id: stored.tenant_id,
        owner_username: stored.owner.clone(),
    });
}

/// Register-dispatch half: dispatch a `Register` request to this node's
/// Data Plane so subsequent `DocumentOp::Scan` calls find the collection
/// in `doc_configs` and decode strict (Binary Tuple) documents correctly.
///
/// Called via `block_in_place` inside `spawn_post_apply_async_side_effects`
/// for `PutCollection` — it completes synchronously before the applied-index
/// watcher bumps, making it part of the applied-index contract.
pub async fn put_async(stored: StoredCollection, shared: Arc<SharedState>) {
    crate::control::server::pgwire::ddl::collection::create::dispatch_register_from_stored(
        &shared, &stored,
    )
    .await;
    debug!(
        collection = %stored.name,
        "catalog_entry: Register dispatched to local Data Plane"
    );
}

/// Synchronous half of `PurgeCollection` post-apply: remove the
/// in-memory owner entry + any permission-cache entries keyed on
/// the purged collection. The primary `StoredCollection` redb row
/// is already gone at this point (removed by `apply/collection.rs::purge`).
/// The Data Plane `UnregisterCollection` dispatch is the async half
/// and lives in `async_dispatch/collection.rs::purge_async`.
pub fn purge_sync(tenant_id: u32, name: String, shared: Arc<SharedState>) {
    let owner_removed =
        shared
            .permissions
            .install_replicated_remove_owner("collection", tenant_id, &name);
    // Permission grants referencing the purged collection are
    // evicted from the in-memory grant set — stale cached entries
    // would otherwise outlive the catalog row they reference.
    // Grant targets for collections are keyed as
    // "collection:<tenant_id>:<name>" (see grant_target_for_collection
    // in the pgwire GRANT handler).
    let grant_target = format!("collection:{tenant_id}:{name}");
    let grants_removed = shared.permissions.remove_grants_for_target(&grant_target);
    debug!(
        collection = %name,
        tenant = tenant_id,
        owner_removed,
        grants_removed,
        "catalog_entry: PurgeCollection post-apply sync (owner + grants evicted)"
    );
}

pub fn deactivate(tenant_id: u32, name: String, _shared: Arc<SharedState>) {
    // Ownership is intentionally preserved on soft-delete. The
    // primary `StoredCollection` record is kept for audit / undrop
    // (see `CatalogEntry::DeactivateCollection`); removing the
    // in-memory owner entry would split truth from the preserved
    // primary row's `stored.owner` field and force any future
    // UNDROP to be admin-only. `is_owner` returning true for a
    // soft-deleted collection is the correct semantics: the former
    // owner remains the rightful restorer. Hard deletion of the
    // collection (not wired today) would clear both halves via
    // `delete_parent_owner` in the applier.
    debug!(
        collection = %name,
        tenant = tenant_id,
        "catalog_entry: DeactivateCollection post-apply (owner retained for undrop; Data Plane Unregister deferred)"
    );
}
