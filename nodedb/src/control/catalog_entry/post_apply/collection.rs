// SPDX-License-Identifier: BUSL-1.1

//! Collection post-apply side effects.

use std::sync::Arc;

use tracing::debug;

use crate::control::security::auth_fence::TreeDefChange;
use crate::control::security::catalog::{StoredCollection, StoredOwner};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

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
        database_id: stored.database_id.as_u64(),
        object_type: "collection".into(),
        object_name: stored.name.clone(),
        tenant_id: stored.tenant_id,
        owner_username: stored.owner.clone(),
    });
    // The collection's declared columns type its RLS policies' literals, so
    // every node recompiles them against the schema this entry carries.
    if let Err(e) = shared.rls.recompile_for_collection(
        shared.credentials.catalog(),
        stored.database_id,
        stored.tenant_id,
        &stored.name,
    ) {
        tracing::error!(
            collection = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "post_apply: RLS policies could not be re-read for recompilation"
        );
    }
}

/// Queue the tree-definition change a committed collection descriptor makes.
/// Every node runs this, so each node's permission cache learns the tree
/// defined through any node. Planning and lease coverage move the queue into
/// the cache.
pub fn queue_tree_def_sync(stored: &StoredCollection, shared: &SharedState) {
    match TreeDefChange::from_collection(stored) {
        Ok(Some(change)) => {
            change.note_committed(shared.authorization_fence.sources());
            shared.authorization_fence.tree_defs().push(change);
        }
        Ok(None) => {}
        Err(e) => {
            // The DDL commits the serialization of a parsed definition, so
            // this JSON always parses. The prior definition stays in place:
            // removing it would drop the filter and expose rows.
            tracing::error!(
                collection = %stored.name,
                tenant = stored.tenant_id,
                error = %e,
                "post_apply: PERMISSION_TREE of a committed collection could not be read"
            );
        }
    }
}

/// Queue the removal of a collection's tree definition, for a collection
/// that was dropped or purged.
pub fn queue_tree_def_removal_sync(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    shared: &SharedState,
) {
    if database_id != DatabaseId::DEFAULT.as_u64() {
        return;
    }
    let change = TreeDefChange::Unregister {
        tenant_id,
        collection: name.to_owned(),
    };
    change.note_committed(shared.authorization_fence.sources());
    shared.authorization_fence.tree_defs().push(change);
}

/// Register-dispatch half: dispatch a `Register` request to this node's
/// Data Plane so subsequent `DocumentOp::Scan` calls find the collection
/// in `doc_configs` and decode strict (Binary Tuple) documents correctly.
///
/// Called via `block_in_place` inside `spawn_post_apply_async_side_effects`
/// for `PutCollection` — it completes synchronously before the applied-index
/// watcher bumps, making it part of the applied-index contract.
///
/// A collection this entry names as a materialized-sum SOURCE is re-registered
/// too. The binding travels on the target, but the config that decides whether a
/// write folds it is derived for the source, so propagating the target alone
/// leaves every source on this node folding nothing.
pub async fn put_async(stored: StoredCollection, shared: Arc<SharedState>) {
    use crate::control::server::shared::ddl::neutral::collection::{
        dispatch_register_for_sum_sources, dispatch_register_from_stored,
    };

    let registered = match dispatch_register_from_stored(&shared, &stored).await {
        Ok(()) => dispatch_register_for_sum_sources(&shared, &stored).await,
        Err(e) => Err(e),
    };
    match registered {
        Ok(()) => {
            debug!(
                collection = %stored.name,
                "catalog_entry: Register dispatched to all Data Plane cores"
            );
        }
        Err(e) => {
            tracing::error!(
                collection = %stored.name,
                error = %e,
                "catalog_entry: Register barrier failed — one or more Data Plane cores \
                 did not acknowledge the schema update; this node may serve stale schema"
            );
        }
    }
}

/// Synchronous half of `PurgeCollection` post-apply: remove the
/// in-memory owner entry + any permission-cache entries keyed on
/// the purged collection. The primary `StoredCollection` redb row
/// is already gone at this point (removed by `apply/collection.rs::purge`).
/// The result-checked Data Plane `UnregisterCollection` barrier lives in
/// `async_dispatch/collection.rs::reclaim_collection_storage`.
pub fn purge_sync(database_id: u64, tenant_id: u64, name: String, shared: Arc<SharedState>) {
    let owner_removed = shared.permissions.install_replicated_remove_owner(
        "collection",
        database_id,
        tenant_id,
        &name,
    );
    // Permission grants referencing the purged collection are
    // evicted from the in-memory grant set — stale cached entries
    // would otherwise outlive the catalog row they reference.
    // Grant targets for collections are keyed as
    // "collection:<tenant_id>:<name>" (see grant_target_for_collection
    // in the pgwire GRANT handler).
    let grant_target = format!("collection:{tenant_id}:{name}");
    let grants_removed = shared.permissions.remove_grants_for_target(&grant_target);
    // The indexes go with the collection, so their in-memory ownership entries
    // go too. Read before `finalize_purge` removes the registry rows (it runs
    // later, in the async reclaim half).
    let index_owners_removed = evict_index_owners(database_id, tenant_id, &name, &shared);
    debug!(
        collection = %name,
        tenant = tenant_id,
        owner_removed,
        grants_removed,
        index_owners_removed,
        "catalog_entry: PurgeCollection post-apply sync (owner + grants + index owners evicted)"
    );
}

/// Drop the in-memory ownership entry of every index attached to `name`,
/// returning how many were evicted.
fn evict_index_owners(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    shared: &Arc<SharedState>,
) -> usize {
    let records = match shared
        .credentials
        .catalog()
        .list_index_records_for_collection(database_id, tenant_id, name)
    {
        Ok(records) => records,
        Err(e) => {
            debug!(
                collection = %name,
                tenant = tenant_id,
                error = %e,
                "catalog_entry: index owner eviction skipped (registry read failed)"
            );
            return 0;
        }
    };
    records
        .iter()
        .filter(|record| {
            shared.permissions.install_replicated_remove_owner(
                record.kind.owner_object_type(),
                database_id,
                tenant_id,
                &record.name,
            )
        })
        .count()
}

pub fn deactivate(tenant_id: u64, name: String, _shared: Arc<SharedState>) {
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
