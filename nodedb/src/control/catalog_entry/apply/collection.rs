// SPDX-License-Identifier: BUSL-1.1

//! Apply Collection catalog entries to `SystemCatalog` redb.

use nodedb_types::DatabaseId;
use tracing::{debug, warn};

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};

pub fn put(stored: &StoredCollection, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_collection(stored.database_id, stored) {
        warn!(
            collection = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_collection failed"
        );
    }
    super::owner::put_parent_owner_in_database(
        object_type::COLLECTION,
        stored.database_id.as_u64(),
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

/// Create-only variant of [`put`]: writes the collection (and its
/// owner row) exactly as `put` does, but ONLY when no collection with
/// the same `(database_id, tenant_id, name)` already exists. If one is
/// present, this is a no-op — the existing schema is never clobbered.
///
/// The existence check is the ONLY behavioral difference from `put`;
/// the create path mirrors `put`'s body precisely so replay/snapshot
/// re-application stays idempotent.
pub fn put_if_absent(stored: &StoredCollection, catalog: &SystemCatalog) {
    match catalog.get_collection(stored.database_id, stored.tenant_id, &stored.name) {
        Ok(Some(_)) => {
            debug!(
                collection = %stored.name,
                tenant = stored.tenant_id,
                "catalog_entry: put_collection_if_absent skipped existing collection"
            );
        }
        Ok(None) => {
            if let Err(e) = catalog.put_collection(stored.database_id, stored) {
                warn!(
                    collection = %stored.name,
                    tenant = stored.tenant_id,
                    error = %e,
                    "catalog_entry: put_collection_if_absent put_collection failed"
                );
            }
            super::owner::put_parent_owner_in_database(
                object_type::COLLECTION,
                stored.database_id.as_u64(),
                stored.tenant_id,
                &stored.name,
                &stored.owner,
                catalog,
            );
        }
        Err(e) => warn!(
            collection = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_collection_if_absent get failed"
        ),
    }
}

/// Hard-delete the catalog metadata for a collection: primary
/// `StoredCollection` row, owner row, and surrogate ↔ PK map.
///
/// Returns `Err` if any storage step that actually removes data
/// fails. Callers with different failure semantics decide what to do:
/// the raft applier (`apply_to_inner`) runs symmetrically on every
/// node and log-and-continues, while the interactive re-CREATE
/// hard-purge propagates so a `CREATE COLLECTION` never builds over
/// data that was not actually purged. Reporting the error rather than
/// swallowing it is what lets each caller make that choice.
pub fn purge(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let database_id = DatabaseId::new(database_id);
    // Hard delete of the primary StoredCollection row. Symmetric
    // with `apply/function.rs::delete` and the other hard-delete
    // peers: remove the primary, then remove the owner row.
    //
    // The two-step DROP → retention-expiry → PURGE flow means the
    // record may already be absent when this runs (operator-driven
    // PURGE on a record the GC sweeper already reclaimed). The
    // `delete_collection` helper is idempotent and returns `false`
    // in that case, which is fine — we still call
    // `delete_parent_owner` because the owner row may linger
    // independently.
    let removed = catalog.delete_collection(database_id, tenant_id, name)?;
    debug!(
        collection = %name,
        tenant = tenant_id,
        removed,
        "catalog_entry: purge_collection primary row removed"
    );
    super::owner::delete_parent_owner_in_database(
        object_type::COLLECTION,
        database_id.as_u64(),
        tenant_id,
        name,
        catalog,
    );
    // Wipe the surrogate ↔ PK map for this collection. Surrogates are
    // collection-scoped; once the primary row is gone they can never
    // be observed again, so leaving the catalog rows behind would
    // just be allocator-bloat. Mirrors the array-drop cleanup in
    // `array_convert::convert_drop_array`.
    catalog.delete_all_surrogates_for_collection(
        database_id,
        nodedb_types::TenantId::new(tenant_id),
        name,
    )?;
    Ok(())
}

pub fn deactivate(database_id: u64, tenant_id: u64, name: &str, catalog: &SystemCatalog) {
    let database_id = DatabaseId::new(database_id);
    match catalog.get_collection(database_id, tenant_id, name) {
        Ok(Some(mut stored)) => {
            stored.is_active = false;
            if let Err(e) = catalog.put_collection(database_id, &stored) {
                warn!(
                    collection = %name,
                    tenant = tenant_id,
                    error = %e,
                    "catalog_entry: deactivate_collection put failed"
                );
            }
        }
        Ok(None) => {
            debug!(
                collection = %name,
                tenant = tenant_id,
                "catalog_entry: deactivate on missing collection (fresh follower)"
            );
        }
        Err(e) => warn!(
            collection = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: deactivate_collection get failed"
        ),
    }
    // Intentionally preserve the `StoredOwner` row on soft-delete.
    // The primary `StoredCollection` record is kept for audit and
    // undrop (see `CatalogEntry::DeactivateCollection` doc and
    // `ddl/collection/drop.rs`). Stripping the owner row would
    // split truth from the preserved primary row whose
    // `stored.owner` is still populated, and would break any
    // future `UNDROP COLLECTION` by requiring admin to restore
    // ownership that was still knowable from the primary. Hard
    // deletion of the collection (not wired today) would remove
    // both halves via `delete_parent_owner`.
}
