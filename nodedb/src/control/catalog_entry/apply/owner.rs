// SPDX-License-Identifier: BUSL-1.1

//! Apply ownership catalog entries to `SystemCatalog` redb.
//!
//! Two write paths share this file:
//!
//! 1. **Standalone path** — [`put`] / [`delete`] handle
//!    `CatalogEntry::PutOwner` / `DeleteOwner` for objects that have
//!    no parent `Stored*` variant (indexes, spatial indexes).
//!
//! 2. **Parent-replicated path** — [`put_parent_owner`] /
//!    [`delete_parent_owner`] are the single write helpers used by
//!    every sibling applier for objects whose `Stored<T>` record
//!    carries an embedded `owner` field (collection, function,
//!    procedure, trigger, materialized_view, sequence, schedule,
//!    change_stream). Each applier writes the primary row and then
//!    calls one of these helpers so the `OWNERS` redb table — the
//!    persistent backing for the in-memory `PermissionStore.owners`
//!    HashMap — stays in lockstep with the primary row. Omitting
//!    the call leaves redb orphaned on the next restart
//!    (`verify_redb_integrity` aborts boot with `OrphanRow`).

use tracing::warn;

use crate::control::security::catalog::{StoredOwner, SystemCatalog};

pub fn put(stored: &StoredOwner, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_owner(stored) {
        warn!(
            object_type = %stored.object_type,
            tenant = stored.tenant_id,
            object = %stored.object_name,
            error = %e,
            "catalog_entry: put_owner failed"
        );
    }
}

pub fn delete(
    object_type: &str,
    database_id: u64,
    tenant_id: u64,
    object_name: &str,
    catalog: &SystemCatalog,
) {
    if let Err(e) = catalog.delete_owner(object_type, database_id, tenant_id, object_name) {
        warn!(
            object_type = %object_type,
            tenant = tenant_id,
            object = %object_name,
            error = %e,
            "catalog_entry: delete_owner failed"
        );
    }
}

/// Write the `StoredOwner` row for a parent-replicated DDL object.
///
/// Every `apply/<type>.rs::put` for the 8 parent-replicated types
/// must call this after writing the primary row. The primary row's
/// `owner` field is canonical; this call keeps the `OWNERS` redb
/// table in sync so `PermissionStore::load_from` rebuilds the
/// in-memory authorization map correctly on restart.
pub(super) fn put_parent_owner(
    object_type: &'static str,
    tenant_id: u64,
    object_name: &str,
    owner_username: &str,
    catalog: &SystemCatalog,
) {
    put_parent_owner_in_database(
        object_type,
        0,
        tenant_id,
        object_name,
        owner_username,
        catalog,
    );
}

pub(super) fn put_parent_owner_in_database(
    object_type: &'static str,
    database_id: u64,
    tenant_id: u64,
    object_name: &str,
    owner_username: &str,
    catalog: &SystemCatalog,
) {
    let stored = StoredOwner {
        database_id,
        object_type: object_type.to_string(),
        object_name: object_name.to_string(),
        tenant_id,
        owner_username: owner_username.to_string(),
    };
    if let Err(e) = catalog.put_owner(&stored) {
        warn!(
            object_type,
            tenant = tenant_id,
            object = %object_name,
            error = %e,
            "catalog_entry: put_parent_owner failed"
        );
    }
}

/// Remove the `StoredOwner` row for a parent-replicated DDL object.
///
/// Symmetric counterpart of [`put_parent_owner`]. Every drop /
/// deactivate applier for the 8 parent-replicated types must call
/// this so the `OWNERS` redb table does not accumulate orphaned
/// rows after the primary record is gone.
pub(super) fn delete_parent_owner(
    object_type: &'static str,
    tenant_id: u64,
    object_name: &str,
    catalog: &SystemCatalog,
) {
    delete_parent_owner_in_database(object_type, 0, tenant_id, object_name, catalog);
}

pub(super) fn delete_parent_owner_in_database(
    object_type: &'static str,
    database_id: u64,
    tenant_id: u64,
    object_name: &str,
    catalog: &SystemCatalog,
) {
    if let Err(e) = catalog.delete_owner(object_type, database_id, tenant_id, object_name) {
        warn!(
            object_type,
            tenant = tenant_id,
            object = %object_name,
            error = %e,
            "catalog_entry: delete_parent_owner failed"
        );
    }
}
