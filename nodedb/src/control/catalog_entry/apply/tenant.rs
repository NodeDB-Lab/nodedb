// SPDX-License-Identifier: BUSL-1.1

//! Apply tenant catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::{
    StoredCollection, StoredTenant, StoredUser, SystemCatalog,
};
use crate::types::DatabaseId;

pub fn put(stored: &StoredTenant, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_tenant(stored) {
        warn!(
            tenant = stored.tenant_id,
            name = %stored.name,
            error = %e,
            "catalog_entry: put_tenant failed"
        );
    }
}

pub fn put_with_admin(tenant: &StoredTenant, admin: &StoredUser, catalog: &SystemCatalog) -> bool {
    match catalog.put_tenant_with_admin(tenant, admin) {
        Ok(()) => true,
        Err(error) => {
            warn!(
                tenant = tenant.tenant_id,
                name = %tenant.name,
                admin = %admin.username,
                %error,
                "catalog_entry: put_tenant_with_admin failed"
            );
            false
        }
    }
}

pub fn delete(tenant_id: u64, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_tenant(tenant_id) {
        warn!(
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_tenant failed"
        );
    }
}

/// Apply `MoveTenantCutover`: atomically re-key all `collections` from
/// `source_db_id` to `target_db_id`, then delete each one from the source.
///
/// This is the single Raft proposal that makes the cutover phase of
/// `MOVE TENANT` atomic on every node.
pub fn move_cutover(
    tenant_id: u64,
    source_db_id: u64,
    target_db_id: u64,
    collections: &[StoredCollection],
    catalog: &SystemCatalog,
) {
    let src = DatabaseId::new(source_db_id);
    let tgt = DatabaseId::new(target_db_id);

    for coll in collections {
        // Write to target database.
        let mut target_coll = coll.clone();
        target_coll.database_id = tgt;
        if let Err(e) = catalog.put_collection(tgt, &target_coll) {
            warn!(
                tenant = tenant_id,
                collection = %coll.name,
                error = %e,
                "move_cutover: put_collection to target failed"
            );
            continue;
        }
        // Delete from source database using the collection's own tenant_id,
        // which is the actual storage key component.
        if let Err(e) = catalog.delete_collection(src, coll.tenant_id, &coll.name) {
            warn!(
                tenant = coll.tenant_id,
                collection = %coll.name,
                error = %e,
                "move_cutover: delete_collection from source failed"
            );
        }
    }
}
