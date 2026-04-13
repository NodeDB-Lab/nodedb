//! Apply permission grant catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::{StoredPermission, SystemCatalog};

pub fn put(stored: &StoredPermission, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_permission(stored) {
        warn!(
            target = %stored.target,
            grantee = %stored.grantee,
            permission = %stored.permission,
            error = %e,
            "catalog_entry: put_permission failed"
        );
    }
}

pub fn delete(target: &str, grantee: &str, permission: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_permission(target, grantee, permission) {
        warn!(
            target = %target,
            grantee = %grantee,
            permission = %permission,
            error = %e,
            "catalog_entry: delete_permission failed"
        );
    }
}
