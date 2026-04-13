//! Apply Role catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::{StoredRole, SystemCatalog};

pub fn put(stored: &StoredRole, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_role(stored) {
        warn!(
            role = %stored.name,
            error = %e,
            "catalog_entry: put_role failed"
        );
    }
}

pub fn delete(name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_role(name) {
        warn!(
            role = %name,
            error = %e,
            "catalog_entry: delete_role failed"
        );
    }
}
