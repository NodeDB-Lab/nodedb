//! Apply Function catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::function_types::StoredFunction;

pub fn put(stored: &StoredFunction, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_function(stored) {
        warn!(
            function = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_function failed"
        );
    }
}

pub fn delete(tenant_id: u32, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_function(tenant_id, name) {
        warn!(
            function = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_function failed"
        );
    }
}
