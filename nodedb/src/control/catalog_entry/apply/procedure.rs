//! Apply Procedure catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::procedure_types::StoredProcedure;

pub fn put(stored: &StoredProcedure, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_procedure(stored) {
        warn!(
            procedure = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_procedure failed"
        );
    }
}

pub fn delete(tenant_id: u32, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_procedure(tenant_id, name) {
        warn!(
            procedure = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_procedure failed"
        );
    }
}
