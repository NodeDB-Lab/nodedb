//! Apply Schedule catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::event::scheduler::types::ScheduleDef;

pub fn put(stored: &ScheduleDef, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_schedule(stored) {
        warn!(
            schedule = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_schedule failed"
        );
    }
}

pub fn delete(tenant_id: u32, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_schedule(tenant_id, name) {
        warn!(
            schedule = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_schedule failed"
        );
    }
}
