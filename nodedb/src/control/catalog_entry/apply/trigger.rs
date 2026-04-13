//! Apply Trigger catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::trigger_types::StoredTrigger;

pub fn put(stored: &StoredTrigger, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_trigger(stored) {
        warn!(
            trigger = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_trigger failed"
        );
    }
}

pub fn delete(tenant_id: u32, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_trigger(tenant_id, name) {
        warn!(
            trigger = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_trigger failed"
        );
    }
}
