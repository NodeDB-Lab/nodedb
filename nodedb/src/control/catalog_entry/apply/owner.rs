//! Apply ownership catalog entries to `SystemCatalog` redb.
//!
//! Used by the orphan path (objects without a replicated parent
//! variant). Parent-replicated objects (collection / function /
//! procedure / trigger / materialized_view / sequence / schedule /
//! change_stream) write their owner row from the parent applier
//! and don't go through this file.

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

pub fn delete(object_type: &str, tenant_id: u32, object_name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_owner(object_type, tenant_id, object_name) {
        warn!(
            object_type = %object_type,
            tenant = tenant_id,
            object = %object_name,
            error = %e,
            "catalog_entry: delete_owner failed"
        );
    }
}
