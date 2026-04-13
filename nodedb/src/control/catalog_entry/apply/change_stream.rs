//! Apply ChangeStream catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::event::cdc::stream_def::ChangeStreamDef;

pub fn put(stored: &ChangeStreamDef, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_change_stream(stored) {
        warn!(
            stream = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_change_stream failed"
        );
    }
}

pub fn delete(tenant_id: u32, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_change_stream(tenant_id, name) {
        warn!(
            stream = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_change_stream failed"
        );
    }
}
