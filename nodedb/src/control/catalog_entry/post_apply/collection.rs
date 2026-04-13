//! Collection post-apply side effects.

use std::sync::Arc;

use tracing::debug;

use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;

pub async fn put(stored: StoredCollection, shared: Arc<SharedState>) {
    // Tell this node's Data Plane about the new collection so the
    // first cross-node INSERT doesn't need to rediscover the
    // storage mode.
    crate::control::server::pgwire::ddl::collection::create::dispatch_register_from_stored(
        &shared, &stored,
    )
    .await;
    debug!(
        collection = %stored.name,
        "catalog_entry: Register dispatched to local Data Plane"
    );
}

pub fn deactivate(tenant_id: u32, name: String, _shared: Arc<SharedState>) {
    // Data Plane Unregister is out of scope for now — the existing
    // enforcement runtime tolerates an orphan register for an
    // inactive collection until the next collection-level reload.
    debug!(
        collection = %name,
        tenant = tenant_id,
        "catalog_entry: DeactivateCollection post-apply (no Data Plane hook yet)"
    );
}
