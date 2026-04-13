//! MaterializedView post-apply — no in-memory registry to sync.
//! The refresh loop reads the view definition straight from
//! `SystemCatalog` on its next tick.

use std::sync::Arc;

use tracing::debug;

use crate::control::security::catalog::StoredMaterializedView;
use crate::control::state::SharedState;

pub fn put(stored: StoredMaterializedView, shared: Arc<SharedState>) {
    debug!(
        view = %stored.name,
        tenant = stored.tenant_id,
        "catalog_entry: materialized view upserted (refresh loop will pick it up)"
    );
    super::owner::install_from_parent(
        "materialized_view",
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
}

pub fn delete(tenant_id: u32, name: String, shared: Arc<SharedState>) {
    debug!(
        view = %name,
        tenant = tenant_id,
        "catalog_entry: materialized view removed"
    );
    shared
        .permissions
        .install_replicated_remove_owner("materialized_view", tenant_id, &name);
}
