// SPDX-License-Identifier: BUSL-1.1

//! Continuous-aggregate post-apply — install the owner cache entry
//! and let the Data Plane register-dispatch happen on the handler
//! side (the manager's runtime state is per-core; the proposing
//! node calls `MetaOp::RegisterContinuousAggregate` after the
//! propose returns and startup replay does the same on every node).

use std::sync::Arc;

use tracing::debug;

use crate::control::security::catalog::StoredContinuousAggregate;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::state::SharedState;

pub fn put(stored: StoredContinuousAggregate, shared: Arc<SharedState>) {
    debug!(
        cagg = %stored.name,
        tenant = stored.tenant_id,
        "catalog_entry: continuous aggregate upserted"
    );
    super::owner::install_from_parent_in_database(
        object_type::CONTINUOUS_AGGREGATE,
        stored.database_id,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
}

pub fn delete(database_id: u64, tenant_id: u64, name: String, shared: Arc<SharedState>) {
    debug!(
        cagg = %name,
        tenant = tenant_id,
        "catalog_entry: continuous aggregate removed"
    );
    shared
        .permissions
        .install_replicated_remove_owner_in_database(
            object_type::CONTINUOUS_AGGREGATE,
            database_id,
            tenant_id,
            &name,
        );
}
