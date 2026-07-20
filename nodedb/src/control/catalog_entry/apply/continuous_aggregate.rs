// SPDX-License-Identifier: BUSL-1.1

//! Apply ContinuousAggregate catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredContinuousAggregate, SystemCatalog};

pub fn put(stored: &StoredContinuousAggregate, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_continuous_aggregate(stored) {
        warn!(
            cagg = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_continuous_aggregate failed"
        );
    }
    super::owner::put_parent_owner_in_database(
        object_type::CONTINUOUS_AGGREGATE,
        stored.database_id,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(database_id: u64, tenant_id: u64, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_continuous_aggregate(database_id, tenant_id, name) {
        warn!(
            cagg = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_continuous_aggregate failed"
        );
    }
    super::owner::delete_parent_owner_in_database(
        object_type::CONTINUOUS_AGGREGATE,
        database_id,
        tenant_id,
        name,
        catalog,
    );
}
