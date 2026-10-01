// SPDX-License-Identifier: BUSL-1.1

//! Synchronous post-apply side effects for database catalog entries.
//!
//! Database descriptors and grants are read directly from redb on the hot
//! path (no separate in-memory registry), so most arms are no-ops. The
//! `DeleteDatabase` arm releases the dropped scope's quota caps on every node.

use std::sync::Arc;

use nodedb_types::DatabaseId;

use crate::control::security::catalog::database_types::DatabaseDescriptor;
use crate::control::state::SharedState;

/// Post-apply for `PutDatabase` — update in-memory maintenance budget name resolution.
pub fn put(descriptor: DatabaseDescriptor, shared: Arc<SharedState>) {
    shared
        .maintenance_budget
        .set_database_name(descriptor.id, &descriptor.name);
}

/// Post-apply for `DeleteDatabase` — release the quota caps of the dropped
/// scope, its tenants' caps included, unregister from maintenance budget,
/// and remove any metrics handles from `DatabaseMetricsRegistry`.
pub fn delete(db_id: u64, shared: Arc<SharedState>) {
    let db = DatabaseId::new(db_id);
    if let Ok(Some(name)) = shared.credentials.catalog().get_database_name_by_id(db) {
        shared.database_metrics.remove(&name);
    }
    shared.database_metrics.remove(&format!("db-{}", db_id));
    shared.maintenance_budget.remove_database(db);
    super::quota::release_database_scope(db, &shared);
}

/// Post-apply for `PutDatabaseGrant`.
pub fn put_grant(_db_id: u64, _user_id: u64, _privilege: String, _shared: Arc<SharedState>) {}

/// Post-apply for `DeleteDatabaseGrant`.
pub fn delete_grant(_db_id: u64, _user_id: u64, _privilege: String, _shared: Arc<SharedState>) {}
