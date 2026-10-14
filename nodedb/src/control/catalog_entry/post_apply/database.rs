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

/// Post-apply for `PutDatabase` — no in-memory cache to update.
pub fn put(_descriptor: DatabaseDescriptor, _shared: Arc<SharedState>) {}

/// Post-apply for `DeleteDatabase` — release the quota caps of the dropped
/// scope, its tenants' caps included, unregister from maintenance budget,
/// and remove any metrics handles from `DatabaseMetricsRegistry`.
pub fn delete(db_id: u64, shared: Arc<SharedState>) {
    let db = DatabaseId::new(db_id);
    shared.database_metrics.remove(db);
    shared.maintenance_budget.remove_database(db);
    super::quota::release_database_scope(db, &shared);
}

/// Post-apply for `PutDatabaseGrant`.
pub fn put_grant(_db_id: u64, _user_id: u64, _privilege: String, _shared: Arc<SharedState>) {}

/// Post-apply for `DeleteDatabaseGrant`.
pub fn delete_grant(_db_id: u64, _user_id: u64, _privilege: String, _shared: Arc<SharedState>) {}
