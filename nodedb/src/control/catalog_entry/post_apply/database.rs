// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for database catalog entries.
//!
//! Database descriptors and grants are read directly from redb on the hot
//! path (no separate in-memory registry). The `PutDatabase` arm creates the
//! database metrics entry. The `DeleteDatabase` arm releases the dropped
//! scope's quota caps and metrics entry.

use std::sync::Arc;

use nodedb_types::DatabaseId;

use crate::control::security::catalog::database_types::DatabaseDescriptor;
use crate::control::state::SharedState;

/// Post-apply for `PutDatabase` — create the database metrics entry.
pub fn put(descriptor: DatabaseDescriptor, shared: Arc<SharedState>) {
    shared.database_metrics.ensure_database(descriptor.id);
}

/// Synchronous post-apply for `DeleteDatabase` — release the quota caps and
/// metrics entry of the dropped scope, its tenants' caps included.
pub fn delete(db_id: u64, shared: Arc<SharedState>) {
    super::quota::release_database_scope(DatabaseId::new(db_id), &shared);
}

/// Post-apply for `PutDatabaseGrant`.
pub fn put_grant(_db_id: u64, _user_id: u64, _privilege: String, _shared: Arc<SharedState>) {}

/// Post-apply for `DeleteDatabaseGrant`.
pub fn delete_grant(_db_id: u64, _user_id: u64, _privilege: String, _shared: Arc<SharedState>) {}
