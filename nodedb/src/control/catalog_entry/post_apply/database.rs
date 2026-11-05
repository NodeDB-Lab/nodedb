// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for database catalog entries.
//!
//! Database descriptors and grants are read directly from redb. The write
//! gates read the mirror fence instead, which these arms keep in step. The
//! `PutDatabase` arm creates the database metrics entry. The `DeleteDatabase`
//! arm releases the dropped scope's quota caps and metrics entry.

use std::sync::Arc;

use nodedb_types::DatabaseId;

use crate::control::security::catalog::database_types::DatabaseDescriptor;
use crate::control::state::SharedState;

/// Post-apply for `PutDatabase` — create the database metrics entry and follow
/// the descriptor's mirror status.
pub fn put(descriptor: DatabaseDescriptor, shared: Arc<SharedState>) {
    shared.database_metrics.ensure_database(descriptor.id);
    shared.mirror.fence.apply_descriptor(&descriptor);
}

/// Synchronous post-apply for `DeleteDatabase` — release the quota caps and
/// metrics entry of the dropped scope, its tenants' caps included, and its
/// mirror fence entry.
pub fn delete(db_id: u64, shared: Arc<SharedState>) {
    let database_id = DatabaseId::new(db_id);
    super::quota::release_database_scope(database_id, &shared);
    shared.mirror.fence.remove(database_id);
}

/// Post-apply for `PutDatabaseGrant`. A session-lifetime identity copies the
/// grant set, so open sessions rebuild it at their next request.
pub fn put_grant(_db_id: u64, user_id: u64, _privilege: String, shared: Arc<SharedState>) {
    shared.credentials.mark_identity_changed(user_id);
}

/// Post-apply for `DeleteDatabaseGrant`. See [`put_grant`].
pub fn delete_grant(_db_id: u64, user_id: u64, _privilege: String, shared: Arc<SharedState>) {
    shared.credentials.mark_identity_changed(user_id);
}
