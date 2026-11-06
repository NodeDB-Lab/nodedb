// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for database catalog entries.
//!
//! Database descriptors and grants are read directly from redb. The write
//! gates read the mirror fence instead, which these arms keep in step. The
//! `PutDatabase` arm creates the database metrics entry. The `DeleteDatabase`
//! arm releases the dropped scope's quota caps and metrics entry.

use std::sync::Arc;

use nodedb_types::{DatabaseId, MirrorStatusStep};

use crate::control::catalog_entry::CatalogEntry;

use crate::control::security::catalog::database_types::DatabaseDescriptor;
use crate::control::state::SharedState;

/// Post-apply for `PutDatabase` and `ClaimMirrorDatabase` — create the database
/// metrics entry and follow the row's mirror status. The entry can carry an
/// older status than the row keeps, so the row decides.
pub fn put(descriptor: DatabaseDescriptor, shared: Arc<SharedState>) {
    shared.database_metrics.ensure_database(descriptor.id);
    match shared.credentials.catalog().get_database(descriptor.id) {
        Ok(Some(row)) => {
            shared.mirror.fence.apply_descriptor(&row);
            shared.mirror.cuts.apply_descriptor(&row);
        }
        Ok(None) => {}
        Err(error) => crate::diag::mirror_status_unread(&error, descriptor.id.as_u64()),
    }
}

/// Synchronous post-apply for `DeleteDatabase` — release the quota caps and
/// metrics entry of the dropped scope, its tenants' caps included, and its
/// mirror state.
pub fn delete(db_id: u64, shared: Arc<SharedState>) {
    let database_id = DatabaseId::new(db_id);
    super::quota::release_database_scope(database_id, &shared);
    shared.mirror.fence.remove(database_id);
    shared.mirror.cuts.remove(database_id);
    shared.calvin.applied.remove_mirror_database(db_id);
    shared.mirror.pins.source_dropped(db_id);
}

/// Synchronous post-apply for a mirror row: follow it before the applied index
/// moves. The apply loop's verdict on a held entry reads the cut state, and
/// each log's compaction reads the pins. Any other entry changes nothing here.
pub fn follow_mirror_row(entry: &CatalogEntry, shared: &SharedState) {
    if let CatalogEntry::PutMirrorCutState { database_id, state } = entry {
        shared
            .mirror
            .cuts
            .apply_state(DatabaseId::new(*database_id), state);
    } else if let CatalogEntry::PutMirrorPin(pin) = entry {
        shared.mirror.pins.apply(pin);
    } else if let CatalogEntry::DeleteMirrorPin { link_id } = entry {
        shared.mirror.pins.remove(*link_id);
    }
}

/// Synchronous post-apply for a status step: follow the row the step left,
/// before the applied index moves.
///
/// A row that does not read leaves the fence and the verdicts at the last row
/// they followed. The report names it, and a restart rebuilds both.
///
/// A rebuild also moves the verdicts back to the row it reset, and forgets the
/// mirror's seals and Calvin floors.
pub fn follow_mirror_status(database_id: u64, step: &MirrorStatusStep, shared: &SharedState) {
    let id = DatabaseId::new(database_id);
    let catalog = shared.credentials.catalog();
    match catalog.get_database(id) {
        Ok(Some(descriptor)) => {
            shared.mirror.fence.apply_descriptor(&descriptor);
            shared.mirror.cuts.apply_descriptor(&descriptor);
            if matches!(step, MirrorStatusStep::Rebootstrap) {
                match catalog.get_mirror_cut_state(id) {
                    Ok(state) => shared.mirror.cuts.restart(id, &state.unwrap_or_default()),
                    Err(error) => crate::diag::mirror_status_unread(&error, database_id),
                }
                shared.calvin.applied.remove_mirror_database(database_id);
            }
        }
        Ok(None) => {}
        Err(error) => crate::diag::mirror_status_unread(&error, database_id),
    }
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
