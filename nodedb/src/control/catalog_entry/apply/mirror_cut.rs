// SPDX-License-Identifier: BUSL-1.1

//! Apply a mirror's cut state to `SystemCatalog` redb.

use nodedb_types::{DatabaseId, MirrorCutState, MirrorStatus, MirrorStatusStep};

use crate::control::security::catalog::database_types::DatabaseStatus;
use crate::control::security::catalog::{SystemCatalog, catalog_err};

/// Apply a `PutMirrorCutState` entry.
///
/// A database that no longer follows a source writes nothing, so a late
/// entry never moves a promoted mirror's row or brings back a row DROP
/// removed. Metadata entries apply one at a time, so the descriptor read and
/// the write see one state.
pub fn raise(
    database_id: u64,
    state: &MirrorCutState,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let id = DatabaseId::new(database_id);
    let descriptor = catalog
        .get_database(id)
        .map_err(|e| catalog_err(&format!("get_database (database {database_id})"), e))?;
    let follows = descriptor
        .and_then(|descriptor| descriptor.mirror_origin)
        .is_some_and(|origin| !matches!(origin.status, MirrorStatus::Promoted));
    if !follows {
        return Ok(());
    }
    catalog
        .raise_mirror_cut_state(id, state)
        .map(drop)
        .map_err(|e| catalog_err(&format!("put_mirror_cut_state (database {database_id})"), e))
}

/// Apply a `StepMirrorStatus` entry against the row this replica holds.
///
/// Metadata entries apply one at a time, so the reads and the write see one
/// state: a `Seal` fixes the last cut at the `ready_cut` of that index, and no
/// `MirrorCut` can land between them.
pub fn step_status(
    database_id: u64,
    step: &MirrorStatusStep,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let id = DatabaseId::new(database_id);
    let Some(mut descriptor) = catalog
        .get_database(id)
        .map_err(|e| catalog_err(&format!("get_database (database {database_id})"), e))?
    else {
        return Ok(());
    };
    let ready_cut = catalog
        .get_mirror_cut_state(id)
        .map_err(|e| catalog_err(&format!("get_mirror_cut_state (database {database_id})"), e))?
        .map_or(0, |state| state.ready_cut);
    let Some(origin) = descriptor.mirror_origin.as_mut() else {
        return Ok(());
    };
    let Some(next) = step.next(&origin.status, ready_cut) else {
        return Ok(());
    };
    origin.status = next;
    if matches!(origin.status, MirrorStatus::Promoted) {
        descriptor.status = DatabaseStatus::Active;
    }
    catalog
        .put_database(&descriptor)
        .map_err(|e| catalog_err(&format!("put_database (database {database_id})"), e))
}

#[cfg(test)]
mod tests {
    use nodedb_types::{MirrorMode, MirrorOrigin};

    use super::*;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::catalog_entry::apply::apply_to;
    use crate::control::security::catalog::database_types::DatabaseDescriptor;

    const DB: u64 = 1040;

    fn descriptor(mirror_status: Option<MirrorStatus>) -> DatabaseDescriptor {
        let status = match mirror_status {
            Some(MirrorStatus::Promoted) | None => DatabaseStatus::Active,
            Some(_) => DatabaseStatus::Mirroring,
        };
        DatabaseDescriptor {
            id: DatabaseId::new(DB),
            name: "mirror_db".to_string(),
            status,
            created_at_lsn: 0,
            quota_ref: 0,
            parent_clone: None,
            mirror_origin: mirror_status.map(|status| MirrorOrigin {
                source_cluster: "source".to_string(),
                source_database: DatabaseId::new(DB),
                source_database_name: "orders".to_string(),
                mode: MirrorMode::Async,
                status,
            }),
            audit_dml: nodedb_types::AuditDmlMode::None,
            idle_session_timeout_secs: 0,
        }
    }

    fn state(ready_cut: u64) -> MirrorCutState {
        MirrorCutState {
            ready_cut,
            applied_cut: ready_cut,
            applied_cut_hlc_ns: 1,
            cap_ordinal: 1,
            positions: Vec::new(),
            epochs: Vec::new(),
            fence_epoch: 0,
        }
    }

    fn entry(ready_cut: u64) -> CatalogEntry {
        CatalogEntry::PutMirrorCutState {
            database_id: DB,
            state: Box::new(state(ready_cut)),
        }
    }

    fn cut_state(catalog: &SystemCatalog) -> Option<MirrorCutState> {
        catalog
            .get_mirror_cut_state(DatabaseId::new(DB))
            .expect("read cut state")
    }

    #[test]
    fn a_following_mirror_takes_the_state_and_a_replay_moves_nothing_back() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        catalog
            .put_database(&descriptor(Some(MirrorStatus::Following)))
            .expect("put");
        apply_to(&entry(3), &catalog).expect("apply");
        apply_to(&entry(2), &catalog).expect("apply late entry");
        assert_eq!(cut_state(&catalog), Some(state(3)));
    }

    #[test]
    fn a_failed_mirror_still_takes_the_state() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        catalog
            .put_database(&descriptor(Some(MirrorStatus::Failed {
                failure: nodedb_types::MirrorFailure::SourceDropped,
            })))
            .expect("put");
        apply_to(&entry(3), &catalog).expect("apply");
        assert_eq!(cut_state(&catalog), Some(state(3)));
    }

    #[test]
    fn a_database_that_follows_nothing_writes_nothing() {
        for present in [
            None,
            Some(descriptor(None)),
            Some(descriptor(Some(MirrorStatus::Promoted))),
        ] {
            let catalog = SystemCatalog::open_in_memory().expect("catalog");
            if let Some(present) = present {
                catalog.put_database(&present).expect("put");
            }
            apply_to(&entry(3), &catalog).expect("apply");
            assert_eq!(cut_state(&catalog), None);
        }
    }
}
