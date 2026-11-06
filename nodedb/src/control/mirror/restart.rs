// SPDX-License-Identifier: BUSL-1.1

//! Which mirror databases need an applier, and where each one resumes.
//!
//! The applier supervisor reads this set on every pass, so a restart and a new
//! `MIRROR DATABASE` take one path. A reboot fetches no fresh capture: each
//! source group resumes from the replicated cut state.
//!
//! A `Promoted` mirror is a normal writable database and follows no source.
//!
//! A mirror with no ready cut starts from bootstrap.

use nodedb_types::{DatabaseId, MirrorGroupPosition, MirrorStatus};

use crate::control::security::catalog::SystemCatalog;

/// One mirror database's restart decision.
#[derive(Debug)]
pub struct MirrorRestartDecision {
    /// The mirror's id, which is the source database's id.
    pub database: DatabaseId,
    pub database_name: String,
    /// The name `[mirror.sources]` knows the source cluster by.
    pub source_cluster: String,
    pub source_database_name: String,
    pub status: MirrorStatus,
    /// The newest source cut that committed in every mirror group.
    pub ready_cut: u64,
    /// Where each source group resumes.
    pub positions: Vec<MirrorGroupPosition>,
    pub needs_bootstrap: bool,
}

impl MirrorRestartDecision {
    /// Whether an applier has work on this mirror. A failed mirror waits for an
    /// operator, and a promoting one ends on its own path.
    pub fn wants_applier(&self) -> bool {
        match self.status {
            MirrorStatus::Bootstrapping { .. }
            | MirrorStatus::Following
            | MirrorStatus::Degraded
            | MirrorStatus::Disconnected => true,
            MirrorStatus::Promoting { .. }
            | MirrorStatus::Failed { .. }
            | MirrorStatus::Promoted => false,
        }
    }
}

/// Every mirror in `catalog` that is not promoted.
///
/// A cut state that does not read fails the enumeration: a guessed position
/// applies entries twice, or skips them.
pub fn enumerate_resumable_mirrors(
    catalog: &SystemCatalog,
) -> crate::Result<Vec<MirrorRestartDecision>> {
    let mut decisions = Vec::new();
    for db in catalog.list_databases()? {
        let Some(origin) = db.mirror_origin else {
            continue;
        };
        if matches!(origin.status, MirrorStatus::Promoted) {
            continue;
        }
        let cut_state = catalog.get_mirror_cut_state(db.id)?.unwrap_or_default();
        decisions.push(MirrorRestartDecision {
            database: db.id,
            database_name: db.name,
            source_cluster: origin.source_cluster,
            source_database_name: origin.source_database_name,
            status: origin.status,
            ready_cut: cut_state.ready_cut,
            needs_bootstrap: cut_state.ready_cut == 0,
            positions: cut_state.positions,
        });
    }
    Ok(decisions)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use nodedb_types::{MirrorCutState, MirrorFailure};
    use tempfile::TempDir;

    use super::super::test_fixture::mirror_descriptor;
    use super::*;
    use crate::control::security::catalog::SystemCatalog;
    use crate::control::security::catalog::database_types::DatabaseDescriptor;

    fn open_tmp_catalog(tmp: &TempDir) -> SystemCatalog {
        let path: PathBuf = tmp.path().join("system.redb");
        SystemCatalog::open(&path).expect("open catalog")
    }

    fn make_mirror_db(id: u64, name: &str, status: MirrorStatus) -> DatabaseDescriptor {
        mirror_descriptor(id, name, status)
    }

    #[test]
    fn promoted_mirror_is_skipped() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(100);
        let db = make_mirror_db(db_id.as_u64(), "promoted_db", MirrorStatus::Promoted);
        catalog.put_database(&db).unwrap();

        let decisions = enumerate_resumable_mirrors(&catalog).unwrap();
        assert!(
            !decisions.iter().any(|d| d.database_name == "promoted_db"),
            "promoted mirror must be excluded from restart"
        );
    }

    #[test]
    fn following_mirror_resumes_from_its_cut_state() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(101);
        let db = make_mirror_db(db_id.as_u64(), "follower_db", MirrorStatus::Following);
        catalog.put_database(&db).unwrap();

        let position = nodedb_types::MirrorGroupPosition {
            group_id: 2,
            log_index: 77,
        };
        catalog
            .raise_mirror_cut_state(
                db_id,
                &MirrorCutState {
                    ready_cut: 99,
                    positions: vec![position],
                    ..MirrorCutState::default()
                },
            )
            .unwrap();

        let decisions = enumerate_resumable_mirrors(&catalog).unwrap();
        let d = decisions
            .iter()
            .find(|d| d.database_name == "follower_db")
            .unwrap();
        assert_eq!(d.database, db_id);
        assert_eq!(d.ready_cut, 99);
        assert_eq!(d.positions, vec![position]);
        assert!(!d.needs_bootstrap);
        assert!(d.wants_applier());
    }

    #[test]
    fn mirror_with_no_cut_state_needs_bootstrap() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(103);
        let db = make_mirror_db(
            db_id.as_u64(),
            "fresh_db",
            MirrorStatus::Bootstrapping {
                bytes_done: 0,
                bytes_total: 0,
            },
        );
        catalog.put_database(&db).unwrap();

        let decisions = enumerate_resumable_mirrors(&catalog).unwrap();
        let d = decisions
            .iter()
            .find(|d| d.database_name == "fresh_db")
            .unwrap();
        assert_eq!(d.ready_cut, 0);
        assert!(d.positions.is_empty());
        assert!(d.needs_bootstrap);
        assert!(d.wants_applier());
    }

    #[test]
    fn a_failed_mirror_stays_listed_and_gets_no_applier() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db = make_mirror_db(
            104,
            "failed_db",
            MirrorStatus::Failed {
                failure: MirrorFailure::SourceDropped,
            },
        );
        catalog.put_database(&db).unwrap();

        let decisions = enumerate_resumable_mirrors(&catalog).unwrap();
        let d = decisions
            .iter()
            .find(|d| d.database_name == "failed_db")
            .unwrap();
        assert!(!d.wants_applier());
    }
}
