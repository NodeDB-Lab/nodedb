// SPDX-License-Identifier: BUSL-1.1

//! Restart-resume for mirror databases.
//!
//! On server start, each database with `mirror_origin.is_some()` and
//! `status != Promoted` must have its cross-cluster observer link
//! re-established from its replicated cut state. A reboot therefore fetches
//! no fresh snapshot: each source group resumes after `ready_cut`.
//!
//! A `Promoted` mirror is a normal writable database; it must NOT
//! attempt to reconnect.
//!
//! A mirror with no ready cut restarts from bootstrap.

use nodedb_types::{MirrorGroupPosition, MirrorStatus};
use tracing::info;

use crate::control::security::catalog::SystemCatalog;

/// Summary of one database's restart decision.
#[derive(Debug)]
pub struct MirrorRestartDecision {
    pub database_name: String,
    /// The newest source cut that committed in every mirror group.
    pub ready_cut: u64,
    /// Where each source group resumes.
    pub positions: Vec<MirrorGroupPosition>,
    pub needs_bootstrap: bool,
}

/// Enumerate databases in `catalog` that need their observer link restarted.
///
/// Returns a list of databases (by name + resume positions) that are mirrors
/// but not yet promoted. Databases with `MirrorStatus::Promoted` are excluded
/// — they are normal writable databases and must not attempt to reconnect.
/// A cut state that does not read fails the enumeration: a guessed position
/// applies entries twice, or skips them.
pub fn enumerate_resumable_mirrors(
    catalog: &SystemCatalog,
) -> crate::Result<Vec<MirrorRestartDecision>> {
    let databases = catalog.list_databases()?;
    let mut decisions = Vec::new();

    for db in databases {
        let origin = match db.mirror_origin {
            Some(ref o) => o,
            None => continue,
        };

        // Promoted mirrors are normal writable databases — skip.
        if matches!(origin.status, MirrorStatus::Promoted) {
            info!(
                database = %db.name,
                "mirror restart: database is Promoted — skipping observer reconnect"
            );
            continue;
        }

        let cut_state = catalog.get_mirror_cut_state(db.id)?.unwrap_or_default();
        let needs_bootstrap = cut_state.ready_cut == 0;
        let ready_cut = cut_state.ready_cut;

        decisions.push(MirrorRestartDecision {
            database_name: db.name.clone(),
            ready_cut,
            positions: cut_state.positions,
            needs_bootstrap,
        });

        info!(
            database = %db.name,
            ready_cut,
            needs_bootstrap,
            status = ?origin.status,
            "mirror restart: scheduling observer resume"
        );
    }

    Ok(decisions)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use nodedb_types::{DatabaseId, MirrorCutState, MirrorMode, MirrorOrigin, MirrorStatus};
    use tempfile::TempDir;

    use super::*;
    use crate::control::security::catalog::SystemCatalog;
    use crate::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};

    fn open_tmp_catalog(tmp: &TempDir) -> SystemCatalog {
        let path: PathBuf = tmp.path().join("system.redb");
        SystemCatalog::open(&path).expect("open catalog")
    }

    fn make_mirror_db(id: u64, name: &str, status: MirrorStatus) -> DatabaseDescriptor {
        DatabaseDescriptor {
            id: DatabaseId::new(id),
            name: name.to_string(),
            status: DatabaseStatus::Mirroring,
            created_at_lsn: 0,
            quota_ref: 0,
            parent_clone: None,
            mirror_origin: Some(MirrorOrigin {
                source_cluster: "prod-us".to_string(),
                source_database: DatabaseId::new(0),
                source_database_name: "source_db".to_string(),
                mode: MirrorMode::Async,
                status,
            }),
            audit_dml: nodedb_types::AuditDmlMode::None,
            idle_session_timeout_secs: 0,
        }
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
        assert_eq!(d.ready_cut, 99);
        assert_eq!(d.positions, vec![position]);
        assert!(!d.needs_bootstrap);
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
    }
}
