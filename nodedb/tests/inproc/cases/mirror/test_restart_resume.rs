// SPDX-License-Identifier: BUSL-1.1

//! `mirror_restart_resume`: kill mirror server, restart, verify it
//! reconnects from its cut state without requiring a fresh snapshot.
//!
//! Tests the restart-resume catalog logic: `enumerate_resumable_mirrors`
//! must return the resume positions from `_system.mirror_cut_state` and must
//! exclude promoted mirrors.

use nodedb_types::{
    DatabaseId, MirrorCutState, MirrorGroupPosition, MirrorMode, MirrorOrigin, MirrorStatus,
};
use tempfile::TempDir;

use nodedb::control::mirror::enumerate_resumable_mirrors;
use nodedb::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};

use super::helpers::{
    TEST_SOURCE_CLUSTER, TEST_SOURCE_DATABASE, make_mirror_descriptor, open_tmp_catalog,
};

#[test]
fn mirror_restart_resume_uses_the_cut_state() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);

    let db_id = DatabaseId::new(4001);
    catalog
        .put_database(&make_mirror_descriptor(
            db_id.as_u64(),
            "resume_db",
            MirrorStatus::Following,
        ))
        .unwrap();
    let position = MirrorGroupPosition {
        group_id: 3,
        log_index: 120,
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
        .find(|d| d.database_name == "resume_db")
        .expect("resume_db must be in decisions");

    assert_eq!(d.ready_cut, 99, "must resume after the ready cut");
    assert_eq!(d.positions, vec![position]);
    assert!(!d.needs_bootstrap, "a ready cut must not require bootstrap");
}

#[test]
fn mirror_restart_resume_excludes_promoted() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);

    catalog
        .put_database(&make_mirror_descriptor(
            4002,
            "promoted_db",
            MirrorStatus::Promoted,
        ))
        .unwrap();

    let decisions = enumerate_resumable_mirrors(&catalog).unwrap();
    assert!(
        !decisions.iter().any(|d| d.database_name == "promoted_db"),
        "promoted mirror must be excluded from restart decisions"
    );
}

#[test]
fn mirror_restart_resume_no_cut_state_needs_bootstrap() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);

    catalog
        .put_database(&make_mirror_descriptor(
            4003,
            "fresh_mirror",
            MirrorStatus::Bootstrapping {
                bytes_done: 0,
                bytes_total: 0,
            },
        ))
        .unwrap();

    let decisions = enumerate_resumable_mirrors(&catalog).unwrap();
    let d = decisions
        .iter()
        .find(|d| d.database_name == "fresh_mirror")
        .expect("fresh_mirror must be in decisions");

    assert_eq!(d.ready_cut, 0);
    assert!(d.positions.is_empty());
    assert!(
        d.needs_bootstrap,
        "no ready cut must require fresh snapshot"
    );
}

/// Verify that a promoted mirror is excluded from the restart observer reconnect
/// enumeration — the restart-resume path must never try to reconnect a promoted
/// database to its former source cluster.
///
/// This test duplicates the assertion in `test_promotion_durability.rs`'s
/// `mirror_restart_does_not_reconnect_promoted` but with explicit catalog
/// persistence and reload (simulating a crash + restart cycle).
#[test]
fn mirror_restart_does_not_reconnect_promoted() {
    let dir = TempDir::new().unwrap();

    // Write a Promoted descriptor.
    let db_id = DatabaseId::new(4099);
    let db_name = "promoted_no_reconnect";
    {
        let catalog = open_tmp_catalog(&dir);
        let descriptor = DatabaseDescriptor {
            id: db_id,
            name: db_name.to_string(),
            status: DatabaseStatus::Active,
            created_at_lsn: 0,
            quota_ref: 0,
            parent_clone: None,
            mirror_origin: Some(MirrorOrigin {
                source_cluster: TEST_SOURCE_CLUSTER.to_string(),
                source_database: DatabaseId::new(0),
                source_database_name: TEST_SOURCE_DATABASE.to_string(),
                mode: MirrorMode::Async,
                status: MirrorStatus::Promoted,
            }),
            audit_dml: nodedb_types::AuditDmlMode::None,
            idle_session_timeout_secs: 0,
        };
        catalog.put_database(&descriptor).expect("inject promoted");
    }

    // Reopen to simulate restart — same as `enumerate_resumable_mirrors`
    // would be called at server boot.
    let catalog = open_tmp_catalog(&dir);
    let decisions = enumerate_resumable_mirrors(&catalog).expect("enumerate");
    assert!(
        !decisions.iter().any(|d| d.database_name == db_name),
        "promoted mirror must have no observer link after restart"
    );
}
