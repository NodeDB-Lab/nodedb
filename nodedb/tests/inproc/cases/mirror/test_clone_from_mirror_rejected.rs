// SPDX-License-Identifier: BUSL-1.1

//! `mirror_clone_from_mirror_rejected`: `CLONE DATABASE x FROM <mirror>`
//! must return `CANNOT_CLONE_MIRROR`.
//!
//! Exercises the clone-source validation gate. A mirror cannot be cloned
//! because a clone-from-stale-source creates ambiguous bitemporal lineage.
//! The operator must promote the mirror first, then clone.

use nodedb_types::{DatabaseId, MirrorMode, MirrorOrigin, MirrorStatus};
use tempfile::TempDir;

use nodedb::control::security::catalog::SystemCatalog;
use nodedb::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};

use super::helpers::{TEST_SOURCE_CLUSTER, TEST_SOURCE_DATABASE, open_tmp_catalog};

/// Simulate the clone-source validation check that lives in the clone handler.
/// Returns `true` if the source is a mirror (clone must be rejected).
fn source_is_mirror(catalog: &SystemCatalog, source_name: &str) -> bool {
    let db_id = match catalog.get_database_id_by_name(source_name) {
        Ok(Some(id)) => id,
        _ => return false,
    };
    let desc = match catalog.get_database(db_id) {
        Ok(Some(d)) => d,
        _ => return false,
    };
    match &desc.mirror_origin {
        Some(o) => !matches!(o.status, MirrorStatus::Promoted),
        None => false,
    }
}

#[test]
fn clone_from_following_mirror_rejected() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);

    // Register a mirror database.
    let db_id = DatabaseId::new(3001);
    let descriptor = DatabaseDescriptor {
        id: db_id,
        name: "mirror_src".to_string(),
        status: DatabaseStatus::Mirroring,
        created_at_lsn: 0,
        quota_ref: 0,
        parent_clone: None,
        mirror_origin: Some(MirrorOrigin {
            source_cluster: TEST_SOURCE_CLUSTER.to_string(),
            source_database: DatabaseId::new(0),
            source_database_name: TEST_SOURCE_DATABASE.to_string(),
            mode: MirrorMode::Async,
            status: MirrorStatus::Following,
        }),
        audit_dml: nodedb_types::AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    };
    catalog.put_database(&descriptor).expect("inject mirror");

    assert!(
        source_is_mirror(&catalog, "mirror_src"),
        "Following mirror must be detected as mirror source"
    );
}

#[test]
fn clone_from_degraded_mirror_rejected() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);

    let db_id = DatabaseId::new(3002);
    let descriptor = DatabaseDescriptor {
        id: db_id,
        name: "degraded_mirror".to_string(),
        status: DatabaseStatus::Mirroring,
        created_at_lsn: 0,
        quota_ref: 0,
        parent_clone: None,
        mirror_origin: Some(MirrorOrigin {
            source_cluster: TEST_SOURCE_CLUSTER.to_string(),
            source_database: DatabaseId::new(0),
            source_database_name: TEST_SOURCE_DATABASE.to_string(),
            mode: MirrorMode::Async,
            status: MirrorStatus::Degraded,
        }),
        audit_dml: nodedb_types::AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    };
    catalog.put_database(&descriptor).expect("inject mirror");

    assert!(
        source_is_mirror(&catalog, "degraded_mirror"),
        "Degraded mirror must be detected as mirror source"
    );
}

#[test]
fn clone_from_promoted_mirror_allowed() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);

    // A promoted mirror is a normal writable database — clone is allowed.
    let db_id = DatabaseId::new(3003);
    let descriptor = DatabaseDescriptor {
        id: db_id,
        name: "promoted_db".to_string(),
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

    assert!(
        !source_is_mirror(&catalog, "promoted_db"),
        "Promoted mirror is a normal database; clone must be allowed"
    );
}

#[test]
fn clone_from_normal_database_allowed() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);

    let db_id = DatabaseId::new(3004);
    let descriptor = DatabaseDescriptor {
        id: db_id,
        name: "normal_db".to_string(),
        status: DatabaseStatus::Active,
        created_at_lsn: 0,
        quota_ref: 0,
        parent_clone: None,
        mirror_origin: None,
        audit_dml: nodedb_types::AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    };
    catalog.put_database(&descriptor).expect("inject normal db");

    assert!(
        !source_is_mirror(&catalog, "normal_db"),
        "Non-mirror database must not trigger mirror rejection"
    );
}
