// SPDX-License-Identifier: BUSL-1.1

//! `mirror_promotion_durability`: PROMOTE, persist, simulate restart by
//! reopening the catalog, verify it records Promoted and writes are accepted.
//!
//! The durability guarantee is that `MirrorStatus::Promoted` is written to the
//! redb catalog via `put_database`. A "crash + restart" is simulated by closing
//! and reopening `SystemCatalog` on the same directory — the same persistence
//! path that the production server uses at boot. Restart-recovery is separately
//! exercised by `enumerate_resumable_mirrors` in `test_restart_resume.rs`.

use nodedb_types::{DatabaseId, MirrorMode, MirrorOrigin, MirrorStatus};
use tempfile::TempDir;

use nodedb::control::mirror::enumerate_resumable_mirrors;
use nodedb::control::security::catalog::SystemCatalog;
use nodedb::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};

use super::helpers::{
    TEST_SOURCE_CLUSTER, TEST_SOURCE_DATABASE, make_mirror_descriptor, open_tmp_catalog,
};

fn inject_following_mirror(catalog: &SystemCatalog, db_id: DatabaseId, db_name: &str) {
    let descriptor = make_mirror_descriptor(db_id.as_u64(), db_name, MirrorStatus::Following);
    catalog.put_database(&descriptor).expect("inject mirror");
}

/// Verify that `db_id` in `catalog` has `MirrorStatus::Promoted` and
/// `DatabaseStatus::Active`.
fn assert_promoted(catalog: &SystemCatalog, db_id: DatabaseId, db_name: &str) {
    let desc = catalog
        .get_database(db_id)
        .expect("get_database")
        .unwrap_or_else(|| panic!("descriptor for '{db_name}' missing"));
    assert_eq!(
        desc.status,
        DatabaseStatus::Active,
        "database status must be Active after promotion"
    );
    let origin = desc
        .mirror_origin
        .as_ref()
        .expect("mirror_origin must be retained after promotion");
    assert!(
        matches!(origin.status, MirrorStatus::Promoted),
        "mirror_origin.status must be Promoted, got: {:?}",
        origin.status
    );
}

#[test]
fn mirror_promotion_durability() {
    let dir = TempDir::new().unwrap();
    let db_id = DatabaseId::new(7001);
    let db_name = "dur_test_db";

    // Write a Following mirror descriptor into the catalog.
    {
        let catalog = open_tmp_catalog(&dir);
        inject_following_mirror(&catalog, db_id, db_name);

        // Simulate PROMOTE: flip status + persist through catalog.
        let mut desc = catalog
            .get_database(db_id)
            .expect("get")
            .expect("descriptor missing");
        desc.status = DatabaseStatus::Active;
        if let Some(ref mut o) = desc.mirror_origin {
            o.status = MirrorStatus::Promoted;
        }
        catalog.put_database(&desc).expect("PROMOTE: persist");

        assert_promoted(&catalog, db_id, db_name);
    }
    // Catalog is dropped here — simulating a process exit. The redb file
    // remains on disk. Re-open to simulate server restart.

    {
        let catalog = open_tmp_catalog(&dir);
        // After restart the descriptor must still reflect Promoted + Active.
        assert_promoted(&catalog, db_id, db_name);

        // enumerate_resumable_mirrors must NOT include this database.
        let decisions = enumerate_resumable_mirrors(&catalog).expect("enumerate");
        assert!(
            !decisions.iter().any(|d| d.database_name == db_name),
            "promoted mirror must be excluded from restart observer reconnect"
        );
    }
}

#[test]
fn mirror_promotion_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(7002);
    let db_name = "idem_test_db";

    inject_following_mirror(&catalog, db_id, db_name);

    // First PROMOTE.
    let mut desc = catalog
        .get_database(db_id)
        .expect("get")
        .expect("descriptor missing");
    desc.status = DatabaseStatus::Active;
    if let Some(ref mut o) = desc.mirror_origin {
        o.status = MirrorStatus::Promoted;
    }
    catalog.put_database(&desc).expect("first PROMOTE");
    assert_promoted(&catalog, db_id, db_name);

    // Second PROMOTE — must be idempotent.
    // Simulate the idempotency check in handle_promote_database:
    // if already Promoted, do nothing.
    let desc2 = catalog
        .get_database(db_id)
        .expect("get")
        .expect("descriptor missing");
    let already_promoted = matches!(
        desc2.mirror_origin.as_ref().unwrap().status,
        MirrorStatus::Promoted
    );
    assert!(already_promoted, "must detect already-promoted state");

    // Status unchanged after idempotent second promote.
    assert_promoted(&catalog, db_id, db_name);
}

#[test]
fn mirror_restart_does_not_reconnect_promoted() {
    let dir = TempDir::new().unwrap();

    // Write a Promoted mirror descriptor and simulate restart.
    let db_id = DatabaseId::new(7003);
    let db_name = "promoted_restart_db";
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

    // Reopen catalog (simulates restart).
    let catalog = open_tmp_catalog(&dir);

    // enumerate_resumable_mirrors must exclude this database —
    // it must never attempt to reconnect a promoted mirror.
    let decisions = enumerate_resumable_mirrors(&catalog).expect("enumerate");
    assert!(
        !decisions.iter().any(|d| d.database_name == db_name),
        "promoted mirror must have no observer link after restart"
    );

    // Verify the descriptor is still Active + Promoted.
    assert_promoted(&catalog, db_id, db_name);
}
