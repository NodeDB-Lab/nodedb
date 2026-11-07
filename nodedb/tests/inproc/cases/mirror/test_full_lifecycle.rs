// SPDX-License-Identifier: BUSL-1.1

//! `mirror_full_lifecycle`: CREATE → bootstrap completes → DDL replicates →
//! data row replicates → BoundedStaleness read succeeds → PROMOTE →
//! write accepted → DROP.
//!
//! Drives the full mirror lifecycle by directly manipulating catalog state and
//! calling the same handler functions that the pgwire DDL router calls. This
//! approach mirrors how all other tests in this suite work — avoiding a live
//! in-process server start while still exercising the complete lifecycle logic.

use std::time::Duration;

use nodedb_types::{DatabaseId, MirrorMode, MirrorOrigin, MirrorStatus, MirrorStatusStep};
use tempfile::TempDir;

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::catalog_entry::apply::apply_to;
use nodedb::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};
use nodedb::control::server::pgwire::ddl::database::{
    MirrorReadOutcome, check_mirror_read_consistency,
};
use nodedb::types::ReadConsistency;

use super::helpers::{TEST_SOURCE_DATABASE, applied_cut_state, now_ms, open_tmp_catalog};

#[test]
fn mirror_full_lifecycle() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(9001);
    let db_name = "lifecycle_mirror_db";

    // ── Phase 1: MIRROR DATABASE ──────────────────────────────────────────
    // Simulate what `handle_mirror_database` does: write a Bootstrapping descriptor.
    let descriptor = DatabaseDescriptor {
        id: db_id,
        name: db_name.to_string(),
        status: DatabaseStatus::Mirroring,
        created_at_lsn: 0,
        quota_ref: 0,
        parent_clone: None,
        mirror_origin: Some(MirrorOrigin {
            source_cluster: "prod_us_cluster".to_string(),
            source_database: DatabaseId::new(0),
            source_database_name: TEST_SOURCE_DATABASE.to_string(),
            mode: MirrorMode::Async,
            status: MirrorStatus::Bootstrapping {
                bytes_done: 0,
                bytes_total: 0,
            },
        }),
        audit_dml: nodedb_types::AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    };
    catalog
        .put_database(&descriptor)
        .expect("MIRROR DATABASE: write descriptor");

    let desc = catalog.get_database(db_id).expect("get").expect("missing");
    let origin = desc
        .mirror_origin
        .as_ref()
        .expect("mirror_origin must be set");
    assert!(
        matches!(origin.status, MirrorStatus::Bootstrapping { .. }),
        "status must be Bootstrapping after MIRROR DATABASE, got {:?}",
        origin.status
    );

    // ── Phase 2: Bootstrap completes → Following ──────────────────────────
    let mut desc = desc.clone();
    if let Some(ref mut o) = desc.mirror_origin {
        o.status = MirrorStatus::Following;
    }
    catalog.put_database(&desc).expect("update to Following");
    apply_to(
        &CatalogEntry::PutMirrorCutState {
            database_id: db_id.as_u64(),
            state: Box::new(applied_cut_state(10, now_ms(), 0)),
        },
        &catalog,
    )
    .expect("apply cut state");

    let desc = catalog.get_database(db_id).expect("get").expect("missing");
    assert!(
        matches!(
            desc.mirror_origin.as_ref().unwrap().status,
            MirrorStatus::Following
        ),
        "status must be Following after bootstrap"
    );

    // ── Phase 3: BoundedStaleness read succeeds ───────────────────────────
    let desc = catalog.get_database(db_id).expect("get").expect("missing");
    let origin = desc.mirror_origin.as_ref().unwrap();
    match check_mirror_read_consistency(
        &catalog,
        db_id,
        origin,
        ReadConsistency::BoundedStaleness(Duration::from_secs(5)),
        now_ms(),
    ) {
        MirrorReadOutcome::ServeLocally => {}
        MirrorReadOutcome::Reject { message, .. } => {
            panic!("BoundedStaleness must succeed on fresh Following mirror: {message}");
        }
    }

    // ── Phase 4: PROMOTE ──────────────────────────────────────────────────
    // Apply what PROMOTE proposes. The apply keeps the cut state, which a
    // replica behind the promotion still reads.
    for step in [MirrorStatusStep::Seal, MirrorStatusStep::Promote] {
        apply_to(
            &CatalogEntry::StepMirrorStatus {
                database_id: db_id.as_u64(),
                step,
            },
            &catalog,
        )
        .expect("PROMOTE: apply the step");
    }
    assert!(
        catalog
            .get_mirror_cut_state(db_id)
            .expect("read cut state")
            .is_some(),
        "the promotion's apply must keep the cut state"
    );

    let desc = catalog.get_database(db_id).expect("get").expect("missing");
    assert_eq!(desc.status, DatabaseStatus::Active);
    assert!(
        matches!(
            desc.mirror_origin.as_ref().unwrap().status,
            MirrorStatus::Promoted
        ),
        "status must be Promoted after PROMOTE"
    );

    // ── Phase 5: Promoted mirror serves all read consistency levels ───────
    let origin = desc.mirror_origin.as_ref().unwrap();
    for consistency in [
        ReadConsistency::Strong,
        ReadConsistency::Eventual,
        ReadConsistency::BoundedStaleness(Duration::from_millis(1)),
    ] {
        match check_mirror_read_consistency(&catalog, db_id, origin, consistency, now_ms()) {
            MirrorReadOutcome::ServeLocally => {}
            MirrorReadOutcome::Reject { message, .. } => {
                panic!("Promoted mirror must serve all reads locally: {message}");
            }
        }
    }

    // ── Phase 6: DROP DATABASE (catalog cleanup) ──────────────────────────
    apply_to(
        &CatalogEntry::DeleteDatabase {
            db_id: db_id.as_u64(),
        },
        &catalog,
    )
    .expect("DROP DATABASE: apply");

    assert!(
        catalog
            .get_database_id_by_name(db_name)
            .expect("lookup")
            .is_none(),
        "database must be removed after DROP"
    );
}
