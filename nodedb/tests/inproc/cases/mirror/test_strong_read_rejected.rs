// SPDX-License-Identifier: BUSL-1.1

//! `mirror_strong_read_rejected`: a read with `ReadConsistency::Strong`
//! on a non-promoted mirror must return `STALE_READ_NOT_LEADER`.
//!
//! This test exercises the catalog-level read gate directly without
//! requiring a live cross-cluster QUIC link.

use nodedb_types::{DatabaseId, MirrorMode, MirrorOrigin, MirrorStatus};
use tempfile::TempDir;

use nodedb::control::server::pgwire::ddl::database::{
    MirrorReadOutcome, check_mirror_read_consistency,
};
use nodedb::types::ReadConsistency;

use super::helpers::{TEST_SOURCE_CLUSTER, TEST_SOURCE_DATABASE, now_ms, open_tmp_catalog};

fn following_origin() -> MirrorOrigin {
    MirrorOrigin {
        source_cluster: TEST_SOURCE_CLUSTER.to_string(),
        source_database: DatabaseId::new(0),
        source_database_name: TEST_SOURCE_DATABASE.to_string(),
        mode: MirrorMode::Async,
        status: MirrorStatus::Following,
    }
}

#[test]
fn mirror_strong_read_rejected() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(1001);
    let origin = following_origin();

    let outcome =
        check_mirror_read_consistency(&catalog, db_id, &origin, ReadConsistency::Strong, now_ms());

    match outcome {
        MirrorReadOutcome::Reject {
            sqlstate_code,
            message,
        } => {
            assert_eq!(
                sqlstate_code,
                nodedb_types::error::sqlstate::STALE_READ_NOT_LEADER,
                "wrong SQLSTATE: {sqlstate_code}"
            );
            assert!(
                message.contains(TEST_SOURCE_CLUSTER),
                "error message should mention source cluster: {message}"
            );
        }
        MirrorReadOutcome::ServeLocally => {
            panic!("Strong read on non-promoted mirror must be rejected");
        }
    }
}

#[test]
fn mirror_strong_read_rejected_degraded() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(1002);
    let origin = MirrorOrigin {
        source_cluster: TEST_SOURCE_CLUSTER.to_string(),
        source_database: DatabaseId::new(0),
        source_database_name: TEST_SOURCE_DATABASE.to_string(),
        mode: MirrorMode::Async,
        status: MirrorStatus::Degraded,
    };

    let outcome =
        check_mirror_read_consistency(&catalog, db_id, &origin, ReadConsistency::Strong, now_ms());

    match outcome {
        MirrorReadOutcome::Reject { sqlstate_code, .. } => {
            assert_eq!(
                sqlstate_code,
                nodedb_types::error::sqlstate::STALE_READ_NOT_LEADER
            );
        }
        MirrorReadOutcome::ServeLocally => {
            panic!("Strong read on Degraded mirror must be rejected");
        }
    }
}

#[test]
fn mirror_strong_read_rejected_disconnected() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(1003);
    let origin = MirrorOrigin {
        source_cluster: TEST_SOURCE_CLUSTER.to_string(),
        source_database: DatabaseId::new(0),
        source_database_name: TEST_SOURCE_DATABASE.to_string(),
        mode: MirrorMode::Async,
        status: MirrorStatus::Disconnected,
    };

    let outcome =
        check_mirror_read_consistency(&catalog, db_id, &origin, ReadConsistency::Strong, now_ms());

    match outcome {
        MirrorReadOutcome::Reject { sqlstate_code, .. } => {
            assert_eq!(
                sqlstate_code,
                nodedb_types::error::sqlstate::STALE_READ_NOT_LEADER
            );
        }
        MirrorReadOutcome::ServeLocally => {
            panic!("Strong read on Disconnected mirror must be rejected");
        }
    }
}
