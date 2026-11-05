// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for mirror integration tests.
//!
//! Two access modes:
//!
//! 1. **Catalog-only fixtures** (used by most tests in this directory):
//!    [`open_tmp_catalog`], [`make_mirror_descriptor`], [`inject_mirror`],
//!    [`inject_lag_record_for_id`]. These fabricate `SystemCatalog` state
//!    on a temp dir without standing up a server.
//!
//! 2. **Two-server pgwire fixtures** ([`inject_mirror_descriptor`],
//!    [`assert_sqlstate`]) — drive a live `TestServer` for end-to-end SQL
//!    behaviour. The transport between source and mirror uses real QUIC
//!    (nexar+quinn) via `CrossClusterLink` — no mocks.
//!
//! Both modes share [`now_ms`].

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::metadata_proposer::propose_catalog_entry_async;
use nodedb::control::security::catalog::SystemCatalog;
use nodedb::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};
pub use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{DatabaseId, Lsn, MirrorLagRecord, MirrorMode, MirrorOrigin, MirrorStatus};

/// Default source cluster id used by every fixture in this module.
pub const TEST_SOURCE_CLUSTER: &str = "prod-us-test";

/// Open a fresh `SystemCatalog` rooted at `dir/system.redb`.
///
/// The catalog file lifetime is bound to `dir`; dropping the [`TempDir`]
/// removes the file.
pub fn open_tmp_catalog(dir: &TempDir) -> SystemCatalog {
    let path: PathBuf = dir.path().join("system.redb");
    SystemCatalog::open(&path).expect("open catalog")
}

/// Build a mirror [`DatabaseDescriptor`] with the supplied identity and status.
///
/// `DatabaseStatus` is derived from `status`: `Promoted` ⇒ `Active`,
/// everything else ⇒ `Mirroring`. The `source_database` is `DatabaseId::new(0)`
/// and the source cluster is [`TEST_SOURCE_CLUSTER`].
pub fn make_mirror_descriptor(
    id: u64,
    name: &str,
    status: MirrorStatus,
    last_applied_lsn: u64,
) -> DatabaseDescriptor {
    DatabaseDescriptor {
        id: DatabaseId::new(id),
        name: name.to_string(),
        status: match &status {
            MirrorStatus::Promoted => DatabaseStatus::Active,
            MirrorStatus::Bootstrapping { .. }
            | MirrorStatus::Following
            | MirrorStatus::Degraded { .. }
            | MirrorStatus::Disconnected => DatabaseStatus::Mirroring,
        },
        created_at_lsn: 0,
        quota_ref: 0,
        parent_clone: None,
        mirror_origin: Some(MirrorOrigin {
            source_cluster: TEST_SOURCE_CLUSTER.to_string(),
            source_database: DatabaseId::new(0),
            mode: MirrorMode::Async,
            last_applied: Lsn::new(last_applied_lsn),
            status,
        }),
        audit_dml: nodedb_types::AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    }
}

/// Persist a mirror descriptor in `catalog`. Convenience over
/// [`make_mirror_descriptor`] + [`SystemCatalog::put_database`].
pub fn inject_mirror(catalog: &SystemCatalog, db_id: DatabaseId, name: &str, status: MirrorStatus) {
    let descriptor = make_mirror_descriptor(db_id.as_u64(), name, status, 0);
    catalog
        .put_database(&descriptor)
        .expect("inject mirror descriptor");
}

/// Write a `MirrorLagRecord` for `db_id` directly via the catalog handle.
/// `last_apply_ms` is computed as `now_ms - lag_offset_ms`.
pub fn inject_lag_record_for_id(
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    lag_offset_ms: u64,
    lsn: u64,
) {
    let record = MirrorLagRecord {
        last_applied_lsn: Lsn::new(lsn),
        last_apply_ms: now_ms().saturating_sub(lag_offset_ms),
    };
    catalog
        .put_mirror_lag(db_id, &record)
        .expect("inject lag record");
}

/// Propose `descriptor` through the metadata group, so the catalog and every
/// mirror gate on the node see it.
async fn propose_descriptor(server: &TestServer, descriptor: DatabaseDescriptor) {
    propose_catalog_entry_async(
        &server.shared,
        &CatalogEntry::PutDatabase(Box::new(descriptor)),
    )
    .await
    .expect("propose database descriptor");
}

/// Create a mirror database in `server`, as if `MIRROR DATABASE` had been
/// issued.
///
/// `status` must be one of `MirrorStatus::Following`, `Degraded`, etc.
pub async fn inject_mirror_descriptor(
    server: &TestServer,
    name: &str,
    status: MirrorStatus,
    last_applied_lsn: u64,
) {
    let db_id = nodedb::control::database::allocate_database_id(&server.shared)
        .await
        .expect("allocate mirror database id");
    let descriptor = make_mirror_descriptor(db_id.as_u64(), name, status, last_applied_lsn);
    propose_descriptor(server, descriptor).await;
}

/// Turn the existing database `name` into a mirror. Its collections stay, so a
/// statement against it reaches the mirror gates and not a missing-object
/// refusal.
pub async fn mark_database_mirror(server: &TestServer, name: &str, status: MirrorStatus) {
    let catalog = server.shared.credentials.catalog();
    let db_id = catalog
        .get_database_id_by_name(name)
        .expect("catalog lookup")
        .expect("database exists");
    let mut descriptor = catalog
        .get_database(db_id)
        .expect("catalog read")
        .expect("descriptor exists");
    descriptor.status = DatabaseStatus::Mirroring;
    descriptor.mirror_origin = Some(MirrorOrigin {
        source_cluster: TEST_SOURCE_CLUSTER.to_string(),
        source_database: DatabaseId::new(0),
        mode: MirrorMode::Async,
        last_applied: Lsn::new(1),
        status,
    });
    propose_descriptor(server, descriptor).await;
}

/// Write a `MirrorLagRecord` for `db_name` in `server`'s catalog with
/// `last_apply_ms` set to `now_ms - lag_offset_ms`.
pub fn inject_lag_record(server: &TestServer, db_name: &str, lag_offset_ms: u64, lsn: u64) {
    let catalog = server.shared.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(db_name)
        .expect("catalog lookup")
        .expect("db not found");

    inject_lag_record_for_id(catalog, db_id, lag_offset_ms, lsn);
}

/// Assert that a pgwire query returns a specific SQLSTATE error code.
pub async fn assert_sqlstate(client: &tokio_postgres::Client, query: &str, expected_state: &str) {
    let err = client.simple_query(query).await.expect_err(&format!(
        "expected error with SQLSTATE {expected_state} but query succeeded: {query}"
    ));
    let pg_err = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DB error but got: {err:?}"));
    assert_eq!(
        pg_err.code().code(),
        expected_state,
        "wrong SQLSTATE for query {query:?}; error: {pg_err:?}"
    );
}

/// Return the current wall-clock milliseconds since UNIX epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}
