// SPDX-License-Identifier: BUSL-1.1

//! Dropping a database or tenant must take its quota with it.
//!
//! A stale quota row keeps consuming the sum-of-quotas ceiling and lets a
//! recycled id inherit a dead cap, so the drop path purges the row and
//! releases live enforcement. These tests assert on the catalog row, the
//! admission registry, and the memory governor — reading `SHOW QUOTA` back
//! would pass with the cleanup deleted.

use nodedb_mem::{engine::EngineId, error::MemError};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{DatabaseId, TenantId};

/// Tenant id `CREATE TENANT ... ID 2` assigns.
const TENANT: u64 = 2;

/// Second tenant id, used to prove a drop is scoped to one tenant.
const OTHER_TENANT: u64 = 3;

/// One mebibyte, the memory ceiling these tests configure.
const MIB: u64 = 1024 * 1024;

/// Resolve a database name to its id.
fn db_id(server: &TestServer, name: &str) -> DatabaseId {
    server
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(name)
        .expect("catalog lookup")
        .expect("the database exists")
}

/// Dropping a database removes its quota row, connection cap, and memory
/// budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_database_removes_quota_row_and_releases_caps() {
    let server = TestServer::start().await;
    server
        .exec("CREATE DATABASE quota_drop_db")
        .await
        .expect("CREATE DATABASE");
    let db = db_id(&server, "quota_drop_db");
    let tenant = TenantId::new(0);
    let registry = &server.shared.admission_registry;
    let gov = server.shared.governor.clone();

    server
        .exec(&format!(
            "ALTER DATABASE quota_drop_db SET QUOTA \
             (max_connections = 2, max_memory_bytes = {MIB})"
        ))
        .await
        .expect("ALTER DATABASE SET QUOTA");
    assert_eq!(registry.database_live_connections(db), Some(0));
    let err = gov
        .try_reserve(db, tenant, EngineId::DocumentSchemaless, 2 * MIB as usize)
        .expect_err("2 MiB must exceed the 1 MiB database ceiling");
    assert!(
        matches!(err, MemError::DatabaseBudgetExhausted { .. }),
        "the denial must come from the database budget, got {err:?}"
    );

    server
        .exec("DROP DATABASE quota_drop_db")
        .await
        .expect("DROP DATABASE");

    assert!(
        server
            .shared
            .credentials
            .catalog()
            .get_database_quota(db)
            .expect("quota read")
            .is_none(),
        "the quota row must not outlive the database"
    );
    assert_eq!(
        registry.database_live_connections(db),
        None,
        "the connection cap must be released"
    );
    let token = gov
        .try_reserve(db, tenant, EngineId::DocumentSchemaless, 2 * MIB as usize)
        .expect("the memory budget must be released");
    drop(token);
}

/// Dropping a database removes the quota rows of the tenants inside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_database_removes_tenant_quota_rows() {
    let server = TestServer::start().await;
    server
        .exec("CREATE DATABASE quota_drop_tenants")
        .await
        .expect("CREATE DATABASE");
    server
        .exec(&format!("CREATE TENANT acme ID {TENANT}"))
        .await
        .expect("CREATE TENANT");
    let db = db_id(&server, "quota_drop_tenants");
    let tenant = TenantId::new(TENANT);
    let registry = &server.shared.admission_registry;

    server
        .exec(&format!(
            "ALTER TENANT acme IN DATABASE quota_drop_tenants SET QUOTA \
             (max_connections = 2, max_memory_bytes = {MIB})"
        ))
        .await
        .expect("ALTER TENANT SET QUOTA");
    assert_eq!(registry.tenant_live_connections(db, tenant), Some(0));

    server
        .exec("DROP DATABASE quota_drop_tenants")
        .await
        .expect("DROP DATABASE");

    assert!(
        server
            .shared
            .credentials
            .catalog()
            .get_tenant_quota(db, tenant)
            .expect("quota read")
            .is_none(),
        "a database drop must not orphan its tenants' quota rows"
    );
    assert_eq!(
        registry.tenant_live_connections(db, tenant),
        None,
        "the tenant connection cap must be released"
    );
}

/// Dropping a tenant removes that tenant's quota row and no other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_tenant_removes_only_its_quota_row() {
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE TENANT acme ID {TENANT}"))
        .await
        .expect("CREATE TENANT acme");
    server
        .exec(&format!("CREATE TENANT globex ID {OTHER_TENANT}"))
        .await
        .expect("CREATE TENANT globex");
    let db = DatabaseId::DEFAULT;
    let dropped = TenantId::new(TENANT);
    let kept = TenantId::new(OTHER_TENANT);
    let registry = &server.shared.admission_registry;

    for tenant_name in ["acme", "globex"] {
        server
            .exec(&format!(
                "ALTER TENANT {tenant_name} IN DATABASE default SET QUOTA (max_connections = 2)"
            ))
            .await
            .expect("ALTER TENANT SET QUOTA");
    }

    server
        .exec("DROP TENANT acme")
        .await
        .expect("DROP TENANT acme");

    let catalog = server.shared.credentials.catalog();
    assert!(
        catalog
            .get_tenant_quota(db, dropped)
            .expect("quota read")
            .is_none(),
        "the dropped tenant's quota row must be gone"
    );
    assert!(
        catalog
            .get_tenant_quota(db, kept)
            .expect("quota read")
            .is_some(),
        "the surviving tenant keeps its quota row"
    );
    assert_eq!(registry.tenant_live_connections(db, dropped), None);
    assert_eq!(registry.tenant_live_connections(db, kept), Some(0));
}

/// Dropping a database that never had a quota succeeds silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_database_without_quota_row_succeeds() {
    let server = TestServer::start().await;
    server
        .exec("CREATE DATABASE quota_drop_plain")
        .await
        .expect("CREATE DATABASE");
    let db = db_id(&server, "quota_drop_plain");

    server
        .exec("DROP DATABASE quota_drop_plain")
        .await
        .expect("a database with no quota row drops cleanly");

    assert!(
        server
            .shared
            .credentials
            .catalog()
            .get_database_quota(db)
            .expect("quota read")
            .is_none()
    );
}

/// A dropped database id is uncapped again, so reuse inherits nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_database_id_does_not_inherit_cap() {
    let server = TestServer::start().await;
    server
        .exec("CREATE DATABASE quota_drop_reuse")
        .await
        .expect("CREATE DATABASE");
    let db = db_id(&server, "quota_drop_reuse");
    let registry = &server.shared.admission_registry;

    server
        .exec("ALTER DATABASE quota_drop_reuse SET QUOTA (max_connections = 1)")
        .await
        .expect("ALTER DATABASE SET QUOTA");
    let permit = registry
        .try_acquire_database(db)
        .expect("first admission")
        .expect("a configured cap hands out a permit");
    registry
        .try_acquire_database(db)
        .expect_err("the cap of 1 refuses the second connection");
    drop(permit);

    server
        .exec("DROP DATABASE quota_drop_reuse")
        .await
        .expect("DROP DATABASE");

    assert_eq!(
        registry.database_live_connections(db),
        None,
        "the dead cap must not survive for a database reusing this id"
    );
    let permit = registry
        .try_acquire_database(db)
        .expect("an uncapped database admits");
    assert!(
        permit.is_some(),
        "an uncapped database hands out an unconstrained permit"
    );
}
