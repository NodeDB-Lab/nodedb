// SPDX-License-Identifier: BUSL-1.1

//! `SET QUOTA` must change what the server does, not only what it stores.
//!
//! A persisted `QuotaRecord` is inert on its own. The admission registry holds
//! the connection semaphores and the memory governor holds the byte ceilings,
//! so the DDL handler pushes both after the catalog write succeeds. These
//! tests assert on those live components, not on `SHOW QUOTA`: a test that
//! reads the record back passes with the apply step deleted.

use nodedb_mem::{engine::EngineId, error::MemError};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{DatabaseId, TenantId};

/// Tenant id `CREATE TENANT ... ID 2` assigns.
const TENANT: u64 = 2;

/// One mebibyte, the tenant memory ceiling these tests configure.
const MIB: u64 = 1024 * 1024;

/// Starts a server and creates tenant `acme` with id [`TENANT`], the shared
/// fixture for every tenant-quota test below.
async fn start_with_tenant() -> TestServer {
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE TENANT acme ID {TENANT}"))
        .await
        .expect("CREATE TENANT");
    server
}

/// `ALTER DATABASE … SET QUOTA (max_connections = N)` installs the cap in the
/// admission registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_database_quota_applies_connection_cap() {
    let server = TestServer::start().await;
    let db = DatabaseId::DEFAULT;
    let registry = &server.shared.admission_registry;

    assert_eq!(
        registry.database_live_connections(db),
        None,
        "no cap is configured before the ALTER"
    );

    server
        .exec("ALTER DATABASE default SET QUOTA (max_connections = 2)")
        .await
        .expect("ALTER DATABASE SET QUOTA");

    assert_eq!(
        registry.database_live_connections(db),
        Some(0),
        "the cap must exist in the registry with no connection holding it"
    );

    let _p1 = registry
        .try_acquire_database(db)
        .expect("first admission")
        .expect("a configured cap hands out a permit");
    let _p2 = registry
        .try_acquire_database(db)
        .expect("second admission")
        .expect("a configured cap hands out a permit");
    registry
        .try_acquire_database(db)
        .expect_err("the third connection must be refused by the cap of 2");
}

/// `max_connections = 0` clears the database cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_database_quota_zero_clears_connection_cap() {
    let server = TestServer::start().await;
    let db = DatabaseId::DEFAULT;
    let registry = &server.shared.admission_registry;

    server
        .exec("ALTER DATABASE default SET QUOTA (max_connections = 2)")
        .await
        .expect("ALTER DATABASE SET QUOTA");
    assert_eq!(registry.database_live_connections(db), Some(0));

    server
        .exec("ALTER DATABASE default SET QUOTA (max_connections = 0)")
        .await
        .expect("ALTER DATABASE SET QUOTA clearing the cap");

    assert_eq!(
        registry.database_live_connections(db),
        None,
        "zero drops the entry, so the database is uncapped again"
    );
    let permit = registry
        .try_acquire_database(db)
        .expect("an uncapped database admits");
    assert!(
        permit.is_some(),
        "an uncapped database hands out an unconstrained permit"
    );
}

/// `ALTER TENANT … IN DATABASE … SET QUOTA` applies both the connection cap
/// and the memory ceiling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_tenant_quota_applies_connection_cap_and_memory_budget() {
    let server = start_with_tenant().await;
    let db = DatabaseId::DEFAULT;
    let tenant = TenantId::new(TENANT);
    let registry = &server.shared.admission_registry;
    let gov = server.shared.governor.clone();

    assert_eq!(
        registry.tenant_live_connections(db, tenant),
        None,
        "no tenant cap before the ALTER"
    );
    {
        let token = gov
            .try_reserve(db, tenant, EngineId::DocumentSchemaless, 2 * MIB as usize)
            .expect("an unbudgeted tenant reserves freely");
        drop(token);
    }

    server
        .exec(&format!(
            "ALTER TENANT acme IN DATABASE default SET QUOTA \
             (max_connections = 2, max_memory_bytes = {MIB})"
        ))
        .await
        .expect("ALTER TENANT SET QUOTA");

    assert_eq!(
        registry.tenant_live_connections(db, tenant),
        Some(0),
        "the tenant cap must exist in the registry"
    );
    let _p1 = registry
        .try_acquire_tenant(db, tenant)
        .expect("first tenant admission")
        .expect("a configured cap hands out a permit");
    let _p2 = registry
        .try_acquire_tenant(db, tenant)
        .expect("second tenant admission")
        .expect("a configured cap hands out a permit");
    registry
        .try_acquire_tenant(db, tenant)
        .expect_err("the third tenant connection must be refused by the cap of 2");

    let err = gov
        .try_reserve(db, tenant, EngineId::DocumentSchemaless, 2 * MIB as usize)
        .expect_err("2 MiB must exceed the 1 MiB tenant ceiling");
    assert!(
        matches!(err, MemError::TenantBudgetExhausted { .. }),
        "the denial must come from the tenant budget, got {err:?}"
    );
}

/// The unqualified `ALTER TENANT <name> SET QUOTA <field> = <value>` form
/// writes the session database's quota row. A handler that mutates only the
/// in-memory tenant view leaves no row, so this test fails outright.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_tenant_string_form_persists_every_field() {
    let server = start_with_tenant().await;
    let db = DatabaseId::DEFAULT;
    let tenant = TenantId::new(TENANT);

    for stmt in [
        format!("ALTER TENANT acme SET QUOTA max_memory_bytes = {MIB}"),
        "ALTER TENANT acme SET QUOTA max_qps = 250".to_string(),
        "ALTER TENANT acme SET QUOTA max_concurrent_requests = 64".to_string(),
        "ALTER TENANT acme SET QUOTA max_vector_dim = 512".to_string(),
        "ALTER TENANT acme SET QUOTA max_graph_depth = 4".to_string(),
        "ALTER TENANT acme SET QUOTA deactivated_collection_retention_days = 14".to_string(),
    ] {
        server.exec(&stmt).await.expect("ALTER TENANT SET QUOTA");
    }

    let stored = server
        .shared
        .credentials
        .catalog()
        .get_tenant_quota(db, tenant)
        .expect("quota read")
        .expect("the string form must leave a persisted row");

    assert_eq!(stored.max_memory_bytes, MIB);
    assert_eq!(stored.max_qps, 250);
    assert_eq!(stored.max_concurrent_requests, 64);
    assert_eq!(stored.max_vector_dim, 512);
    assert_eq!(stored.max_graph_depth, 4);
    assert_eq!(stored.deactivated_collection_retention_days, Some(14));
}

/// The string form reaches the same live enforcement the `IN DATABASE` form
/// reaches: the memory governor and the tenant isolation view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_tenant_string_form_applies_live_enforcement() {
    let server = start_with_tenant().await;
    let db = DatabaseId::DEFAULT;
    let tenant = TenantId::new(TENANT);
    let gov = server.shared.governor.clone();

    server
        .exec(&format!(
            "ALTER TENANT acme SET QUOTA max_memory_bytes = {MIB}"
        ))
        .await
        .expect("ALTER TENANT SET QUOTA");
    server
        .exec("ALTER TENANT acme SET QUOTA max_graph_depth = 4")
        .await
        .expect("ALTER TENANT SET QUOTA");

    let err = gov
        .try_reserve(db, tenant, EngineId::DocumentSchemaless, 2 * MIB as usize)
        .expect_err("2 MiB must exceed the 1 MiB tenant ceiling");
    assert!(
        matches!(err, MemError::TenantBudgetExhausted { .. }),
        "the denial must come from the tenant budget, got {err:?}"
    );

    let tenants = server
        .shared
        .tenants
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    assert_eq!(
        tenants.quota(tenant).max_graph_depth,
        4,
        "the graph depth gate reads the isolation view, so the record must land there"
    );
}

/// `max_connections = 0` clears the tenant cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_tenant_quota_zero_clears_connection_cap() {
    let server = start_with_tenant().await;
    let db = DatabaseId::DEFAULT;
    let tenant = TenantId::new(TENANT);
    let registry = &server.shared.admission_registry;

    server
        .exec("ALTER TENANT acme IN DATABASE default SET QUOTA (max_connections = 2)")
        .await
        .expect("ALTER TENANT SET QUOTA");
    assert_eq!(registry.tenant_live_connections(db, tenant), Some(0));

    server
        .exec("ALTER TENANT acme IN DATABASE default SET QUOTA (max_connections = 0)")
        .await
        .expect("ALTER TENANT SET QUOTA clearing the cap");

    assert_eq!(
        registry.tenant_live_connections(db, tenant),
        None,
        "zero drops the entry, so the tenant is uncapped again"
    );
}
