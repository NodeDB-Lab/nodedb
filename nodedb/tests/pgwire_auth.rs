use std::sync::Arc;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::security::audit::AuditEvent;
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use nodedb::control::server::pgwire::ddl;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;
use nodedb::wal::WalManager;
use tokio_postgres::SimpleQueryMessage;

/// Create a minimal SharedState (no Data Plane needed for DDL tests).
fn make_state() -> Arc<SharedState> {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    SharedState::new(dispatcher, wal)
}

/// Superuser identity for DDL tests.
fn superuser() -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 0,
        username: "admin".into(),
        tenant_id: TenantId::new(0),
        auth_method: AuthMethod::Trust,
        roles: vec![Role::Superuser],
        is_superuser: true,
    }
}

/// Readonly identity for permission tests.
fn readonly_user() -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 99,
        username: "viewer".into(),
        tenant_id: TenantId::new(1),
        auth_method: AuthMethod::Trust,
        roles: vec![Role::ReadOnly],
        is_superuser: false,
    }
}

/// Helper: run DDL, expect success.
async fn ddl_ok(state: &SharedState, identity: &AuthenticatedIdentity, sql: &str) {
    let result = ddl::dispatch(state, identity, sql).await;
    assert!(result.is_some(), "DDL not recognized: {sql}");
    result
        .unwrap()
        .unwrap_or_else(|e| panic!("DDL failed: {sql}: {e}"));
}

/// Helper: run DDL, expect error.
async fn ddl_err(state: &SharedState, identity: &AuthenticatedIdentity, sql: &str) -> String {
    let result = ddl::dispatch(state, identity, sql).await;
    assert!(result.is_some(), "DDL not recognized: {sql}");
    let err = result.unwrap().unwrap_err();
    err.to_string()
}

// ── User management ─────────────────────────────────────────────────

#[tokio::test]
async fn create_user() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER alice WITH PASSWORD 'secret123' ROLE readwrite TENANT 1",
    )
    .await;

    let user = state.credentials.get_user("alice").unwrap();
    assert_eq!(user.tenant_id, TenantId::new(1));
    assert!(user.roles.contains(&Role::ReadWrite));
    assert!(!user.is_superuser);
}

#[tokio::test]
async fn create_user_duplicate_rejected() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER bob WITH PASSWORD 'pass'").await;

    let err = ddl_err(&state, &su, "CREATE USER bob WITH PASSWORD 'pass2'").await;
    assert!(
        err.contains("already exists"),
        "expected duplicate error: {err}"
    );
}

#[tokio::test]
async fn create_user_default_role_and_tenant() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER carol WITH PASSWORD 'pass'").await;

    let user = state.credentials.get_user("carol").unwrap();
    // Default role is readwrite, tenant inherited from identity (0 for superuser).
    assert!(user.roles.contains(&Role::ReadWrite));
}

#[tokio::test]
async fn drop_user() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER dave WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "DROP USER dave").await;

    assert!(state.credentials.get_user("dave").is_none());
}

#[tokio::test]
async fn drop_self_rejected() {
    let state = make_state();
    let su = superuser();
    let err = ddl_err(&state, &su, "DROP USER admin").await;
    assert!(err.contains("cannot drop your own"), "{err}");
}

#[tokio::test]
async fn drop_nonexistent_user() {
    let state = make_state();
    let su = superuser();
    let err = ddl_err(&state, &su, "DROP USER nobody").await;
    assert!(err.contains("does not exist"), "{err}");
}

#[tokio::test]
async fn alter_user_password() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER eve WITH PASSWORD 'old'").await;
    ddl_ok(&state, &su, "ALTER USER eve SET PASSWORD 'new'").await;

    // Verify new password works.
    assert!(state.credentials.verify_password("eve", "new"));
    assert!(!state.credentials.verify_password("eve", "old"));
}

#[tokio::test]
async fn alter_user_role() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER frank WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;
    ddl_ok(&state, &su, "ALTER USER frank SET ROLE readwrite").await;

    let user = state.credentials.get_user("frank").unwrap();
    assert!(user.roles.contains(&Role::ReadWrite));
}

// ── Grant / Revoke ──────────────────────────────────────────────────

#[tokio::test]
async fn grant_role() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER grace WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;
    ddl_ok(&state, &su, "GRANT ROLE readwrite TO grace").await;

    let user = state.credentials.get_user("grace").unwrap();
    assert!(user.roles.contains(&Role::ReadOnly));
    assert!(user.roles.contains(&Role::ReadWrite));
}

#[tokio::test]
async fn revoke_role() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER heidi WITH PASSWORD 'pass' ROLE readwrite",
    )
    .await;
    ddl_ok(&state, &su, "REVOKE ROLE readwrite FROM heidi").await;

    let user = state.credentials.get_user("heidi").unwrap();
    assert!(!user.roles.contains(&Role::ReadWrite));
}

#[tokio::test]
async fn grant_superuser_requires_superuser() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER ivan WITH PASSWORD 'pass'").await;

    // Tenant admin cannot grant superuser.
    let admin = AuthenticatedIdentity {
        user_id: 50,
        username: "ta".into(),
        tenant_id: TenantId::new(1),
        auth_method: AuthMethod::Trust,
        roles: vec![Role::TenantAdmin],
        is_superuser: false,
    };
    let err = ddl_err(&state, &admin, "GRANT ROLE superuser TO ivan").await;
    assert!(err.contains("only superuser"), "{err}");
}

#[tokio::test]
async fn revoke_own_superuser_rejected() {
    let state = make_state();
    let su = superuser();
    let err = ddl_err(&state, &su, "REVOKE ROLE superuser FROM admin").await;
    // Even though the user doesn't exist in credential store, the self-check
    // should fire first if the username matches.
    assert!(err.contains("cannot revoke your own superuser"), "{err}");
}

// ── Tenant management ───────────────────────────────────────────────

#[tokio::test]
async fn create_tenant() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;

    // Verify audit event was recorded.
    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::TenantCreated);
    assert!(!events.is_empty());
    assert!(events.last().unwrap().detail.contains("acme"));
}

#[tokio::test]
async fn drop_system_tenant_rejected() {
    let state = make_state();
    let su = superuser();
    let err = ddl_err(&state, &su, "DROP TENANT 0").await;
    assert!(err.contains("cannot drop system tenant"), "{err}");
}

#[tokio::test]
async fn tenant_ops_require_superuser() {
    let state = make_state();
    let viewer = readonly_user();
    let err = ddl_err(&state, &viewer, "CREATE TENANT evil").await;
    assert!(err.contains("permission denied"), "{err}");
}

// ── SHOW commands ───────────────────────────────────────────────────

#[tokio::test]
async fn show_session() {
    let state = make_state();
    let su = superuser();
    let result = ddl::dispatch(&state, &su, "SHOW SESSION")
        .await
        .unwrap()
        .unwrap();

    // Should return one row with username = "admin".
    match &result[0] {
        pgwire::api::results::Response::Query(_) => {} // Correct type.
        other => panic!("expected Query response, got: {other:?}"),
    }
}

#[tokio::test]
async fn show_grants() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER judy WITH PASSWORD 'pass' ROLE readwrite",
    )
    .await;
    ddl_ok(&state, &su, "GRANT ROLE monitor TO judy").await;

    let result = ddl::dispatch(&state, &su, "SHOW GRANTS FOR judy")
        .await
        .unwrap()
        .unwrap();
    match &result[0] {
        pgwire::api::results::Response::Query(_) => {}
        other => panic!("expected Query response, got: {other:?}"),
    }
}

// ── Permission enforcement ──────────────────────────────────────────

#[tokio::test]
async fn readonly_cannot_create_user() {
    let state = make_state();
    let viewer = readonly_user();
    let err = ddl_err(&state, &viewer, "CREATE USER hacker WITH PASSWORD 'x'").await;
    assert!(err.contains("permission denied"), "{err}");
}

#[tokio::test]
async fn readonly_cannot_drop_user() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER target WITH PASSWORD 'pass'").await;

    let viewer = readonly_user();
    let err = ddl_err(&state, &viewer, "DROP USER target").await;
    assert!(err.contains("permission denied"), "{err}");
}

#[tokio::test]
async fn readonly_cannot_grant() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER target WITH PASSWORD 'pass'").await;

    let viewer = readonly_user();
    let err = ddl_err(&state, &viewer, "GRANT ROLE superuser TO target").await;
    assert!(err.contains("permission denied"), "{err}");
}

#[tokio::test]
async fn show_tenants_requires_superuser() {
    let state = make_state();
    let viewer = readonly_user();
    let err = ddl_err(&state, &viewer, "SHOW TENANTS").await;
    assert!(err.contains("permission denied"), "{err}");
}

// ── Audit log ───────────────────────────────────────────────────────

#[tokio::test]
async fn audit_records_create_and_drop() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER audit_test WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "DROP USER audit_test").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::PrivilegeChange);
    assert!(
        events.len() >= 2,
        "expected at least 2 PrivilegeChange events, got {}",
        events.len()
    );
    assert!(events.iter().any(|e| e.detail.contains("created")));
    assert!(events.iter().any(|e| e.detail.contains("dropped")));
}

#[tokio::test]
async fn audit_records_grant_revoke() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER karl WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;
    ddl_ok(&state, &su, "GRANT ROLE readwrite TO karl").await;
    ddl_ok(&state, &su, "REVOKE ROLE readonly FROM karl").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::PrivilegeChange);
    assert!(events.iter().any(|e| e.detail.contains("granted")));
    assert!(events.iter().any(|e| e.detail.contains("revoked")));
}

// ── Audit persistence ───────────────────────────────────────────────

#[test]
fn audit_flush_persists_to_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
    let catalog_path = dir.path().join("system.redb");

    let auth_config = nodedb::config::auth::AuthConfig::default();
    let (dispatcher, _sides) = Dispatcher::new(1, 64);
    let state = SharedState::open(
        dispatcher,
        wal,
        &catalog_path,
        &auth_config,
        nodedb_types::config::TuningConfig::default(),
    )
    .unwrap();

    // Record some audit events.
    state.audit_record(AuditEvent::AuthSuccess, None, "test", "user logged in");
    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(TenantId::new(1)),
        "test",
        "granted role",
    );

    // Flush to catalog.
    state.flush_audit_log();

    // Verify entries persisted.
    let catalog = state.credentials.catalog().as_ref().unwrap();
    let count = catalog.audit_entry_count().unwrap();
    assert_eq!(count, 2, "expected 2 persisted audit entries");

    // Verify sequence counter.
    let max_seq = catalog.load_audit_max_seq().unwrap();
    assert!(max_seq >= 2);
}

#[test]
fn audit_sequence_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let catalog_path = dir.path().join("system.redb");
    let auth_config = nodedb::config::auth::AuthConfig::default();

    // First run: record and flush.
    {
        let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::open(
            dispatcher,
            wal,
            &catalog_path,
            &auth_config,
            nodedb_types::config::TuningConfig::default(),
        )
        .unwrap();

        state.audit_record(AuditEvent::AuthSuccess, None, "src", "event1");
        state.audit_record(AuditEvent::AuthSuccess, None, "src", "event2");
        state.flush_audit_log();
    }

    // Second run: sequence should continue from where it left off.
    {
        let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::open(
            dispatcher,
            wal,
            &catalog_path,
            &auth_config,
            nodedb_types::config::TuningConfig::default(),
        )
        .unwrap();

        state.audit_record(AuditEvent::AdminAction, None, "src", "event3");
        state.flush_audit_log();

        let catalog = state.credentials.catalog().as_ref().unwrap();
        let count = catalog.audit_entry_count().unwrap();
        assert_eq!(
            count, 3,
            "expected 3 total persisted audit entries across restarts"
        );

        let max_seq = catalog.load_audit_max_seq().unwrap();
        assert!(max_seq >= 3, "sequence should be >= 3, got {max_seq}");
    }
}

// ── Connection-level test (only one, uses TCP) ──────────────────────

#[tokio::test]
async fn pgwire_ddl_roundtrip() {
    let state = make_state();

    let pg_listener =
        nodedb::control::server::pgwire::listener::PgListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let port = pg_listener.local_addr().port();

    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&state.shutdown));
    let shared_pg = Arc::clone(&state);
    let test_startup_gate = Arc::clone(&state.startup);
    let bus_pg = shutdown_bus.clone();
    tokio::spawn(async move {
        pg_listener
            .run(
                shared_pg,
                nodedb::config::auth::AuthMode::Trust,
                None,
                Arc::new(tokio::sync::Semaphore::new(128)),
                test_startup_gate,
                bus_pg,
            )
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let conn_str = format!("host=127.0.0.1 port={port} user=admin dbname=nodedb");
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // CREATE USER over the wire.
    client
        .simple_query("CREATE USER wire_test WITH PASSWORD 'pass'")
        .await
        .unwrap();

    // SHOW SESSION over the wire.
    let msgs = client.simple_query("SHOW SESSION").await.unwrap();
    let username = msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
        _ => None,
    });
    assert_eq!(username, Some("admin".to_string()));

    // Verify user was created.
    assert!(state.credentials.get_user("wire_test").is_some());
}
