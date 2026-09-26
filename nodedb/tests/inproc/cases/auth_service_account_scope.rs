// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for service-account database scoping (section G).

use nodedb_test_support::pgwire_auth_helpers::{
    ddl_err, ddl_ok, make_state, make_state_with_catalog, readonly_user, superuser,
};
use nodedb_types::id::DatabaseId;

/// CREATE SERVICE ACCOUNT without FOR DATABASE clause creates legacy account.
#[tokio::test]
async fn create_service_account_legacy_no_database() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE SERVICE ACCOUNT svc_legacy ROLE readwrite TENANT 1",
    )
    .await;

    let user = state.credentials.get_user("svc_legacy").unwrap();
    assert!(user.is_service_account);
    // Legacy: no explicit databases set.
    assert!(user.accessible_databases.is_empty());
}

/// CREATE SERVICE ACCOUNT with FOR DATABASE sets accessible_databases.
#[tokio::test]
async fn create_service_account_for_database() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(
        &state,
        &su,
        "CREATE SERVICE ACCOUNT svc_db FOR DATABASE default",
    )
    .await;

    let user = state.credentials.get_user("svc_db").unwrap();
    assert!(user.is_service_account);
    assert_eq!(user.accessible_databases, vec![DatabaseId::DEFAULT]);
}

/// ALTER SERVICE ACCOUNT SET DATABASES — superuser only.
#[tokio::test]
async fn alter_service_account_set_databases_requires_superuser() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE SERVICE ACCOUNT svc_alter ROLE readwrite TENANT 1",
    )
    .await;

    let viewer = readonly_user();
    let err = ddl_err(
        &state,
        &viewer,
        "ALTER SERVICE ACCOUNT svc_alter SET DATABASES default",
    )
    .await;
    assert!(
        err.contains("permission denied") || err.contains("superuser"),
        "expected permission denied for non-superuser: {err}"
    );
}

/// Non-superuser cannot use FOR TENANT ... IN DATABASE.
#[tokio::test]
async fn create_service_account_for_tenant_in_database_requires_superuser() {
    let state = make_state();
    state
        .credentials
        .catalog()
        .bootstrap_default_database()
        .unwrap();

    // Non-superuser TenantAdmin attempting cross-tenant create.
    let viewer = readonly_user();
    let err = ddl_err(
        &state,
        &viewer,
        "CREATE SERVICE ACCOUNT cross_svc FOR TENANT 2 IN DATABASE default",
    )
    .await;
    assert!(
        err.contains("permission denied") || err.contains("superuser") || err.contains("admin"),
        "expected permission denied: {err}"
    );
}

/// ALTER SERVICE ACCOUNT SET DATABASES with unknown DB rejected with 42704.
#[tokio::test]
async fn alter_service_account_set_databases_unknown_db_rejected() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE SERVICE ACCOUNT svc_baddb ROLE readwrite TENANT 1",
    )
    .await;

    let err = ddl_err(
        &state,
        &su,
        "ALTER SERVICE ACCOUNT svc_baddb SET DATABASES ghost_db",
    )
    .await;
    assert!(
        err.contains("not found") || err.contains("42704"),
        "expected 42704 for unknown db: {err}"
    );
}

/// `ALTER SERVICE ACCOUNT ... SET DATABASES` replaces the account's database
/// scope through the replicated user entry: the new database is in scope and
/// the old one is not.
#[tokio::test]
async fn set_service_account_databases_replaces_list() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE DATABASE svc_scope_b").await;
    let db_b = state
        .credentials
        .catalog()
        .get_database_id_by_name("svc_scope_b")
        .unwrap()
        .expect("svc_scope_b exists");
    ddl_ok(
        &state,
        &su,
        "CREATE SERVICE ACCOUNT svc_replace FOR DATABASE default",
    )
    .await;
    let before = state.credentials.get_user("svc_replace").unwrap();
    assert_eq!(before.accessible_databases, vec![DatabaseId::DEFAULT]);

    ddl_ok(
        &state,
        &su,
        "ALTER SERVICE ACCOUNT svc_replace SET DATABASES svc_scope_b",
    )
    .await;

    let user = state.credentials.get_user("svc_replace").unwrap();
    assert_eq!(user.accessible_databases, vec![db_b]);
    assert!(
        !user.accessible_databases.contains(&DatabaseId::DEFAULT),
        "the replaced database must leave the account's scope"
    );
    let stored = state
        .credentials
        .catalog()
        .get_user("svc_replace")
        .unwrap()
        .expect("the catalog holds the account");
    assert_eq!(
        stored.accessible_databases,
        vec![db_b.as_u64()],
        "the catalog must hold the replaced scope"
    );
}

/// Service account + key inheritance: key picks up service account's databases.
#[tokio::test]
async fn service_account_key_inherits_accessible_databases() {
    use nodedb::control::security::identity::DatabaseSet;
    use nodedb::types::TenantId;
    use smallvec::smallvec;

    let db_a = DatabaseId::new(1);

    let state = make_state();
    state
        .credentials
        .create_service_account(
            "svc_inherit",
            TenantId::new(1),
            vec![nodedb::control::security::identity::Role::ReadWrite],
            vec![db_a],
        )
        .unwrap();

    let user = state.credentials.get_user("svc_inherit").unwrap();
    assert_eq!(user.accessible_databases, vec![db_a]);

    // DatabaseSet built from service account should be Some([db_a]).
    let expected = DatabaseSet::Some(smallvec![db_a]);
    let actual = DatabaseSet::Some(smallvec::SmallVec::from_iter(
        user.accessible_databases.iter().copied(),
    ));
    assert_eq!(actual, expected);
}

/// `CREATE SERVICE ACCOUNT IF NOT EXISTS <name>` creates an account named
/// `<name>`, not one named after the `IF` clause keyword.
#[tokio::test]
async fn create_service_account_if_not_exists_names_real_account() {
    use nodedb::control::security::audit::AuditEvent;

    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE SERVICE ACCOUNT IF NOT EXISTS reporter").await;

    let log = state.audit.lock().unwrap();
    let details: Vec<&String> = log
        .query_by_event(&AuditEvent::PrivilegeChange)
        .iter()
        .map(|e| &e.detail)
        .collect();
    assert!(
        details
            .iter()
            .any(|d| d.contains("service account 'reporter'")),
        "{details:?}"
    );
    // Regression guard: the `IF NOT EXISTS` keywords must never leak
    // into the service account name.
    assert!(
        !details.iter().any(|d| d.contains("service account 'IF'")),
        "clause keyword used as service account name: {details:?}"
    );
}

/// `DROP SERVICE ACCOUNT IF EXISTS <name>` on an account that does not
/// exist is a no-op success, not an error.
#[tokio::test]
async fn drop_service_account_if_exists_missing_is_noop() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "DROP SERVICE ACCOUNT IF EXISTS ghost").await;
}

/// `DROP SERVICE ACCOUNT IF EXISTS <name>` on an existing account
/// actually drops it — the `IF EXISTS` clause must not turn the
/// statement into a total no-op.
#[tokio::test]
async fn drop_service_account_if_exists_existing_drops() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE SERVICE ACCOUNT reporter").await;
    ddl_ok(&state, &su, "DROP SERVICE ACCOUNT IF EXISTS reporter").await;

    assert!(
        state.credentials.get_user("reporter").is_none(),
        "DROP SERVICE ACCOUNT IF EXISTS must drop an existing account"
    );
}
