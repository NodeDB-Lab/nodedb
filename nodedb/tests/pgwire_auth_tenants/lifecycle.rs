// SPDX-License-Identifier: BUSL-1.1

//! CREATE / DROP TENANT lifecycle, plus the `IF NOT EXISTS` /
//! `IF EXISTS` / `WITH ADMIN` clause guards.

use crate::common::pgwire_auth_helpers::{
    assert_readonly_denied, ddl_err, ddl_ok, make_state, make_state_with_catalog, superuser,
};
use nodedb::control::security::audit::AuditEvent;

#[tokio::test]
async fn create_tenant() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::TenantCreated);
    assert!(!events.is_empty());
    assert!(events.last().unwrap().detail.contains("acme"));
}

/// Two `CREATE TENANT`s without an explicit `ID` must receive distinct
/// ids. The pre-fix allocator derived the id from a lazily-populated
/// traffic counter, so with no traffic every auto-allocated tenant got
/// id 1 and the second create silently overwrote the first.
#[tokio::test]
async fn create_tenant_without_id_allocates_distinct_ids() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT alpha").await;
    ddl_ok(&state, &su, "CREATE TENANT beta").await;

    let catalog = state.credentials.catalog();
    let a = catalog
        .find_tenant_by_name("alpha")
        .unwrap()
        .expect("tenant alpha must exist");
    let b = catalog
        .find_tenant_by_name("beta")
        .unwrap()
        .expect("tenant beta must survive (not be overwritten by alpha's slot)");
    assert_ne!(
        a.tenant_id, b.tenant_id,
        "two auto-allocated tenants must not share an id"
    );
    // Reserved slots 0 (system) and 1 (bootstrap) are never handed out.
    assert!(a.tenant_id >= 2 && b.tenant_id >= 2, "{a:?} {b:?}");
}

/// An auto-allocated id is never reused after the tenant is dropped:
/// the durable high-water-mark only moves forward, so a dropped id
/// cannot be silently reassigned to a different tenant.
#[tokio::test]
async fn dropped_tenant_id_is_not_reused() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT gamma").await;
    let catalog = state.credentials.catalog();
    let dropped_id = catalog
        .find_tenant_by_name("gamma")
        .unwrap()
        .expect("gamma must exist")
        .tenant_id;

    ddl_ok(&state, &su, &format!("DROP TENANT {dropped_id}")).await;
    ddl_ok(&state, &su, "CREATE TENANT delta").await;

    let reused = state
        .credentials
        .catalog()
        .find_tenant_by_name("delta")
        .unwrap()
        .expect("delta must exist")
        .tenant_id;
    assert_ne!(
        reused, dropped_id,
        "a fresh tenant must not reuse a dropped tenant's id"
    );
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
    assert_readonly_denied(&state, "CREATE TENANT evil").await;
}

#[tokio::test]
async fn show_tenants_requires_superuser() {
    let state = make_state();
    assert_readonly_denied(&state, "SHOW TENANTS").await;
}

// ── IF NOT EXISTS on CREATE TENANT ───────────────────────────────────
//
// `CREATE TENANT IF NOT EXISTS <name>` is the standard PostgreSQL idiom.
// The handler must recognize the `IF NOT EXISTS` clause and name the
// tenant `<name>` — not consume the clause keywords as the tenant name.

/// `CREATE TENANT IF NOT EXISTS <name>` creates a tenant named `<name>`,
/// not one named after the `IF` keyword.
#[tokio::test]
async fn create_tenant_if_not_exists_names_real_tenant() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT IF NOT EXISTS acme").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::TenantCreated);
    let detail = &events.last().expect("tenant created").detail;
    assert!(detail.contains("'acme'"), "{detail}");
    // Regression guard: the `IF NOT EXISTS` keywords must never leak
    // into the tenant name.
    assert!(
        !detail.contains("'IF'"),
        "clause keyword used as name: {detail}"
    );
}

/// The auto-created tenant admin is named after the real tenant
/// (`acme_admin`), not after a consumed clause keyword (`IF_admin`).
#[tokio::test]
async fn create_tenant_if_not_exists_admin_uses_tenant_name() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT IF NOT EXISTS acme").await;

    let tenant = state
        .credentials
        .catalog()
        .find_tenant_by_name("acme")
        .unwrap()
        .expect("persisted tenant");
    assert_eq!(tenant.admin_username, "acme_admin");

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::TenantCreated);
    let detail = &events.last().expect("tenant created").detail;
    assert!(detail.contains("acme_admin"), "{detail}");
    assert!(
        !detail.contains("IF_admin"),
        "phantom admin named after clause keyword: {detail}"
    );
}

/// `CREATE TENANT IF NOT EXISTS <name> ID <id>` honors both the
/// `IF NOT EXISTS` clause and the trailing explicit `ID`.
#[tokio::test]
async fn create_tenant_if_not_exists_with_explicit_id() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT IF NOT EXISTS acme ID 7").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::TenantCreated);
    let detail = &events.last().expect("tenant created").detail;
    assert!(detail.contains("'acme'"), "{detail}");
    assert!(
        detail.contains("tenant:7"),
        "explicit ID not honored: {detail}"
    );
}

/// A second `CREATE TENANT IF NOT EXISTS <name>` for an existing tenant
/// is a no-op success — it does not create a second, differently named
/// tenant.
#[tokio::test]
async fn create_tenant_if_not_exists_is_idempotent() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme").await;
    ddl_ok(&state, &su, "CREATE TENANT IF NOT EXISTS acme").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::TenantCreated);
    assert_eq!(
        events.len(),
        1,
        "IF NOT EXISTS re-create must be a no-op, got: {:?}",
        events.iter().map(|e| &e.detail).collect::<Vec<_>>()
    );
}

// ── WITH ADMIN clause on CREATE TENANT ───────────────────────────────
//
// `CREATE TENANT <name> WITH ADMIN <user>` must name the auto-created
// tenant admin after `<user>` — not silently ignore the clause and
// derive `<name>_admin`.

/// `CREATE TENANT <name> WITH ADMIN <user>` names the tenant admin
/// `<user>`, honoring the explicit clause.
#[tokio::test]
async fn create_tenant_with_admin_uses_named_admin() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme WITH ADMIN bootstrap_admin").await;

    let tenant = state
        .credentials
        .catalog()
        .find_tenant_by_name("acme")
        .unwrap()
        .expect("persisted tenant");
    assert_eq!(tenant.admin_username, "bootstrap_admin");

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::TenantCreated);
    let detail = &events.last().expect("tenant created").detail;
    assert!(detail.contains("'acme'"), "{detail}");
    assert!(
        detail.contains("with admin 'bootstrap_admin'"),
        "WITH ADMIN clause ignored: {detail}"
    );
}

#[tokio::test]
async fn create_tenant_rejects_an_existing_admin_identity_atomically() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER bootstrap_admin PASSWORD 'existing-secret'",
    )
    .await;

    let err = ddl_err(&state, &su, "CREATE TENANT acme WITH ADMIN bootstrap_admin").await;

    assert!(err.contains("already exists"), "{err}");
    assert!(
        state
            .credentials
            .catalog()
            .find_tenant_by_name("acme")
            .unwrap()
            .is_none(),
        "failed administrator provisioning must not leave a tenant row"
    );
}

#[tokio::test]
async fn authoritative_tenant_admin_cannot_be_dropped_independently() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme WITH ADMIN bootstrap_admin").await;

    let err = ddl_err(&state, &su, "DROP USER bootstrap_admin").await;

    assert!(err.contains("authoritative tenant administrator"), "{err}");
    assert!(state.credentials.get_user("bootstrap_admin").is_some());
}

#[tokio::test]
async fn legacy_tenant_admin_cannot_be_dropped_independently() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT legacy").await;
    let catalog = state.credentials.catalog();
    let mut tenant = catalog
        .find_tenant_by_name("legacy")
        .unwrap()
        .expect("persisted tenant");
    tenant.admin_username.clear();
    catalog.put_tenant(&tenant).unwrap();

    let err = ddl_err(&state, &su, "DROP USER legacy_admin").await;

    assert!(err.contains("authoritative tenant administrator"), "{err}");
    assert!(state.credentials.get_user("legacy_admin").is_some());
}

#[tokio::test]
async fn drop_tenant_removes_its_explicit_lifecycle_admin() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme WITH ADMIN bootstrap_admin").await;

    ddl_ok(&state, &su, "DROP TENANT acme").await;

    assert!(state.credentials.get_user("bootstrap_admin").is_none());
    assert!(
        state
            .credentials
            .catalog()
            .find_tenant_by_name("acme")
            .unwrap()
            .is_none()
    );
}

// ── IF EXISTS on DROP TENANT ─────────────────────────────────────────
//
// `DROP TENANT IF EXISTS <id>` must recognize the `IF EXISTS` clause:
// dropping a missing tenant is a no-op success, and the clause keywords
// must not be parsed in place of the tenant id.

/// `DROP TENANT IF EXISTS <id>` on a tenant that does not exist is a
/// no-op success, not an error.
#[tokio::test]
async fn drop_tenant_if_exists_missing_is_noop() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "DROP TENANT IF EXISTS 999").await;
}

/// `DROP TENANT IF EXISTS <id>` on an existing tenant actually drops it —
/// the `IF EXISTS` clause must not turn the statement into a total no-op.
#[tokio::test]
async fn drop_tenant_if_exists_existing_drops() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 5").await;
    ddl_ok(&state, &su, "DROP TENANT IF EXISTS 5").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::TenantDeleted);
    let detail = &events.last().expect("tenant dropped").detail;
    assert!(detail.contains("tenant:5"), "{detail}");
}
