// SPDX-License-Identifier: BUSL-1.1

//! Privilege-matrix gating tests for admin-level database DDL.
//!
//! For each operation in the role matrix, two tests verify:
//! 1. Positive: the allowed identity succeeds (or gets 0A000 for
//!    not-yet-implemented, never 42501).
//! 2. Negative: the forbidden identity is denied with SQLSTATE 42501 and
//!    a `PermissionDenied` audit event is emitted.

use nodedb::control::security::audit::AuditEvent;
use nodedb_test_support::pgwire_auth_helpers::{
    assert_audit_has, cluster_admin_user, database_owner_user, ddl_err, ddl_ok,
    make_state_with_catalog, readonly_user, superuser, try_ddl,
};
use nodedb_types::id::DatabaseId;

// Pre-create a database named `foo` and return its `DatabaseId`.
async fn setup_foo_db(state: &nodedb::control::state::SharedState) -> DatabaseId {
    let su = superuser();
    ddl_ok(state, &su, "CREATE DATABASE foo").await;
    state
        .credentials
        .catalog()
        .get_database_id_by_name("foo")
        .unwrap()
        .expect("foo db must exist after CREATE DATABASE")
}

// ─── CREATE DATABASE ──────────────────────────────────────────────────────────

#[tokio::test]
async fn create_database_cluster_admin_allowed() {
    let state = make_state_with_catalog();
    let ca = cluster_admin_user();
    ddl_ok(&state, &ca, "CREATE DATABASE testcreate1").await;
}

#[tokio::test]
async fn create_database_readonly_denied() {
    let state = make_state_with_catalog();
    let ro = readonly_user();
    let err = ddl_err(&state, &ro, "CREATE DATABASE testcreate2").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, None);
}

// ─── DROP DATABASE ────────────────────────────────────────────────────────────

#[tokio::test]
async fn drop_database_superuser_allowed() {
    let state = make_state_with_catalog();
    setup_foo_db(&state).await;
    let su = superuser();
    ddl_ok(&state, &su, "DROP DATABASE foo").await;
}

#[tokio::test]
async fn drop_database_cluster_admin_denied() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let ca = cluster_admin_user();
    let err = ddl_err(&state, &ca, "DROP DATABASE foo").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

// ─── DROP DATABASE FORCE ─────────────────────────────────────────────────────

#[tokio::test]
async fn drop_database_force_superuser_allowed() {
    let state = make_state_with_catalog();
    setup_foo_db(&state).await;
    let su = superuser();
    ddl_ok(&state, &su, "DROP DATABASE foo FORCE").await;
}

#[tokio::test]
async fn drop_database_force_cluster_admin_denied() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let ca = cluster_admin_user();
    let err = ddl_err(&state, &ca, "DROP DATABASE foo FORCE").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

// ─── ALTER DATABASE … RENAME ──────────────────────────────────────────────────

#[tokio::test]
async fn alter_database_rename_owner_allowed() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let owner = database_owner_user(foo_id);
    ddl_ok(&state, &owner, "ALTER DATABASE foo RENAME TO foorename").await;
}

#[tokio::test]
async fn alter_database_rename_cluster_admin_denied() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let ca = cluster_admin_user();
    let err = ddl_err(&state, &ca, "ALTER DATABASE foo RENAME TO foorenamed").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

// ─── ALTER DATABASE … SET QUOTA ───────────────────────────────────────────────

#[tokio::test]
async fn alter_database_set_quota_cluster_admin_allowed() {
    let state = make_state_with_catalog();
    setup_foo_db(&state).await;
    let ca = cluster_admin_user();
    ddl_ok(
        &state,
        &ca,
        "ALTER DATABASE foo SET QUOTA (max_memory_bytes = 1073741824)",
    )
    .await;
}

#[tokio::test]
async fn alter_database_set_quota_db_owner_denied() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let owner = database_owner_user(foo_id);
    let err = ddl_err(
        &state,
        &owner,
        "ALTER DATABASE foo SET QUOTA (max_memory_bytes = 1073741824)",
    )
    .await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

// ─── ALTER DATABASE … SET AUDIT_DML ──────────────────────────────────────────

#[tokio::test]
async fn alter_database_set_audit_dml_cluster_admin_allowed() {
    let state = make_state_with_catalog();
    setup_foo_db(&state).await;
    let ca = cluster_admin_user();
    ddl_ok(&state, &ca, "ALTER DATABASE foo SET AUDIT_DML = 'writes'").await;
}

#[tokio::test]
async fn alter_database_set_audit_dml_db_owner_denied() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let owner = database_owner_user(foo_id);
    let err = ddl_err(
        &state,
        &owner,
        "ALTER DATABASE foo SET AUDIT_DML = 'writes'",
    )
    .await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

// ─── ALTER DATABASE … MATERIALIZE ────────────────────────────────────────────

#[tokio::test]
async fn alter_database_materialize_owner_allowed() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let owner = database_owner_user(foo_id);
    // A non-clone database materializes as a no-op (no collections to materialize).
    ddl_ok(&state, &owner, "ALTER DATABASE foo MATERIALIZE").await;
}

#[tokio::test]
async fn alter_database_materialize_readonly_denied() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let ro = readonly_user();
    let err = ddl_err(&state, &ro, "ALTER DATABASE foo MATERIALIZE").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

// ─── ALTER DATABASE … PROMOTE ────────────────────────────────────────────────

#[tokio::test]
async fn alter_database_promote_superuser_allowed() {
    let state = make_state_with_catalog();
    setup_foo_db(&state).await;
    let su = superuser();
    // foo never was a mirror. The superuser passes the gate, and the statement
    // refuses with 0A000.
    let err = ddl_err(&state, &su, "ALTER DATABASE foo PROMOTE").await;
    assert!(
        err.contains("0A000") && err.contains("is not a mirror database"),
        "superuser must pass the gate and get the typed refusal; got: {err}"
    );
}

#[tokio::test]
async fn alter_database_promote_cluster_admin_denied() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let ca = cluster_admin_user();
    let err = ddl_err(&state, &ca, "ALTER DATABASE foo PROMOTE").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

// ─── CLONE DATABASE ───────────────────────────────────────────────────────────

#[tokio::test]
async fn clone_database_superuser_allowed() {
    let state = make_state_with_catalog();
    setup_foo_db(&state).await;
    let su = superuser();
    ddl_ok(&state, &su, "CLONE DATABASE bar FROM foo").await;
}

#[tokio::test]
async fn clone_database_cluster_admin_denied() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let ca = cluster_admin_user();
    let err = ddl_err(&state, &ca, "CLONE DATABASE barclone FROM foo").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

// ─── MIRROR DATABASE ─────────────────────────────────────────────────────────

#[tokio::test]
async fn mirror_database_superuser_allowed() {
    let state = make_state_with_catalog();
    let su = superuser();
    // The local mirror descriptor is created before the cross-cluster link
    // attempt; the call may return Ok (descriptor inserted, link pending)
    // or a non-42501 error if the source cluster cannot be reached. Either
    // way the gate passed.
    let result = try_ddl(
        &state,
        &su,
        "MIRROR DATABASE mymirror FROM cluster1.sourcedb",
    )
    .await;
    if let Err(err) = result {
        assert!(
            !err.contains("42501"),
            "superuser must pass the gate; got: {err}"
        );
    }
}

#[tokio::test]
async fn mirror_database_cluster_admin_denied() {
    let state = make_state_with_catalog();
    let ca = cluster_admin_user();
    let err = ddl_err(
        &state,
        &ca,
        "MIRROR DATABASE mymirror2 FROM cluster1.sourcedb",
    )
    .await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, None);
}

// ─── MOVE TENANT ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn move_tenant_superuser_allowed() {
    let state = make_state_with_catalog();
    let su = superuser();
    // No tenant named 't' exists; we expect 42P01 (not found), not 42501.
    // The gate was passed; tenant lookup failed.
    let err = ddl_err(&state, &su, "MOVE TENANT t FROM foo TO bar").await;
    assert!(
        !err.contains("42501"),
        "superuser must pass the gate; got: {err}"
    );
}

#[tokio::test]
async fn move_tenant_cluster_admin_denied() {
    let state = make_state_with_catalog();
    setup_foo_db(&state).await;
    let ca = cluster_admin_user();
    let err = ddl_err(&state, &ca, "MOVE TENANT t FROM foo TO bar").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    // db_id for source 'foo' was resolved before the gate.
    let foo_id = state
        .credentials
        .catalog()
        .get_database_id_by_name("foo")
        .unwrap()
        .expect("foo must still exist");
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

// ─── BACKUP DATABASE ─────────────────────────────────────────────────────────

#[tokio::test]
async fn backup_database_owner_passes_the_gate() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let owner = database_owner_user(foo_id);
    let err = ddl_err(&state, &owner, "BACKUP DATABASE foo TO 's3://x'").await;
    // The gate passed; the bucket-only URI is refused next.
    assert!(
        !err.contains("42501"),
        "database_owner must pass the gate; got: {err}"
    );
    assert!(
        err.contains("22023"),
        "expected 22023 for a URI with no object key; got: {err}"
    );
}

#[tokio::test]
async fn backup_database_readonly_denied() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let ro = readonly_user();
    let err = ddl_err(&state, &ro, "BACKUP DATABASE foo TO 's3://x'").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

// ─── RESTORE DATABASE ────────────────────────────────────────────────────────

#[tokio::test]
async fn restore_database_superuser_passes_the_gate() {
    let state = make_state_with_catalog();
    let su = superuser();
    let err = ddl_err(&state, &su, "RESTORE DATABASE foo FROM 's3://x'").await;
    // Superuser passes the gate; the bucket-only URI is refused next.
    assert!(
        !err.contains("42501"),
        "superuser must pass the gate; got: {err}"
    );
    assert!(
        err.contains("22023"),
        "expected 22023 for a URI with no object key; got: {err}"
    );
}

#[tokio::test]
async fn backup_database_cluster_admin_denied() {
    let state = make_state_with_catalog();
    let foo_id = setup_foo_db(&state).await;
    let ca = cluster_admin_user();
    let err = ddl_err(&state, &ca, "BACKUP DATABASE foo TO 's3://x/y'").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "BACKUP DATABASE needs superuser or the database owner, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, Some(foo_id));
}

#[tokio::test]
async fn restore_database_cluster_admin_denied() {
    let state = make_state_with_catalog();
    let ca = cluster_admin_user();
    let err = ddl_err(&state, &ca, "RESTORE DATABASE foo FROM 's3://x'").await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected 42501/permission denied, got: {err}"
    );
    assert_audit_has(&state, AuditEvent::PermissionDenied, None);
}
