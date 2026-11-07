// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for per-database idle session timeout and KILL SESSION.

use nodedb::control::security::sessions::idle_sweep::next_close_deadline_ms;
use nodedb::control::security::sessions::{KillReason, SessionParams, SessionRegistry};
use nodedb_test_support::pgwire_auth_helpers::{
    cluster_admin_user, ddl_err, ddl_ok, make_state_with_catalog, superuser,
};
use nodedb_types::id::DatabaseId;

// ── Pure-function deadline smoke tests (stable-signature check) ─────────────
// Truth table is exercised exhaustively in idle_sweep::tests; these check
// the public re-export path used by downstream code.
//
// Helper semantics: `next_close_deadline_ms(token_exp_ms, last_active_ms, cap_secs)`
// computes `idle_deadline = last_active_ms + cap_secs * 1000`, then returns
// the earlier of (token_exp, idle_deadline) treating zero as "no cap".

#[test]
fn next_close_deadline_token_earlier_than_idle_returns_token() {
    // last_active=1_000_000ms, cap=300s → idle_deadline = 1_300_000ms.
    // token_exp = 600_000ms (earlier) → token wins.
    let dl = next_close_deadline_ms(600_000, 1_000_000, 300);
    assert_eq!(dl, Some(600_000));
}

#[test]
fn next_close_deadline_idle_earlier_than_token_returns_idle() {
    // last_active=1_000_000ms, cap=300s → idle_deadline = 1_300_000ms.
    // token_exp = 9_999_000ms (later) → idle wins.
    let dl = next_close_deadline_ms(9_999_000, 1_000_000, 300);
    assert_eq!(dl, Some(1_300_000));
}

#[test]
fn next_close_deadline_no_caps_returns_none() {
    let dl = next_close_deadline_ms(0, 1_000_000, 0);
    assert!(dl.is_none());
}

// ── ALTER DATABASE SET IDLE_TIMEOUT persists to cache ───────────────────────

#[tokio::test]
async fn alter_database_set_idle_timeout_updates_cache() {
    let state = make_state_with_catalog();
    let ca = cluster_admin_user();
    ddl_ok(
        &state,
        &ca,
        "ALTER DATABASE default SET IDLE_TIMEOUT = 1800",
    )
    .await;

    // The live cache must immediately reflect the new value.
    assert_eq!(state.idle_timeout_cache.get(DatabaseId::DEFAULT), 1800);
}

#[tokio::test]
async fn alter_database_set_idle_timeout_persists_to_catalog() {
    let state = make_state_with_catalog();
    let ca = cluster_admin_user();
    ddl_ok(
        &state,
        &ca,
        "ALTER DATABASE default SET IDLE_TIMEOUT = 3600",
    )
    .await;

    // Read back via catalog.
    let cat = state.credentials.catalog();
    let desc = cat
        .get_database(DatabaseId::DEFAULT)
        .unwrap()
        .expect("default database must exist");
    assert_eq!(desc.idle_session_timeout_secs, 3600);
}

// ── KILL SESSION signals the kill_tx watch ───────────────────────────────────

#[tokio::test]
async fn kill_session_by_id_signals_kill_tx() {
    let state = make_state_with_catalog();

    let params = SessionParams {
        user_id: 42,
        username: "alice".into(),
        db_user: "alice".into(),
        peer_addr: "127.0.0.1:9999".into(),
        protocol: "native".into(),
        auth_method: "password".into(),
        tenant_id: 1,
        credential_version: 0,
        current_database: Some(DatabaseId::DEFAULT),
        token_expiry_ms: None,
        api_key_id: None,
    };
    let mut kill_rx = state
        .session_registry
        .register("s_test_kill", &params)
        .unwrap();

    // Verify the returned current_database matches.
    let killed_db = state
        .session_registry
        .kill_session_by_id("s_test_kill", KillReason::AdminKill);
    assert_eq!(killed_db, Some(DatabaseId::DEFAULT));

    // The watch fires.
    assert!(kill_rx.changed().await.is_ok());
    assert_eq!(*kill_rx.borrow(), KillReason::AdminKill);
}

// ── KILL SESSION on unknown id returns None ──────────────────────────────────

#[test]
fn kill_session_unknown_id_returns_none() {
    let reg = SessionRegistry::new();
    let result = reg.kill_session_by_id("does_not_exist", KillReason::AdminKill);
    assert!(result.is_none());
}

// ── KILL SESSION via DDL on unknown id returns 42704 ────────────────────────

#[tokio::test]
async fn kill_session_ddl_unknown_id_returns_not_found() {
    let state = make_state_with_catalog();
    let su = superuser();
    let err = ddl_err(&state, &su, "KILL SESSION 'does_not_exist'").await;
    assert!(
        err.contains("42704") || err.contains("not found") || err.contains("does not exist"),
        "expected not-found error, got: {err}"
    );
}

// ── SHOW SESSIONS routes successfully ────────────────────────────────────────

#[tokio::test]
async fn show_sessions_executes_successfully() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "SHOW SESSIONS").await;
}
