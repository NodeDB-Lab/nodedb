// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for session revocation (Section B) and in-flight
//! permission propagation (Section F).
//!
//! Unit-level registry tests (register/unregister, cap, hard-revoke signal)
//! live in `control/security/sessions/registry.rs` and
//! `control/security/buses/consumer.rs`.  These integration tests exercise
//! the cross-component interactions that require a real `CredentialStore`
//! wired to real buses.

use std::sync::Arc;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::security::audit::AuditEvent;
use nodedb::control::security::buses::session_invalidation::SESSION_INVALIDATION_CHANNEL_CAPACITY;
use nodedb::control::security::identity::{AuthMethod, Role};
use nodedb::control::security::sessions::{KillReason, SessionParams, SessionRegistry};
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;
use nodedb::wal::WalManager;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A bare state: these tests drive the credential store and its buses
/// directly, and serve no request.
fn make_state() -> Arc<SharedState> {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
    let (dispatcher, _) = Dispatcher::new(1, 64);
    SharedState::new(dispatcher, wal).unwrap()
}

fn sample_params(user_id: u64, username: &str) -> SessionParams {
    SessionParams {
        user_id,
        username: username.to_string(),
        db_user: username.to_string(),
        peer_addr: "127.0.0.1:5555".to_string(),
        protocol: "native".to_string(),
        auth_method: "password".to_string(),
        tenant_id: 1,
        credential_version: 0,
        current_database: None,
        token_expiry_ms: None,
    }
}

// ---------------------------------------------------------------------------
// Section B: session registry re-tested end-to-end via SharedState
// ---------------------------------------------------------------------------

#[test]
fn active_sessions_register_unregister() {
    let reg = SessionRegistry::new();
    let rx = reg.register("s1", &sample_params(42, "alice")).unwrap();
    assert!(!rx.has_changed().unwrap_or(false));
    assert_eq!(reg.count(None), 1);
    assert_eq!(reg.count(Some(42)), 1);
    reg.unregister("s1");
    assert_eq!(reg.count(None), 0);
}

#[test]
fn max_active_sessions_over_cap_rejects() {
    let reg = SessionRegistry::with_cap(2);
    reg.register("s1", &sample_params(1, "u1")).unwrap();
    reg.register("s2", &sample_params(2, "u2")).unwrap();
    let result = reg.register("s3", &sample_params(3, "u3"));
    assert!(result.is_err(), "over-cap registration must fail");
    assert_eq!(reg.count(None), 2);
}

#[test]
fn session_hard_revoke_close() {
    let reg = SessionRegistry::new();
    let mut rx = reg.register("s1", &sample_params(99, "bob")).unwrap();
    assert!(!rx.has_changed().unwrap_or(false));
    let killed = reg.kill_sessions_for_user(99, KillReason::AdminKill);
    assert_eq!(killed, 1);
    assert!(rx.has_changed().unwrap_or(false));
    assert_ne!(*rx.borrow_and_update(), KillReason::Alive);
}

#[test]
fn show_sessions_lists_active() {
    let reg = SessionRegistry::new();
    reg.register("sess-xyz", &sample_params(7, "carol"))
        .unwrap();
    let all = reg.list_all();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].session_id, "sess-xyz");
    assert_eq!(all[0].user_id, 7);
    assert_eq!(all[0].protocol, "native");
}

// ---------------------------------------------------------------------------
// Section B + F: credential version advances on mutation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn credential_version_bumps_on_mutation() {
    let state = make_state();
    let v0 = state.credentials.current_version(0); // user not yet created

    // create_user allocates a user_id; we need to capture it.
    let user_id = state
        .credentials
        .create_user("dave", "pass123", TenantId::new(1), vec![Role::ReadOnly])
        .expect("create_user failed");

    let v1 = state.credentials.current_version(user_id);
    assert!(v1 > v0, "version must advance after create_user");

    state
        .credentials
        .update_roles("dave", vec![Role::ReadWrite])
        .expect("update_roles failed");

    let v2 = state.credentials.current_version(user_id);
    assert!(v2 > v1, "version must advance after update_roles");
}

// ---------------------------------------------------------------------------
// Section F: identity rehydrates when version advances
// ---------------------------------------------------------------------------

#[tokio::test]
async fn identity_rehydrate_on_version_advance() {
    let state = make_state();

    let user_id = state
        .credentials
        .create_user("eve", "pass456", TenantId::new(1), vec![Role::ReadOnly])
        .expect("create_user failed");

    // Capture the identity as it was at creation time.
    let v_create = state.credentials.current_version(user_id);
    let identity_v1 = state
        .credentials
        .to_identity("eve", AuthMethod::Trust)
        .expect("identity must exist");
    assert!(identity_v1.roles.contains(&Role::ReadOnly));
    assert!(!identity_v1.roles.contains(&Role::ReadWrite));

    // Mutate roles — version advances.
    state
        .credentials
        .update_roles("eve", vec![Role::ReadOnly, Role::ReadWrite])
        .expect("update_roles failed");

    let v_after = state.credentials.current_version(user_id);
    assert!(v_after > v_create, "version must have advanced");

    // Rehydrate: fetch fresh identity.
    let identity_v2 = state
        .credentials
        .to_identity("eve", AuthMethod::Trust)
        .expect("identity must exist after role update");
    assert!(
        identity_v2.roles.contains(&Role::ReadWrite),
        "rehydrated identity must carry new role"
    );
}

// ---------------------------------------------------------------------------
// Section F: grant immediately visible via rehydrate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn in_flight_propagation_grant_visible() {
    let state = make_state();

    let user_id = state
        .credentials
        .create_user("frank", "pw789", TenantId::new(1), vec![Role::ReadOnly])
        .expect("create_user failed");

    let v_before = state.credentials.current_version(user_id);

    // Grant ReadWrite — simulates GRANT ROLE in-flight while session is open.
    state
        .credentials
        .add_role("frank", Role::ReadWrite)
        .expect("add_role failed");

    let v_after = state.credentials.current_version(user_id);
    assert!(v_after > v_before, "grant must bump version");

    // A session that stored v_before would detect the change and rehydrate.
    // Simulate: fetch fresh identity as session.rs does after version check.
    let fresh = state
        .credentials
        .to_identity("frank", AuthMethod::Trust)
        .expect("identity must exist");
    assert!(
        fresh.roles.contains(&Role::ReadWrite),
        "fresh identity must contain newly granted role"
    );
}

// ---------------------------------------------------------------------------
// A hard revoke reaches sessions with no help from the buses
// ---------------------------------------------------------------------------

/// The state has no runtime here, so no consumer reads either bus. The signal
/// is visible when `drop_user` returns, with no await in between.
#[test]
fn drop_user_ends_sessions_with_no_bus_consumer() {
    let state = make_state();
    let user_id = state
        .credentials
        .create_user("ivy", "pw333", TenantId::new(1), vec![Role::ReadOnly])
        .expect("create_user failed");
    let kill_rx = state
        .session_registry
        .register("ivy-1", &sample_params(user_id, "ivy"))
        .expect("register failed");

    state
        .credentials
        .drop_user("ivy")
        .expect("drop_user failed");

    assert_eq!(*kill_rx.borrow(), KillReason::UserDropped);
}

#[test]
fn a_role_change_leaves_sessions_open_and_a_drop_ends_them() {
    let state = make_state();
    let user_id = state
        .credentials
        .create_user("jan", "pw444", TenantId::new(1), vec![Role::ReadOnly])
        .expect("create_user failed");
    let kill_rx = state
        .session_registry
        .register("jan-1", &sample_params(user_id, "jan"))
        .expect("register failed");

    state
        .credentials
        .update_roles("jan", vec![Role::ReadWrite])
        .expect("update_roles failed");
    assert_eq!(*kill_rx.borrow(), KillReason::Alive);

    state
        .credentials
        .drop_user("jan")
        .expect("drop_user failed");
    assert_eq!(*kill_rx.borrow(), KillReason::UserDropped);
}

/// A consumer that lost events and cannot take the audit lock still leaves the
/// revoke intact. The kill lands with the lock held and the bus overrun. The
/// lost rows then surface as one `AuditBusLagged` row once the lock is released.
#[tokio::test]
async fn a_lagged_bus_and_a_stalled_audit_log_do_not_delay_a_hard_revoke() {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
    let (dispatcher, _) = Dispatcher::new(1, 64);
    let state = SharedState::new(dispatcher, wal).unwrap();
    let user_id = state
        .credentials
        .create_user("kim", "pw555", TenantId::new(1), vec![Role::ReadOnly])
        .expect("create_user failed");
    let kill_rx = state
        .session_registry
        .register("kim-1", &sample_params(user_id, "kim"))
        .expect("register failed");

    {
        let _stalled = state.audit.lock().unwrap();
        // This single-threaded runtime never runs the consumer in this block,
        // so these publishes overrun the session-invalidation bus.
        for _ in 0..=SESSION_INVALIDATION_CHANNEL_CAPACITY {
            state
                .credentials
                .update_roles("kim", vec![Role::ReadOnly])
                .expect("update_roles failed");
        }
        state
            .credentials
            .drop_user("kim")
            .expect("drop_user failed");
        assert_eq!(*kill_rx.borrow(), KillReason::UserDropped);
    }

    let lagged = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let details: Vec<String> = state
                .audit
                .lock()
                .unwrap()
                .query_by_event(&AuditEvent::AuditBusLagged)
                .iter()
                .map(|entry| entry.detail.clone())
                .collect();
            if !details.is_empty() {
                return details;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the lag reaches the audit log");
    assert!(lagged[0].contains("session_invalidation_bus lagged"));
}

// ---------------------------------------------------------------------------
// Section B + F: commit_user_mutation publishes both buses
// ---------------------------------------------------------------------------

#[tokio::test]
async fn commit_user_mutation_publishes_both_buses() {
    // Build standalone CredentialStore + buses without going through SharedState,
    // so we can hold receivers before the mutation.
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _) = Dispatcher::new(1, 64);
    let state = SharedState::new(dispatcher, wal).unwrap();

    // Subscribe to both buses before any mutations.
    let mut uc_rx = state.credentials.subscribe_user_changes();
    let mut si_rx = state.credentials.subscribe_session_invalidation();

    let user_id = state
        .credentials
        .create_user("grace", "pw111", TenantId::new(1), vec![Role::ReadOnly])
        .expect("create_user failed");

    // Drop to trigger both UserChanged AND SessionInvalidated(UserDropped).
    state
        .credentials
        .drop_user("grace")
        .expect("drop_user failed");

    // UserChanged must have been published (at least create + drop = 2).
    let ev = tokio::time::timeout(std::time::Duration::from_millis(200), uc_rx.recv())
        .await
        .expect("timed out waiting for UserChanged")
        .expect("channel closed");
    assert_eq!(ev.user_id, user_id);

    // SessionInvalidated must have been published for drop_user.
    let si_ev = tokio::time::timeout(std::time::Duration::from_millis(200), si_rx.recv())
        .await
        .expect("timed out waiting for SessionInvalidated")
        .expect("channel closed");
    assert_eq!(si_ev.user_id, user_id);
    assert!(
        si_ev.reason.is_hard_revoke(),
        "UserDropped must be hard-revoke"
    );
}

#[tokio::test]
async fn commit_user_mutation_no_invalidation_publishes_user_changed_only() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _) = Dispatcher::new(1, 64);
    let state = SharedState::new(dispatcher, wal).unwrap();

    let mut uc_rx = state.credentials.subscribe_user_changes();
    let mut si_rx = state.credentials.subscribe_session_invalidation();

    let user_id = state
        .credentials
        .create_user("henry", "pw222", TenantId::new(1), vec![Role::ReadOnly])
        .expect("create_user failed");

    // create_user passes None for invalidation → only UserChanged, no SessionInvalidated.
    let uc_ev = tokio::time::timeout(std::time::Duration::from_millis(200), uc_rx.recv())
        .await
        .expect("timed out waiting for UserChanged")
        .expect("channel closed");
    assert_eq!(uc_ev.user_id, user_id);

    // SessionInvalidated must NOT arrive within the timeout.
    let si_result = tokio::time::timeout(std::time::Duration::from_millis(50), si_rx.recv()).await;
    assert!(
        si_result.is_err(),
        "no SessionInvalidated expected for create_user"
    );
}
