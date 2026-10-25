// SPDX-License-Identifier: BUSL-1.1

//! Auto-escalation wiring: the `[auth.escalation]` server-config section must
//! reach the engine, repeated violations must turn into an account-status
//! verdict that the admission gate enforces, and that verdict must still be
//! there after the process restarts.
//!
//! The threshold arithmetic itself (rolling window, ban ladder, map bounds) is
//! covered by the unit tests in `control/security/escalation/`.

use std::path::Path;
use std::sync::Arc;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthConfig;
use nodedb::control::security::auth_context::AuthStatus;
use nodedb::control::security::escalation::{
    AuthViolation, EscalationConfig, ViolationSubject, record_auth_violation,
};
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet, Role};
use nodedb::control::server::session_auth::guards::check_blacklist;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;
use nodedb_types::id::DatabaseId;

const VIOLATOR_ID: u64 = 4_242;

/// Open a catalog-backed `SharedState` rooted at `dir`, from a server config
/// whose `[auth]` section carries `escalation`, exactly as an operator's
/// config file would. Reopening the same `dir` is a restart.
fn open_state(dir: &Path, escalation: Option<EscalationConfig>) -> Arc<SharedState> {
    let wal = Arc::new(
        nodedb::wal::WalManager::open_for_testing(&dir.join("test.wal")).expect("open test WAL"),
    );
    let (dispatcher, _sides) = Dispatcher::new(1, 64);
    let auth_config = AuthConfig {
        escalation,
        ..AuthConfig::default()
    };
    SharedState::open(
        nodedb::control::state::DataPlaneHandles {
            dispatcher,
            quiesce: nodedb::bridge::quiesce::CollectionQuiesce::new(),
            array_catalog: nodedb::control::array_catalog::ArrayCatalog::handle(),
            system_metrics: std::sync::Arc::new(nodedb::control::metrics::SystemMetrics::new()),
        },
        wal,
        &dir.join("system.redb"),
        nodedb::control::state::OpenConfig::strict(&auth_config),
        nodedb_types::config::TuningConfig::default(),
        false,
        nodedb::data::executor::core_loop::test_governor(),
    )
    .expect("shared state opens")
}

fn strict_escalation(suspend: u32, ban: u32) -> EscalationConfig {
    EscalationConfig {
        enabled: true,
        suspend_after_violations: suspend,
        ban_after_suspensions: ban,
        violation_window_secs: 0,
        ..EscalationConfig::default()
    }
}

fn violator() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_regular(
        VIOLATOR_ID,
        "violator",
        TenantId::new(1),
        AuthMethod::CleartextPassword,
        vec![Role::ReadWrite],
        None,
        DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
    )
}

fn violate(state: &SharedState, identity: &AuthenticatedIdentity) {
    record_auth_violation(
        state,
        AuthViolation {
            subject: ViolationSubject::Identity(identity),
            tenant_id: Some(identity.tenant_id),
            source: "127.0.0.1:5432",
            detail: "integration-test violation",
        },
    );
}

#[test]
fn escalation_config_from_server_config_reaches_the_engine() {
    let dir = tempfile::tempdir().expect("tempdir");

    let configured = open_state(dir.path(), Some(strict_escalation(4, 2)));
    assert!(
        configured.escalation.is_enabled(),
        "an enabled [auth.escalation] section must make the engine live"
    );
    assert_eq!(configured.escalation.config().suspend_after_violations, 4);
    assert_eq!(configured.escalation.config().ban_after_suspensions, 2);
    drop(configured);

    let absent_dir = tempfile::tempdir().expect("tempdir");
    let absent = open_state(absent_dir.path(), None);
    assert!(
        !absent.escalation.is_enabled(),
        "no [auth.escalation] section leaves escalation dormant"
    );
}

#[test]
fn repeated_violations_suspend_the_account_and_the_gate_refuses_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = open_state(dir.path(), Some(strict_escalation(3, 3)));
    let identity = violator();
    let id = VIOLATOR_ID.to_string();

    check_blacklist(&state, &identity, "127.0.0.1:5432")
        .expect("an unescalated account is admitted");

    violate(&state, &identity);
    violate(&state, &identity);
    violate(&state, &identity);

    assert_eq!(
        state.auth_users.get_status(&id),
        Some(AuthStatus::Suspended)
    );
    assert!(
        check_blacklist(&state, &identity, "127.0.0.1:5432").is_err(),
        "the suspension must be visible to the next request"
    );
}

#[test]
fn escalation_verdict_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let identity = violator();
    let id = VIOLATOR_ID.to_string();

    {
        let state = open_state(dir.path(), Some(strict_escalation(2, 3)));
        violate(&state, &identity);
        violate(&state, &identity);
        assert_eq!(
            state.auth_users.get_status(&id),
            Some(AuthStatus::Suspended)
        );
    }

    let restarted = open_state(dir.path(), Some(strict_escalation(2, 3)));
    assert_eq!(
        restarted.auth_users.get_status(&id),
        Some(AuthStatus::Suspended),
        "a suspension that evaporates on restart is a control that silently stops working"
    );
    assert!(
        check_blacklist(&restarted, &identity, "127.0.0.1:5432").is_err(),
        "the restored suspension must still refuse the account"
    );
}

#[test]
fn the_ban_ladder_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let identity = violator();
    let id = VIOLATOR_ID.to_string();

    {
        // One suspension before the restart.
        let state = open_state(dir.path(), Some(strict_escalation(2, 2)));
        violate(&state, &identity);
        violate(&state, &identity);
        assert_eq!(
            state.auth_users.get_status(&id),
            Some(AuthStatus::Suspended)
        );
    }

    let restarted = open_state(dir.path(), Some(strict_escalation(2, 2)));
    restarted
        .auth_users
        .set_status(&id, AuthStatus::Active)
        .expect("an operator restores the account");

    // The second suspension is the ban threshold — reachable only because the
    // first one was restored from the persisted record.
    violate(&restarted, &identity);
    violate(&restarted, &identity);
    assert_eq!(
        restarted.auth_users.get_status(&id),
        Some(AuthStatus::Banned),
        "the suspension count must be hydrated from the auth-user record"
    );
}
