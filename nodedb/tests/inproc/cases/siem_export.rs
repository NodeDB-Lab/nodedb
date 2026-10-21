// SPDX-License-Identifier: BUSL-1.1

//! SIEM export wiring: the `[auth.siem]` server-config section must reach the
//! exporter, and every audit event recorded through `SharedState` must land in
//! the right export buffer — bounded, with overflow counted.
//!
//! The webhook POST itself has no local HTTP sink in this harness; delivery
//! behaviour (payload signing, requeue-on-failure) is covered by the unit
//! tests in `control/security/siem/`.

use std::sync::Arc;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthConfig;
use nodedb::control::security::audit::AuditEvent;
use nodedb::control::security::siem::SiemConfig;
use nodedb::control::state::SharedState;

/// Open a catalog-backed `SharedState` from a server config whose `[auth]`
/// section carries `siem`, exactly as an operator's config file would.
fn open_state(siem: Option<SiemConfig>) -> (Arc<SharedState>, tempfile::TempDir) {
    let (state, dir) = try_open_state(siem);
    (state.expect("shared state opens"), dir)
}

/// `open_state` without the success expectation.
fn try_open_state(
    siem: Option<SiemConfig>,
) -> (nodedb::Result<Arc<SharedState>>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).expect("wal"));
    let (dispatcher, _sides) = Dispatcher::new(1, 64);
    let catalog_path = dir.path().join("system.redb");
    let auth_config = AuthConfig {
        siem,
        ..AuthConfig::default()
    };
    let state = SharedState::open(
        nodedb::control::state::DataPlaneHandles {
            dispatcher,
            quiesce: nodedb::bridge::quiesce::CollectionQuiesce::new(),
            array_catalog: nodedb::control::array_catalog::ArrayCatalog::handle(),
            system_metrics: std::sync::Arc::new(nodedb::control::metrics::SystemMetrics::new()),
        },
        wal,
        &catalog_path,
        nodedb::control::state::OpenConfig::strict(&auth_config),
        nodedb_types::config::TuningConfig::default(),
        false,
        nodedb::data::executor::core_loop::test_governor(),
    );
    (state, dir)
}

fn webhook_siem(buffer_size: usize) -> SiemConfig {
    SiemConfig {
        destinations: vec!["webhook".into()],
        webhook_url: "https://siem.example/ingest".into(),
        webhook_hmac_secret: "s3cret".into(),
        buffer_size,
        ..SiemConfig::default()
    }
}

/// The SIEM endpoint is operator config. One the `[egress]` policy refuses
/// stops startup instead of failing every export later.
#[test]
fn a_siem_webhook_outside_the_egress_policy_stops_startup() {
    for url in [
        "http://siem.example/ingest",
        "https://127.0.0.1:8088/ingest",
    ] {
        let (state, _dir) = try_open_state(Some(SiemConfig {
            webhook_url: url.into(),
            ..webhook_siem(64)
        }));
        let err = state
            .err()
            .unwrap_or_else(|| panic!("{url} must be refused"));
        assert!(err.to_string().contains("[egress]"), "{url}: {err}");
    }
}

#[test]
fn siem_config_from_server_config_reaches_the_exporter() {
    let (configured, _d1) = open_state(Some(webhook_siem(64)));
    assert!(
        configured.siem.is_configured(),
        "an [auth.siem] section must make the exporter live"
    );
    assert_eq!(configured.siem.buffer_capacity(), 64);

    let (absent, _d2) = open_state(None);
    assert!(
        !absent.siem.is_configured(),
        "no [auth.siem] section leaves the exporter dormant"
    );
}

#[test]
fn recorded_audit_event_lands_in_the_audit_buffer() {
    let (state, _dir) = open_state(Some(webhook_siem(64)));
    state.audit_record(AuditEvent::AdminAction, None, "test", "manual op");

    let audit = state.siem.drain_audit();
    assert_eq!(audit.len(), 1, "audit event must be exported");
    assert_eq!(audit[0].event, AuditEvent::AdminAction);
    assert_eq!(audit[0].detail, "manual op");
    assert!(
        state.siem.drain_auth().is_empty(),
        "non-auth event must not enter the auth buffer"
    );
}

#[test]
fn recorded_auth_event_lands_in_the_auth_buffer() {
    let (state, _dir) = open_state(Some(webhook_siem(64)));
    state.audit_record(AuditEvent::AuthFailure, None, "10.0.0.1", "bad password");

    let auth = state.siem.drain_auth();
    assert_eq!(auth.len(), 1, "auth event must be exported");
    assert_eq!(auth[0].event, AuditEvent::AuthFailure);
    assert!(
        state.siem.drain_audit().is_empty(),
        "auth event must not enter the audit buffer"
    );
}

#[test]
fn unconfigured_exporter_buffers_nothing() {
    let (state, _dir) = open_state(None);
    state.audit_record(AuditEvent::AdminAction, None, "test", "manual op");
    state.audit_record(AuditEvent::AuthSuccess, None, "10.0.0.1", "login");

    assert_eq!(
        state.siem.buffered_count(),
        0,
        "the default (unconfigured) exporter must be a no-op on the audit path"
    );
    assert_eq!(state.siem.dropped_audit_events(), 0);
    assert_eq!(state.siem.dropped_auth_events(), 0);
}

#[test]
fn export_buffer_is_bounded_and_overflow_is_counted() {
    let (state, _dir) = open_state(Some(webhook_siem(2)));
    for i in 0..5 {
        state.audit_record(AuditEvent::AdminAction, None, "test", &format!("op-{i}"));
    }

    assert_eq!(
        state.siem.buffered_count(),
        2,
        "buffer must not grow past the configured ceiling"
    );
    assert_eq!(
        state.siem.dropped_audit_events(),
        3,
        "evicted events must be counted, not silently lost"
    );

    // Eviction is oldest-first: the most recent events survive.
    let kept: Vec<String> = state
        .siem
        .drain_audit()
        .iter()
        .map(|e| e.detail.clone())
        .collect();
    assert_eq!(kept, vec!["op-3".to_string(), "op-4".to_string()]);
}
