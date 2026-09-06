// SPDX-License-Identifier: BUSL-1.1

//! Startup rejection of malformed observability overrides.
//!
//! A mistyped value here costs the metrics endpoint, the OTLP export, or the
//! debug-endpoint gate. The gate names the variable, so the boot log points
//! at the typo.

mod support;

use nodedb::ServerConfig;
use nodedb::config::server::apply_env_overrides;
use support::env_guard::{EnvGuard, assert_rejected};

#[test]
fn malformed_promql_enabled_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_PROMQL_ENABLED", "enabled");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_PROMQL_ENABLED",
        "enabled",
    );
}

#[test]
fn malformed_otlp_receiver_enabled_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_OTLP_RECEIVER_ENABLED", "sometimes");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_OTLP_RECEIVER_ENABLED",
        "sometimes",
    );
}

/// Every toggle in the override surface takes the same vocabulary. `1` on one
/// listener and `yes` on another get the same answer, here too.
#[test]
fn observability_toggles_take_the_shared_bool_vocabulary() {
    let _guard = EnvGuard::set_all(&[
        ("NODEDB_PROMQL_ENABLED", "0"),
        ("NODEDB_OTLP_RECEIVER_ENABLED", "1"),
        ("NODEDB_OTLP_EXPORT_ENABLED", "yes"),
        ("NODEDB_DEBUG_ENDPOINTS_ENABLED", "no"),
    ]);
    let mut cfg = ServerConfig::default();
    apply_env_overrides(&mut cfg).expect("shared bool vocabulary applies");
    assert!(!cfg.observability.promql.enabled);
    assert!(cfg.observability.otlp.receiver.enabled);
    assert!(cfg.observability.otlp.export.enabled);
    assert!(!cfg.observability.debug_endpoints_enabled);
}

#[test]
fn malformed_otlp_http_listen_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_OTLP_HTTP_LISTEN", "0.0.0.0");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_OTLP_HTTP_LISTEN",
        "0.0.0.0",
    );
}

#[test]
fn malformed_otlp_grpc_listen_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_OTLP_GRPC_LISTEN", "localhost:4317");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_OTLP_GRPC_LISTEN",
        "localhost:4317",
    );
}

#[test]
fn malformed_otlp_export_enabled_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_OTLP_EXPORT_ENABLED", "on");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_OTLP_EXPORT_ENABLED",
        "on",
    );
}

#[test]
fn malformed_otlp_export_interval_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_OTLP_EXPORT_INTERVAL", "15s");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_OTLP_EXPORT_INTERVAL",
        "15s",
    );
}

/// The debug endpoints expose raft internals and the metadata cache. A
/// mistyped value on their gate stops the boot. It never resolves to whatever
/// the config file said.
#[test]
fn malformed_debug_endpoints_enabled_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_DEBUG_ENDPOINTS_ENABLED", "TRUE!");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_DEBUG_ENDPOINTS_ENABLED",
        "TRUE!",
    );
}

/// A collector address with no scheme cannot be dialed as an HTTP endpoint.
#[test]
fn endpoint_without_scheme_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_OTLP_EXPORT_ENDPOINT", "collector.internal:4318");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_OTLP_EXPORT_ENDPOINT",
        "collector.internal:4318",
    );
}
