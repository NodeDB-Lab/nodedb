// SPDX-License-Identifier: BUSL-1.1

//! Startup rejection of malformed background-loop interval overrides.
//!
//! The gate reads these in the same pass as every other override. A value read
//! at loop-spawn time cannot refuse a boot, because the process is already up.

mod support;

use nodedb::ServerConfig;
use nodedb::config::server::apply_env_overrides;
use support::env_guard::{EnvGuard, assert_rejected};

#[test]
fn malformed_clone_sweep_interval_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_CLONE_SWEEP_INTERVAL_MS", "30s");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_CLONE_SWEEP_INTERVAL_MS",
        "30s",
    );
}

#[test]
fn malformed_constraint_reconcile_interval_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS", "1_000");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS",
        "1_000",
    );
}

#[test]
fn malformed_scope_expiry_interval_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_SCOPE_EXPIRY_INTERVAL_SECS", "sixty");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_SCOPE_EXPIRY_INTERVAL_SECS",
        "sixty",
    );
}

/// Below the 10-second floor the sweep costs more than the resolution it
/// buys. The value is out of domain, not merely small.
#[test]
fn scope_expiry_interval_below_floor_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_SCOPE_EXPIRY_INTERVAL_SECS", "5");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_SCOPE_EXPIRY_INTERVAL_SECS",
        "5",
    );
}
