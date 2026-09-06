// SPDX-License-Identifier: BUSL-1.1

//! Startup rejection of malformed sizing and admission overrides.
//!
//! These variables decide how much of the machine the process takes. A value
//! that fails to parse stops the boot. The compiled defaults are
//! host-dependent, so a fallback runs the node at an unpredictable capacity.

mod support;

use nodedb::ServerConfig;
use nodedb::config::server::apply_env_overrides;
use support::env_guard::{EnvGuard, assert_rejected};

#[test]
fn malformed_data_plane_cores_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_DATA_PLANE_CORES", "abc");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_DATA_PLANE_CORES",
        "abc",
    );
}

/// Zero parses as a `usize` and asks the Data Plane for a shard count no
/// query can reach. A core count is strictly positive. The gate rejects it
/// the way it rejects a non-numeric value.
#[test]
fn zero_data_plane_cores_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_DATA_PLANE_CORES", "0");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_DATA_PLANE_CORES",
        "0",
    );
}

#[test]
fn negative_data_plane_cores_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_DATA_PLANE_CORES", "-4");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_DATA_PLANE_CORES",
        "-4",
    );
}

#[test]
fn malformed_max_connections_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_MAX_CONNECTIONS", "many");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_MAX_CONNECTIONS",
        "many",
    );
}

/// The TOML path documents log format as rejected at startup with no silent
/// fallback. The environment path is the same setting and owes the operator
/// the same contract.
#[test]
fn unknown_log_format_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_LOG_FORMAT", "logfmt");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_LOG_FORMAT", "logfmt");
}

#[test]
fn malformed_timeseries_memtable_budget_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TS_MEMTABLE_BUDGET_BYTES", "64MiB");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_TS_MEMTABLE_BUDGET_BYTES",
        "64MiB",
    );
}

#[test]
fn zero_timeseries_memtable_budget_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TS_MEMTABLE_BUDGET_BYTES", "0");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_TS_MEMTABLE_BUDGET_BYTES",
        "0",
    );
}

#[test]
fn malformed_timeseries_memtable_hard_limit_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES", "unbounded");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES",
        "unbounded",
    );
}

#[test]
fn zero_timeseries_memtable_hard_limit_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES", "0");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES",
        "0",
    );
}

#[test]
fn malformed_timeseries_tag_cardinality_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TS_MAX_TAG_CARDINALITY", "100k");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_TS_MAX_TAG_CARDINALITY",
        "100k",
    );
}

#[test]
fn zero_timeseries_tag_cardinality_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TS_MAX_TAG_CARDINALITY", "0");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_TS_MAX_TAG_CARDINALITY",
        "0",
    );
}
