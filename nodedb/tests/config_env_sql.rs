// SPDX-License-Identifier: BUSL-1.1

//! The `[server.sql]` transaction-control switches under their env
//! overrides.
//!
//! `false` turns the policy off and `true` turns it on, whatever the config
//! says. A malformed value stops startup and names the variable and the value.

mod support;

use nodedb::ServerConfig;
use nodedb::config::server::apply_env_overrides;
use support::env_guard::{EnvGuard, assert_rejected};

const STATELESS: &str = "NODEDB_SQL_TXN_WRAPPER_STATELESS";

#[test]
fn false_turns_the_policy_off() {
    let _guard = EnvGuard::set(STATELESS, "false");
    let mut cfg = ServerConfig::default();
    apply_env_overrides(&mut cfg).expect("false applies");
    assert!(!cfg.server.sql.txn_wrapper_stateless);
}

#[test]
fn true_turns_a_policy_the_config_turned_off_back_on() {
    let _guard = EnvGuard::set(STATELESS, "1");
    let mut cfg = ServerConfig::default();
    cfg.server.sql.txn_wrapper_stateless = false;
    apply_env_overrides(&mut cfg).expect("true applies");
    assert!(cfg.server.sql.txn_wrapper_stateless);
}

#[test]
fn malformed_txn_wrapper_stateless_fails_startup() {
    let _guard = EnvGuard::set(STATELESS, "off");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), STATELESS, "off");
}

const NATIVE: &str = "NODEDB_SQL_TXN_CONTROL_NATIVE";

#[test]
fn false_turns_the_native_policy_off() {
    let _guard = EnvGuard::set(NATIVE, "false");
    let mut cfg = ServerConfig::default();
    apply_env_overrides(&mut cfg).expect("false applies");
    assert!(!cfg.server.sql.txn_control_native);
    assert!(cfg.server.sql.txn_wrapper_stateless);
}

#[test]
fn malformed_txn_control_native_fails_startup() {
    let _guard = EnvGuard::set(NATIVE, "disabled");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), NATIVE, "disabled");
}
