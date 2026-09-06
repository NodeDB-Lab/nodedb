// SPDX-License-Identifier: BUSL-1.1

//! Startup rejection of malformed listener and memory overrides.
//!
//! A value that fails to parse stops the boot. The error names the variable
//! and the value. An orchestration typo therefore cannot move the node to
//! another address or port, or leave a listener plaintext.

mod support;

use nodedb::ServerConfig;
use nodedb::config::server::{TlsSettings, apply_env_overrides};
use support::env_guard::{EnvGuard, assert_rejected};

fn with_tls() -> ServerConfig {
    let mut cfg = ServerConfig::default();
    cfg.server.tls = Some(TlsSettings {
        cert_path: std::path::PathBuf::from("/etc/nodedb/tls/server.crt"),
        key_path: std::path::PathBuf::from("/etc/nodedb/tls/server.key"),
        cert_reload_interval_secs: None,
        native: true,
        pgwire: true,
        http: true,
        resp: true,
        ilp: true,
    });
    cfg
}

#[test]
fn malformed_host_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_HOST", "not-an-ip");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_HOST", "not-an-ip");
}

#[test]
fn malformed_sync_host_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_SYNC_HOST", "127.0.0.256");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_SYNC_HOST",
        "127.0.0.256",
    );
}

#[test]
fn malformed_native_port_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_PORT_NATIVE", "sixty-four-thirty-three");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_PORT_NATIVE",
        "sixty-four-thirty-three",
    );
}

#[test]
fn out_of_range_pgwire_port_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_PORT_PGWIRE", "70000");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_PORT_PGWIRE", "70000");
}

#[test]
fn malformed_http_port_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_PORT_HTTP", "6480x");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_PORT_HTTP", "6480x");
}

#[test]
fn malformed_sync_port_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_PORT_SYNC", "notaport");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_PORT_SYNC",
        "notaport",
    );
}

/// A malformed value on a listener that is off by default is not "leave it
/// off". The operator asked for RESP. Without this, the client fails to
/// connect instead of the server failing to start.
#[test]
fn malformed_resp_port_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_PORT_RESP", "resp");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_PORT_RESP", "resp");
}

#[test]
fn malformed_ilp_port_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_PORT_ILP", "8086;");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_PORT_ILP", "8086;");
}

#[test]
fn malformed_memory_limit_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_MEMORY_LIMIT", "4ZiB");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_MEMORY_LIMIT", "4ZiB");
}

#[test]
fn non_numeric_memory_limit_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_MEMORY_LIMIT", "notanumber");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_MEMORY_LIMIT",
        "notanumber",
    );
}

/// A malformed TLS toggle is the worst case in this class. A fallback keeps
/// the listener wherever the config file left it. The operator then gets the
/// opposite transport security from the one they asked for.
#[test]
fn malformed_native_tls_toggle_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TLS_NATIVE", "yes-please");
    let mut cfg = with_tls();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_TLS_NATIVE",
        "yes-please",
    );
}

#[test]
fn malformed_pgwire_tls_toggle_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TLS_PGWIRE", "off");
    let mut cfg = with_tls();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_TLS_PGWIRE", "off");
}

#[test]
fn malformed_http_tls_toggle_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TLS_HTTP", "disable");
    let mut cfg = with_tls();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_TLS_HTTP", "disable");
}

#[test]
fn malformed_resp_tls_toggle_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TLS_RESP", "2");
    let mut cfg = with_tls();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_TLS_RESP", "2");
}

#[test]
fn malformed_ilp_tls_toggle_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TLS_ILP", "nope");
    let mut cfg = with_tls();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_TLS_ILP", "nope");
}

/// Enabling TLS with no `[server.tls]` section is unsatisfiable. The process
/// holds no certificate material to serve. The boot refuses instead of
/// pretending the request was honored.
#[test]
fn tls_enable_without_tls_section_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TLS_PGWIRE", "true");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_TLS_PGWIRE", "true");
}

/// Both cert and key paths set with no prior `[server.tls]` section create
/// one, with every protocol toggle on by default.
#[test]
fn tls_cert_and_key_paths_create_tls_section() {
    let _guard = EnvGuard::set_all(&[
        ("NODEDB_TLS_CERT_PATH", "/etc/nodedb/tls/server.crt"),
        ("NODEDB_TLS_KEY_PATH", "/etc/nodedb/tls/server.key"),
    ]);
    let mut cfg = ServerConfig::default();
    apply_env_overrides(&mut cfg).expect("both paths set must create the tls section");
    let tls = cfg.server.tls.expect("tls section must be created");
    assert_eq!(
        tls.cert_path,
        std::path::PathBuf::from("/etc/nodedb/tls/server.crt")
    );
    assert_eq!(
        tls.key_path,
        std::path::PathBuf::from("/etc/nodedb/tls/server.key")
    );
    assert!(tls.native);
    assert!(tls.pgwire);
    assert!(tls.http);
    assert!(tls.resp);
    assert!(tls.ilp);
}

/// A cert path with no key path is unsatisfiable: half a credential pair
/// cannot serve TLS.
#[test]
fn tls_cert_path_without_key_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TLS_CERT_PATH", "/etc/nodedb/tls/server.crt");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_TLS_CERT_PATH",
        "/etc/nodedb/tls/server.crt",
    );
}

/// A key path with no cert path is the symmetric half-a-credential case.
#[test]
fn tls_key_path_without_cert_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_TLS_KEY_PATH", "/etc/nodedb/tls/server.key");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_TLS_KEY_PATH",
        "/etc/nodedb/tls/server.key",
    );
}

/// A set-but-empty `NODEDB_DATA_DIR` is a failed template substitution, not
/// an operator choice of the current directory.
#[test]
fn empty_data_dir_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_DATA_DIR", "");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_DATA_DIR", "");
}
