// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_TLS_CERT_PATH` / `NODEDB_TLS_KEY_PATH` — which can create
//! `[server.tls]` — and the per-protocol `NODEDB_TLS_*` toggles, which
//! require it already exists.

use std::path::PathBuf;

use crate::config::server::{ServerConfig, TlsSettings};

use super::super::parse::parse_bool_lenient;
use super::super::table::EnvRow;

/// Requires `NODEDB_TLS_KEY_PATH` also set — read directly rather than
/// through the dispatcher, since sibling rows are evaluated independently.
fn apply_cert_path(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    if std::env::var("NODEDB_TLS_KEY_PATH").is_err() {
        return Err("NODEDB_TLS_KEY_PATH to be set alongside it");
    }
    ensure_tls(config).cert_path = PathBuf::from(raw);
    Ok(())
}

fn apply_key_path(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    if std::env::var("NODEDB_TLS_CERT_PATH").is_err() {
        return Err("NODEDB_TLS_CERT_PATH to be set alongside it");
    }
    ensure_tls(config).key_path = PathBuf::from(raw);
    Ok(())
}

/// Returns `[server.tls]`, creating it with every protocol toggle on and no
/// cert-reload interval when none exists yet.
fn ensure_tls(config: &mut ServerConfig) -> &mut TlsSettings {
    config.server.tls.get_or_insert_with(|| TlsSettings {
        cert_path: PathBuf::new(),
        key_path: PathBuf::new(),
        cert_reload_interval_secs: None,
        native: true,
        pgwire: true,
        http: true,
        resp: true,
        ilp: true,
    })
}

/// With `[server.tls]` present, sets the field. With no section and a
/// `false` request, the listener is already plaintext, so the request is
/// already satisfied. With no section and a `true` request, TLS cannot be
/// honored: no cert material exists to serve it.
fn apply_toggle(
    config: &mut ServerConfig,
    raw: &str,
    field: impl FnOnce(&mut TlsSettings) -> &mut bool,
) -> Result<(), &'static str> {
    let requested = parse_bool_lenient(raw)?;
    match config.server.tls.as_mut() {
        Some(tls) => {
            *field(tls) = requested;
            Ok(())
        }
        None if !requested => Ok(()),
        None => Err("a [server.tls] section, or NODEDB_TLS_CERT_PATH and NODEDB_TLS_KEY_PATH"),
    }
}

fn apply_native(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    apply_toggle(config, raw, |tls| &mut tls.native)
}

fn apply_pgwire(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    apply_toggle(config, raw, |tls| &mut tls.pgwire)
}

fn apply_http(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    apply_toggle(config, raw, |tls| &mut tls.http)
}

fn apply_resp(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    apply_toggle(config, raw, |tls| &mut tls.resp)
}

fn apply_ilp(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    apply_toggle(config, raw, |tls| &mut tls.ilp)
}

pub(in super::super) const CERT_KEY_ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_TLS_CERT_PATH",
        apply: apply_cert_path,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_TLS_KEY_PATH",
        apply: apply_key_path,
        redact: false,
    },
];

pub(in super::super) const TOGGLE_ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_TLS_NATIVE",
        apply: apply_native,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_TLS_PGWIRE",
        apply: apply_pgwire,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_TLS_HTTP",
        apply: apply_http,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_TLS_RESP",
        apply: apply_resp,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_TLS_ILP",
        apply: apply_ilp,
        redact: false,
    },
];
