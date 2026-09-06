// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_HOST` / `NODEDB_SYNC_HOST` / `NODEDB_PORT_*` / `NODEDB_DATA_DIR`
//! overrides — bind address, per-protocol listener ports, and the on-disk
//! data directory.

use std::path::PathBuf;

use crate::config::server::ServerConfig;

use super::super::parse::{parse_ip, parse_port};
use super::super::table::EnvRow;

fn apply_host(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.host = parse_ip(raw)?;
    Ok(())
}

/// Separate from `NODEDB_HOST`: sync is loopback-only, so this selects a
/// different loopback (`::1`, `127.0.0.2`), not a shared routable address.
fn apply_sync_host(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.sync_host = Some(parse_ip(raw)?);
    Ok(())
}

fn apply_port_native(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.ports.native = parse_port(raw)?;
    Ok(())
}

fn apply_port_pgwire(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.ports.pgwire = parse_port(raw)?;
    Ok(())
}

fn apply_port_http(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.ports.http = parse_port(raw)?;
    Ok(())
}

fn apply_port_sync(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.ports.sync = parse_port(raw)?;
    Ok(())
}

fn apply_port_resp(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.ports.resp = Some(parse_port(raw)?);
    Ok(())
}

fn apply_port_ilp(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.ports.ilp = Some(parse_port(raw)?);
    Ok(())
}

fn apply_data_dir(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.data_dir = PathBuf::from(raw);
    Ok(())
}

pub(in super::super) const ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_HOST",
        apply: apply_host,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_SYNC_HOST",
        apply: apply_sync_host,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_PORT_NATIVE",
        apply: apply_port_native,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_PORT_PGWIRE",
        apply: apply_port_pgwire,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_PORT_HTTP",
        apply: apply_port_http,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_PORT_SYNC",
        apply: apply_port_sync,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_PORT_RESP",
        apply: apply_port_resp,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_PORT_ILP",
        apply: apply_port_ilp,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_DATA_DIR",
        apply: apply_data_dir,
        redact: false,
    },
];

#[cfg(test)]
mod tests {
    use super::super::super::apply_env_overrides;
    use super::*;

    #[test]
    fn env_data_dir_override() {
        unsafe { std::env::set_var("NODEDB_DATA_DIR", "/tmp/test-nodedb") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg).expect("a valid data dir must apply");
        assert_eq!(
            cfg.server.data_dir,
            std::path::PathBuf::from("/tmp/test-nodedb")
        );
        unsafe { std::env::remove_var("NODEDB_DATA_DIR") };
    }

    #[test]
    fn env_sync_port_overrides() {
        unsafe { std::env::set_var("NODEDB_PORT_SYNC", "19090") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg).expect("a valid sync port must apply");
        assert_eq!(cfg.server.ports.sync, 19090);
        unsafe { std::env::remove_var("NODEDB_PORT_SYNC") };
    }
}
