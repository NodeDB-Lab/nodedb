// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_MEMORY_LIMIT` / `NODEDB_DATA_PLANE_CORES` / `NODEDB_MAX_CONNECTIONS`
//! / `NODEDB_LOG_FORMAT` overrides — process sizing and admission knobs.

use crate::config::server::{LogFormat, ServerConfig};

use super::super::memory_size::parse_memory_size;
use super::super::parse::{parse_usize_nonneg, parse_usize_positive};
use super::super::table::EnvRow;

fn apply_memory_limit(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.memory_limit =
        parse_memory_size(raw).map_err(|_| "a memory size such as 4GiB")?;
    Ok(())
}

fn apply_data_plane_cores(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.data_plane_cores = parse_usize_positive(raw)?;
    Ok(())
}

/// Zero is legal here and means unlimited, unlike every other sizing row.
fn apply_max_connections(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.max_connections = parse_usize_nonneg(raw)?;
    Ok(())
}

fn apply_log_format(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.log_format = match raw.trim().to_ascii_lowercase().as_str() {
        "text" => LogFormat::Text,
        "json" => LogFormat::Json,
        _ => return Err("\"text\" or \"json\""),
    };
    Ok(())
}

pub(in super::super) const ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_MEMORY_LIMIT",
        apply: apply_memory_limit,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_DATA_PLANE_CORES",
        apply: apply_data_plane_cores,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_MAX_CONNECTIONS",
        apply: apply_max_connections,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_LOG_FORMAT",
        apply: apply_log_format,
        redact: false,
    },
];

#[cfg(test)]
mod tests {
    use super::super::super::apply_env_overrides;
    use super::*;

    #[test]
    fn env_memory_limit_overrides() {
        unsafe { std::env::set_var("NODEDB_MEMORY_LIMIT", "2GiB") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg).expect("a valid memory size must apply");
        assert_eq!(cfg.server.memory_limit, 2 * 1024 * 1024 * 1024);
        unsafe { std::env::remove_var("NODEDB_MEMORY_LIMIT") };
    }
}
