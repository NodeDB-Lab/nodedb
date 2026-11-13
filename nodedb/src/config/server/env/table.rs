// SPDX-License-Identifier: BUSL-1.1

//! The environment-override startup gate.
//!
//! One row per `NODEDB_*` variable, walked in order. The gate applies an
//! operator-supplied value, or refuses to start. It never substitutes a config
//! value or a compiled default. The gate collects every violation before
//! returning, so one bad value never hides the next.

use super::rows::{
    checkpoint, cluster, host_ports, maintenance, observability, sizing, timeseries, tls, wal,
};
use crate::config::server::ServerConfig;

/// One `NODEDB_*` override.
pub(super) struct EnvRow {
    /// `NODEDB_*` name, matched verbatim.
    pub name: &'static str,
    /// Applies the raw value. `Err` names what the process needed.
    pub apply: fn(&mut ServerConfig, &str) -> Result<(), &'static str>,
    /// `true` logs the name and never the value.
    pub redact: bool,
}

/// Row groups in startup-gate order.
///
/// `NODEDB_TLS_CERT_PATH` and `NODEDB_TLS_KEY_PATH` run first: they can
/// create `[server.tls]`, which the TLS toggle rows further down require.
const TABLE: &[&[EnvRow]] = &[
    tls::CERT_KEY_ROWS,
    host_ports::ROWS,
    sizing::ROWS,
    tls::TOGGLE_ROWS,
    cluster::ROWS,
    wal::ROWS,
    checkpoint::ROWS,
    timeseries::ROWS,
    maintenance::ROWS,
    observability::ROWS,
];

/// One override that `apply_env_overrides` applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedOverride {
    /// `NODEDB_*` variable name.
    pub name: &'static str,
    /// The raw value, or `<redacted>` for a secret row.
    pub logged_value: String,
}

/// Logs each applied override at `info` level.
///
/// Call this once tracing is registered. `apply_env_overrides` itself logs
/// nothing, because the boot applies overrides before tracing exists.
pub fn log_applied_overrides(applied: &[AppliedOverride]) {
    for o in applied {
        tracing::info!(
            env_var = o.name,
            value = o.logged_value.as_str(),
            "environment variable override applied"
        );
    }
}

/// Applies every `NODEDB_*` override present in the environment to `config`.
///
/// Returns the applied overrides, with secret values redacted, or every
/// violation joined into one [`crate::Error::Config`]. A run with any
/// violation must exit the process, so partial application here never
/// reaches a running server.
pub fn apply_env_overrides(config: &mut ServerConfig) -> crate::Result<Vec<AppliedOverride>> {
    let mut violations = Vec::new();
    let mut applied = Vec::new();

    for group in TABLE {
        for row in *group {
            let Ok(raw) = std::env::var(row.name) else {
                continue;
            };
            if raw.trim().is_empty() {
                violations.push(format!(
                    "invalid value '{raw}' for {}: expected a non-empty value",
                    row.name
                ));
                continue;
            }
            match (row.apply)(config, &raw) {
                Ok(()) => {
                    let logged_value = if row.redact {
                        "<redacted>".to_string()
                    } else {
                        raw
                    };
                    applied.push(AppliedOverride {
                        name: row.name,
                        logged_value,
                    });
                }
                Err(expected) => {
                    violations.push(format!(
                        "invalid value '{raw}' for {}: expected {expected}",
                        row.name
                    ));
                }
            }
        }
    }

    if violations.is_empty() {
        Ok(applied)
    } else {
        Err(crate::Error::Config {
            detail: violations.join("; "),
        })
    }
}
