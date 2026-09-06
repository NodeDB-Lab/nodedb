// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_CLONE_SWEEP_INTERVAL_MS` / `NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS`
//! / `NODEDB_SCOPE_EXPIRY_INTERVAL_SECS` overrides — background maintenance
//! loop intervals.

use crate::config::server::ServerConfig;

use super::super::parse::{parse_u64_at_least, parse_u64_positive};
use super::super::table::EnvRow;
use crate::config::server::domain::MIN_SCOPE_EXPIRY_SECS;

/// Below 10 seconds the sweep costs more than the resolution it buys.
fn apply_clone_sweep_interval_ms(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.tuning.maintenance.clone_sweep_interval_ms = parse_u64_positive(raw)?;
    Ok(())
}

fn apply_constraint_reconcile_interval_ms(
    config: &mut ServerConfig,
    raw: &str,
) -> Result<(), &'static str> {
    config.tuning.maintenance.constraint_reconcile_interval_ms = parse_u64_positive(raw)?;
    Ok(())
}

fn apply_scope_expiry_interval_secs(
    config: &mut ServerConfig,
    raw: &str,
) -> Result<(), &'static str> {
    config.tuning.maintenance.scope_expiry_interval_secs = parse_u64_at_least(
        raw,
        MIN_SCOPE_EXPIRY_SECS,
        "an interval of at least 10 seconds",
    )?;
    Ok(())
}

pub(in super::super) const ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_CLONE_SWEEP_INTERVAL_MS",
        apply: apply_clone_sweep_interval_ms,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS",
        apply: apply_constraint_reconcile_interval_ms,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_SCOPE_EXPIRY_INTERVAL_SECS",
        apply: apply_scope_expiry_interval_secs,
        redact: false,
    },
];
