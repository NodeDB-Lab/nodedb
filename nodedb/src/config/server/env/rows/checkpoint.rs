// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_CHECKPOINT_INTERVAL_SECS` / `NODEDB_WAL_SEGMENT_TARGET_MB`
//! overrides.

use crate::config::server::ServerConfig;

use super::super::parse::parse_u64_positive;
use super::super::table::EnvRow;

fn apply_checkpoint_interval(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.checkpoint.interval_secs = parse_u64_positive(raw)?;
    Ok(())
}

fn apply_wal_segment_target_mb(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.checkpoint.wal_segment_target_mb = parse_u64_positive(raw)?;
    Ok(())
}

pub(in super::super) const ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_CHECKPOINT_INTERVAL_SECS",
        apply: apply_checkpoint_interval,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_WAL_SEGMENT_TARGET_MB",
        apply: apply_wal_segment_target_mb,
        redact: false,
    },
];
