// SPDX-License-Identifier: BUSL-1.1

//! Timeseries memtable admission knobs: `NODEDB_TS_MEMTABLE_BUDGET_BYTES`,
//! `NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES`, `NODEDB_TS_MAX_TAG_CARDINALITY`.

use crate::config::server::ServerConfig;

use super::super::parse::{parse_u32_positive, parse_usize_positive};
use super::super::table::EnvRow;

fn apply_memtable_budget_bytes(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.tuning.timeseries.memtable_budget_bytes = parse_usize_positive(raw)?;
    Ok(())
}

fn apply_memtable_hard_limit_bytes(
    config: &mut ServerConfig,
    raw: &str,
) -> Result<(), &'static str> {
    config.tuning.timeseries.memtable_hard_limit_bytes = parse_usize_positive(raw)?;
    Ok(())
}

fn apply_max_tag_cardinality(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.tuning.timeseries.max_tag_cardinality = parse_u32_positive(raw)?;
    Ok(())
}

pub(in super::super) const ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_TS_MEMTABLE_BUDGET_BYTES",
        apply: apply_memtable_budget_bytes,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES",
        apply: apply_memtable_hard_limit_bytes,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_TS_MAX_TAG_CARDINALITY",
        apply: apply_max_tag_cardinality,
        redact: false,
    },
];
