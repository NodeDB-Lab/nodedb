// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_SQL_TXN_WRAPPER_STATELESS` and `NODEDB_SQL_TXN_CONTROL_NATIVE`
//! overrides: the `[server.sql]` transaction-control switches.
//!
//! An unset variable leaves the config value. A malformed value stops
//! startup.

use crate::config::server::ServerConfig;

use super::super::parse::parse_bool_lenient;
use super::super::table::EnvRow;

fn apply_txn_wrapper_stateless(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.sql.txn_wrapper_stateless = parse_bool_lenient(raw)?;
    Ok(())
}

fn apply_txn_control_native(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.server.sql.txn_control_native = parse_bool_lenient(raw)?;
    Ok(())
}

pub(in super::super) const ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_SQL_TXN_WRAPPER_STATELESS",
        apply: apply_txn_wrapper_stateless,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_SQL_TXN_CONTROL_NATIVE",
        apply: apply_txn_control_native,
        redact: false,
    },
];
