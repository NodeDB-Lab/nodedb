// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_SQL_TXN_WRAPPER_STATELESS` override: the `[server.sql]`
//! stateless transaction wrapper switch.
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

pub(in super::super) const ROWS: &[EnvRow] = &[EnvRow {
    name: "NODEDB_SQL_TXN_WRAPPER_STATELESS",
    apply: apply_txn_wrapper_stateless,
    redact: false,
}];
