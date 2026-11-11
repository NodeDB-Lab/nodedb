// SPDX-License-Identifier: BUSL-1.1

//! `[server.sql]` section: switches for transaction control inside one
//! request of several statements.
//!
//! Each switch is on when not set. Only an explicit `false` turns a policy
//! off. A missing `[server.sql]` section leaves every policy on.

use serde::{Deserialize, Serialize};

/// Transaction-control policies for a request of several statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlSection {
    /// HTTP `/v1/query`, NDJSON `/v1/query/stream` and WebSocket RPC accept
    /// a `BEGIN` and a `COMMIT` or `ROLLBACK` that wrap the whole request.
    /// Off, such a request answers SQLSTATE 0A000. Default `true`.
    #[serde(default = "default_true")]
    pub txn_wrapper_stateless: bool,
}

impl Default for SqlSection {
    fn default() -> Self {
        Self {
            txn_wrapper_stateless: true,
        }
    }
}

/// A bare `#[serde(default)]` gives `false` and turns the policy off.
fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use crate::config::server::ServerConfig;

    fn parse(toml: &str) -> ServerConfig {
        toml::from_str(toml).unwrap_or_else(|e| panic!("parse {toml:?}: {e}"))
    }

    #[test]
    fn a_missing_section_or_key_turns_the_policy_on() {
        for toml in [
            "",
            "[server]\n",
            "[server.ports]\nnative = 7433\n",
            "[server.sql]\n",
        ] {
            assert!(parse(toml).server.sql.txn_wrapper_stateless, "{toml:?}");
        }
    }

    #[test]
    fn an_explicit_value_sets_the_policy() {
        let on = parse("[server.sql]\ntxn_wrapper_stateless = true\n");
        assert!(on.server.sql.txn_wrapper_stateless);
        let off = parse("[server.sql]\ntxn_wrapper_stateless = false\n");
        assert!(!off.server.sql.txn_wrapper_stateless);
    }

    #[test]
    fn the_default_section_turns_the_policy_on() {
        assert!(super::SqlSection::default().txn_wrapper_stateless);
        assert!(ServerConfig::default().server.sql.txn_wrapper_stateless);
    }

    #[test]
    fn an_unknown_key_or_a_value_that_is_not_a_bool_is_rejected() {
        for toml in [
            "[server.sql]\ntxn_wrapper = false\n",
            "[server.sql]\ntxn_wrapper_stateless = \"off\"\n",
        ] {
            let result: Result<ServerConfig, _> = toml::from_str(toml);
            assert!(result.is_err(), "{toml:?}");
        }
    }
}
