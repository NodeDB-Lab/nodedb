// SPDX-License-Identifier: BUSL-1.1

//! The route of one request.
//!
//! A transport splits the request text once, then calls [`select_route`] on
//! the carried statements. The route names the path that runs the request.
//! Every transport matches the same enum, so no transport decides a route
//! of its own. See the "Request model" section of `docs/architecture.md`.

use crate::config::server::SqlSection;
use crate::control::server::shared::sql_split::{Statement, statement_texts};
use crate::control::server::shared::statement_exec::sequential_gate;
use crate::control::server::shared::txn_control::{TxnWrapper, classify, classify_txn_wrapper};

/// The transaction-control policies of a transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestPolicy {
    /// Accept `BEGIN; ..; COMMIT` and `BEGIN; ..; ROLLBACK` around a whole
    /// request. A transport with no session sets it from the stateless
    /// wrapper config.
    pub txn_wrapper: bool,
    /// Run a request of several statements that holds transaction control
    /// under PostgreSQL rules. Only the native transport holds a session.
    pub txn_control_native: bool,
}

impl RequestPolicy {
    /// No transaction-control policy: a request that holds transaction
    /// control beside other statements takes the one-set refusal.
    pub const NONE: Self = Self {
        txn_wrapper: false,
        txn_control_native: false,
    };

    /// The policy of a transport with no session: HTTP, NDJSON and
    /// WebSocket RPC.
    pub fn stateless(config: &SqlSection) -> Self {
        Self {
            txn_wrapper: config.txn_wrapper_stateless,
            txn_control_native: false,
        }
    }

    /// The policy of the native transport, which holds a session.
    pub fn session(config: &SqlSection) -> Self {
        Self {
            txn_wrapper: false,
            txn_control_native: config.txn_control_native,
        }
    }
}

/// The path that runs a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestRoute {
    /// Zero or one statement. The transport tries the lone DDL router, then
    /// plans the whole text through the single-statement path.
    Lone,
    /// Several statements, at least one DDL or router statement. The
    /// sequential runner runs them in order on one transaction.
    Sequential,
    /// Several statements that hold transaction control, on a transport with
    /// a session. Each statement runs under PostgreSQL rules.
    TransactionControl,
    /// `BEGIN; ..; COMMIT` (`rolls_back` false) or `BEGIN; ..; ROLLBACK`
    /// (`rolls_back` true) around the whole request. The inner statements
    /// plan as one set.
    Wrapper { rolls_back: bool },
    /// Transaction control in any place the wrapper policy does not accept.
    /// Admission answers SQLSTATE 0A000.
    WrapperRefused,
    /// Several statements, each planned alone and joined into one set.
    OneSet,
}

/// Select the route of a request.
///
/// `statements` is the request text split once. The checks run in this
/// order:
///
/// 1. Fewer than two statements: [`RequestRoute::Lone`].
/// 2. `policy.txn_control_native` and a transaction-control statement:
///    [`RequestRoute::TransactionControl`].
/// 3. [`sequential_gate`] accepts: [`RequestRoute::Sequential`]. The gate
///    declines any request that holds transaction control.
/// 4. `policy.txn_wrapper`: the wrapper shape picks [`RequestRoute::Wrapper`],
///    [`RequestRoute::WrapperRefused`], or [`RequestRoute::OneSet`] when no
///    statement is transaction control.
/// 5. Otherwise [`RequestRoute::OneSet`].
pub fn select_route(statements: &[Statement], policy: RequestPolicy) -> RequestRoute {
    if statements.len() < 2 {
        return RequestRoute::Lone;
    }
    if policy.txn_control_native
        && statements
            .iter()
            .any(|statement| classify(&statement.text).is_some())
    {
        return RequestRoute::TransactionControl;
    }
    if sequential_gate(statements) {
        return RequestRoute::Sequential;
    }
    if !policy.txn_wrapper {
        return RequestRoute::OneSet;
    }
    match classify_txn_wrapper(&statement_texts(statements)) {
        TxnWrapper::None => RequestRoute::OneSet,
        TxnWrapper::Unsupported => RequestRoute::WrapperRefused,
        TxnWrapper::Wrapped { commit } => RequestRoute::Wrapper {
            rolls_back: !commit,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{RequestPolicy, RequestRoute, select_route};
    use crate::control::server::shared::sql_split::split_statements;

    const DDL: &str = "CREATE COLLECTION t (id TEXT PRIMARY KEY)";
    const INSERT: &str = "INSERT INTO t (id) VALUES ('a')";
    const SELECT: &str = "SELECT id FROM t";

    const WRAPPER: RequestPolicy = RequestPolicy {
        txn_wrapper: true,
        txn_control_native: false,
    };
    const NATIVE: RequestPolicy = RequestPolicy {
        txn_wrapper: false,
        txn_control_native: true,
    };
    const BOTH: RequestPolicy = RequestPolicy {
        txn_wrapper: true,
        txn_control_native: true,
    };

    fn route(sql: &str, policy: RequestPolicy) -> RequestRoute {
        select_route(&split_statements(sql), policy)
    }

    #[test]
    fn every_route_is_selected_by_the_request_text_and_the_policy() {
        let rows: Vec<(String, RequestPolicy, RequestRoute)> = vec![
            // Lone: zero or one statement, under every policy.
            (String::new(), RequestPolicy::NONE, RequestRoute::Lone),
            (";".into(), RequestPolicy::NONE, RequestRoute::Lone),
            (SELECT.into(), RequestPolicy::NONE, RequestRoute::Lone),
            (format!("{SELECT};"), WRAPPER, RequestRoute::Lone),
            (DDL.into(), NATIVE, RequestRoute::Lone),
            ("BEGIN".into(), BOTH, RequestRoute::Lone),
            ("BEGIN;".into(), BOTH, RequestRoute::Lone),
            ("COMMIT".into(), WRAPPER, RequestRoute::Lone),
            // Sequential: DDL or a router statement beside other statements.
            (
                format!("{DDL}; {INSERT}"),
                RequestPolicy::NONE,
                RequestRoute::Sequential,
            ),
            (
                format!("{INSERT}; {DDL}; {SELECT}"),
                BOTH,
                RequestRoute::Sequential,
            ),
            (
                format!("{INSERT}; UPSERT INTO t (id) VALUES ('b')"),
                WRAPPER,
                RequestRoute::Sequential,
            ),
            (
                format!("{DDL}; SHOW COLLECTIONS"),
                NATIVE,
                RequestRoute::Sequential,
            ),
            // OneSet: DML and queries only, or transaction control with no
            // policy to run it.
            ("SELECT 1; SELECT 2".into(), NATIVE, RequestRoute::OneSet),
            ("SET x = 1; SELECT 2".into(), NATIVE, RequestRoute::OneSet),
            (
                format!("{INSERT}; {SELECT}"),
                RequestPolicy::NONE,
                RequestRoute::OneSet,
            ),
            (format!("{INSERT}; {SELECT}"), BOTH, RequestRoute::OneSet),
            (
                format!("BEGIN; {INSERT}; COMMIT"),
                RequestPolicy::NONE,
                RequestRoute::OneSet,
            ),
            (
                format!("{INSERT}; SET x = 1"),
                WRAPPER,
                RequestRoute::OneSet,
            ),
            (format!("{DDL}; SET x = 1"), WRAPPER, RequestRoute::OneSet),
            // Wrapper: BEGIN and an end around the whole request.
            (
                format!("BEGIN; {INSERT}; {SELECT}; COMMIT"),
                WRAPPER,
                RequestRoute::Wrapper { rolls_back: false },
            ),
            (
                format!("BEGIN; {INSERT}; ROLLBACK"),
                WRAPPER,
                RequestRoute::Wrapper { rolls_back: true },
            ),
            (
                "BEGIN; COMMIT".into(),
                WRAPPER,
                RequestRoute::Wrapper { rolls_back: false },
            ),
            // WrapperRefused: transaction control in any other place.
            (
                format!("{INSERT}; COMMIT; {SELECT}"),
                WRAPPER,
                RequestRoute::WrapperRefused,
            ),
            (
                format!("BEGIN; {INSERT}"),
                WRAPPER,
                RequestRoute::WrapperRefused,
            ),
            (
                format!("SAVEPOINT s; {INSERT}"),
                WRAPPER,
                RequestRoute::WrapperRefused,
            ),
            (
                format!("BEGIN; BEGIN; {INSERT}; COMMIT"),
                WRAPPER,
                RequestRoute::WrapperRefused,
            ),
            // TransactionControl: a session transport runs every shape.
            (
                format!("BEGIN; {INSERT}; COMMIT"),
                NATIVE,
                RequestRoute::TransactionControl,
            ),
            (
                format!("{SELECT}; COMMIT; {INSERT}"),
                BOTH,
                RequestRoute::TransactionControl,
            ),
            (
                format!("SAVEPOINT s; {INSERT}"),
                NATIVE,
                RequestRoute::TransactionControl,
            ),
        ];
        for (sql, policy, expected) in rows {
            assert_eq!(route(&sql, policy), expected, "{sql:?} under {policy:?}");
        }
    }

    #[test]
    fn a_split_with_semicolons_in_strings_does_not_add_statements() {
        let sql = "INSERT INTO t (id) VALUES ('a;b');";
        assert_eq!(route(sql, BOTH), RequestRoute::Lone);
    }

    #[test]
    fn the_gate_wins_over_the_wrapper_policy_for_ddl() {
        let sql = format!("{DDL}; {INSERT}");
        assert_eq!(route(&sql, WRAPPER), RequestRoute::Sequential);
    }
}
