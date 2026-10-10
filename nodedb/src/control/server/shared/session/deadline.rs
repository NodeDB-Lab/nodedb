// SPDX-License-Identifier: BUSL-1.1

//! The deadline of the statement now running on this connection.
//!
//! One client statement fans out into many Control -> Data requests: the write
//! funnel, the gateway's local route and its remote `ExecuteRequest`, an
//! Exchange gather across every core, a shuffle hop, a graph broadcast. All of
//! them belong to one statement, so all of them must stop at the SAME instant,
//! and each stamps that instant on the request envelope it builds.
//!
//! Passing the value down those signatures means a caller can miss one, and a
//! missed one silently restarts the clock at the node default — the defect this
//! closes. The deadline therefore lives in the connection scope
//! ([`super::conn_scope`]) beside the DDL buffer and the audit context, and
//! every envelope site reads it through [`statement_deadline`].
//!
//! This is Control-Plane state and it never crosses a plane boundary. What
//! reaches the Data Plane is the resolved absolute [`Instant`] on
//! `Request::deadline`, the same field and the same contract as before.
//!
//! [`Instant`] is monotonic. A wall clock can step backwards under NTP and
//! will stretch or shrink a running statement's budget.

use std::time::{Duration, Instant};

use super::conn_scope::with_scope;

/// Resolve one statement's execution budget.
///
/// `statement_timeout` is the session's parsed `statement_timeout`, or `None`
/// when the session sets no limit of its own. `default_deadline_secs` is
/// `tuning.network.default_deadline_secs`, read from config by the caller so
/// this function has no way to invent a literal.
///
/// The session value wins outright when present: `default_deadline_secs` names
/// the budget that applies when nothing else does, and a client asking for
/// longer than the default gets it.
pub fn statement_budget(
    statement_timeout: Option<Duration>,
    default_deadline_secs: u64,
) -> Duration {
    statement_timeout.unwrap_or_else(|| Duration::from_secs(default_deadline_secs))
}

/// Guard installing one statement's deadline on the current connection scope.
///
/// Dropping it restores whatever was there before, so a nested statement (a
/// trigger body, a constraint sub-query) cannot leave its own budget behind for
/// the statement that follows.
pub struct StatementScope {
    previous: Option<Instant>,
}

impl Drop for StatementScope {
    fn drop(&mut self) {
        store(self.previous);
    }
}

/// Pin the deadline for the statement about to run.
///
/// The instant is fixed ONCE here, so every request the statement fans out into
/// shares it and the statement is bounded end to end rather than per hop.
///
/// Outside a connection scope (Event Plane, bootstrap, background DDL) the
/// store is a no-op and every envelope site falls back to the node default, so
/// installing the guard is always safe.
pub fn enter(statement_timeout: Option<Duration>, default_deadline_secs: u64) -> StatementScope {
    let previous = current();
    store(Some(
        Instant::now() + statement_budget(statement_timeout, default_deadline_secs),
    ));
    StatementScope { previous }
}

/// The current statement's deadline, or `None` when no statement scope is
/// installed on this task.
pub fn current() -> Option<Instant> {
    with_scope(None, |scope| scope.statement_deadline.get())
}

/// The absolute instant a Control -> Data request built right now must carry.
///
/// The running statement's deadline when one is installed, else `now` plus the
/// node's configured default. EVERY envelope construction site on a
/// client-reachable path calls this rather than adding a duration of its own.
pub fn statement_deadline(default_deadline_secs: u64) -> Instant {
    current().unwrap_or_else(|| Instant::now() + Duration::from_secs(default_deadline_secs))
}

/// Milliseconds left on the current statement, for the remote-hop
/// `ExecuteRequest.deadline_remaining_ms` field.
///
/// Saturates at 1: a hop dispatched with `0` is refused up front by the
/// receiver's deadline check, which will report a deadline the local half has
/// not actually reached yet.
pub fn statement_deadline_ms(default_deadline_secs: u64) -> u64 {
    let remaining =
        statement_deadline(default_deadline_secs).saturating_duration_since(Instant::now());
    (remaining.as_millis() as u64).max(1)
}

fn store(deadline: Option<Instant>) {
    with_scope((), |scope| scope.statement_deadline.set(deadline));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::conn_scope;

    #[test]
    fn absent_session_timeout_takes_the_configured_default() {
        let configured = nodedb_types::config::tuning::NetworkTuning::default();
        assert_eq!(
            statement_budget(None, configured.default_deadline_secs),
            Duration::from_secs(configured.default_deadline_secs),
            "the budget must come from config"
        );
    }

    #[test]
    fn a_changed_default_changes_the_budget() {
        // Pins the derivation, not the number: a literal in the dispatch path
        // will not move when the configured value does.
        assert_eq!(statement_budget(None, 7), Duration::from_secs(7));
        assert_eq!(statement_budget(None, 120), Duration::from_secs(120));
    }

    #[test]
    fn session_timeout_wins_over_the_default() {
        assert_eq!(
            statement_budget(Some(Duration::from_millis(250)), 30),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn a_session_timeout_longer_than_the_default_is_honoured() {
        // `default_deadline_secs` is a default, not a ceiling.
        assert_eq!(
            statement_budget(Some(Duration::from_secs(600)), 30),
            Duration::from_secs(600)
        );
    }

    #[tokio::test]
    async fn every_envelope_site_in_one_statement_shares_one_instant() {
        conn_scope::scoped(async {
            let _scope = enter(Some(Duration::from_secs(60)), 30);
            let first = statement_deadline(30);
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            let later = statement_deadline(30);
            assert_eq!(
                first, later,
                "a statement's fan-out must not restart the clock per hop"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn the_scope_restores_the_previous_deadline() {
        conn_scope::scoped(async {
            let _outer = enter(Some(Duration::from_secs(60)), 30);
            let outer_deadline = current().expect("outer statement installs a deadline");
            {
                let _inner = enter(Some(Duration::from_millis(1)), 30);
                assert_ne!(current(), Some(outer_deadline));
            }
            assert_eq!(
                current(),
                Some(outer_deadline),
                "a nested statement must not leave its budget behind"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn outside_a_connection_scope_the_node_default_applies() {
        assert!(current().is_none());
        let before = Instant::now();
        let deadline = statement_deadline(5);
        assert!(deadline >= before + Duration::from_secs(5));
    }
}
