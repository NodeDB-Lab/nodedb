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
    previous: Option<(Instant, Duration)>,
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
    let previous = with_scope(None, |scope| scope.statement_deadline.get());
    let budget = statement_budget(statement_timeout, default_deadline_secs);
    store(Some((Instant::now() + budget, budget)));
    StatementScope { previous }
}

/// The current statement's deadline, or `None` when no statement scope is
/// installed on this task.
pub fn current() -> Option<Instant> {
    with_scope(None, |scope| scope.statement_deadline.get()).map(|(at, _)| at)
}

/// The whole budget the current statement was given, or `default_deadline_secs`
/// outside a statement scope. A deadline error reports it: the time left is
/// zero by then.
pub fn statement_total_budget(default_deadline_secs: u64) -> Duration {
    with_scope(None, |scope| scope.statement_deadline.get())
        .map_or(Duration::from_secs(default_deadline_secs), |(_, budget)| {
            budget
        })
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
/// `ExecuteRequest.deadline_remaining_ms` field. See [`hop_budget_ms`].
pub fn statement_deadline_ms(default_deadline_secs: u64) -> u64 {
    hop_budget_ms(statement_deadline(default_deadline_secs))
}

/// The budget a remote hop carries for `deadline`: the milliseconds left now,
/// at least 1.
///
/// A receiver refuses a hop sent with `0` before it checks anything else. It
/// reports a deadline the local half has not reached yet.
pub fn hop_budget_ms(deadline: Instant) -> u64 {
    let remaining = deadline.saturating_duration_since(Instant::now());
    u64::try_from(remaining.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}

/// The deadline of the statement running on this task, as a value.
///
/// The one place a caller with `SharedState` reads the statement deadline:
/// the installed deadline, or now plus `tuning.network.default_deadline_secs`
/// outside a statement. Waits, retry loops and proposals that share the
/// statement's budget all take it from here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StatementDeadline(Instant);

impl StatementDeadline {
    /// The running statement's deadline, or the node default outside one.
    pub fn of_statement(state: &crate::control::state::SharedState) -> Self {
        Self::with_default(state.tuning.network.default_deadline_secs)
    }

    /// The running statement's deadline, or `default_deadline_secs` from now
    /// outside one.
    pub fn with_default(default_deadline_secs: u64) -> Self {
        Self(statement_deadline(default_deadline_secs))
    }

    /// A deadline at `instant`.
    pub fn at(instant: Instant) -> Self {
        Self(instant)
    }

    /// A deadline `budget` from now.
    pub fn after(budget: Duration) -> Self {
        Self(Instant::now() + budget)
    }

    /// The deadline as a std instant.
    pub fn instant(self) -> Instant {
        self.0
    }

    /// The deadline as a Tokio instant, for `tokio::time` waits.
    pub fn tokio_instant(self) -> tokio::time::Instant {
        tokio::time::Instant::from_std(self.0)
    }

    /// The milliseconds a remote hop carries for this deadline. See
    /// [`hop_budget_ms`].
    pub fn hop_budget_ms(self) -> u64 {
        hop_budget_ms(self.0)
    }

    /// Time left until the deadline, zero once it passed.
    pub fn remaining(self) -> Duration {
        self.0.saturating_duration_since(Instant::now())
    }
}

/// Install a fresh node-default deadline for cleanup work, such as an overlay
/// drop.
///
/// Cleanup often runs after its statement ran out of time. Under that dead
/// deadline every request it sends is refused before it runs. Dropping the
/// guard restores the statement's deadline.
pub fn enter_cleanup(default_deadline_secs: u64) -> StatementScope {
    enter(None, default_deadline_secs)
}

fn store(bound: Option<(Instant, Duration)>) {
    with_scope((), |scope| scope.statement_deadline.set(bound));
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

    /// The whole budget stays readable after the deadline passed, and a
    /// nested statement restores the outer budget.
    #[tokio::test]
    async fn the_total_budget_survives_the_deadline() {
        assert_eq!(statement_total_budget(30), Duration::from_secs(30));
        conn_scope::scoped(async {
            let _outer = enter(Some(Duration::from_millis(1)), 30);
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert_eq!(statement_total_budget(30), Duration::from_millis(1));
            {
                let _inner = enter(None, 7);
                assert_eq!(statement_total_budget(30), Duration::from_secs(7));
            }
            assert_eq!(statement_total_budget(30), Duration::from_millis(1));
        })
        .await;
    }

    #[test]
    fn a_spent_deadline_still_sends_a_one_millisecond_hop() {
        assert_eq!(hop_budget_ms(Instant::now()), 1);
        let live = hop_budget_ms(Instant::now() + Duration::from_secs(10));
        assert!(live > 9_000 && live <= 10_000, "budget {live} ms");
    }

    /// Cleanup after a timed-out statement runs on the node default, and the
    /// statement's deadline returns once cleanup ends.
    #[tokio::test]
    async fn cleanup_gets_a_fresh_deadline_after_its_statement_timed_out() {
        conn_scope::scoped(async {
            let _statement = enter(Some(Duration::from_millis(1)), 30);
            let dead = current().expect("the statement installs a deadline");
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert!(dead <= Instant::now());
            {
                let _cleanup = enter_cleanup(30);
                let fresh = statement_deadline(30);
                assert!(fresh >= Instant::now() + Duration::from_secs(29));
                assert!(statement_deadline_ms(30) > 29_000);
            }
            assert_eq!(current(), Some(dead));
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
