// SPDX-License-Identifier: BUSL-1.1

//! The budget a Calvin request carries to the sequencer leader.
//!
//! A Calvin submit belongs to the statement that issued it. The sequencer
//! leader works on it only as long as the statement waits. The budget is
//! what is left of the statement's deadline, never the node default.

use std::time::Duration;

use crate::bridge::envelope::ErrorCode;
use crate::control::state::SharedState;
use crate::control::wal_replication::statement_propose_deadline;

/// The milliseconds left to `deadline`, for a request about to go out.
///
/// A spent budget refuses the request before it goes out. Nothing was
/// submitted, so the refusal is definite.
pub(crate) fn budget_ms_until(deadline: tokio::time::Instant) -> crate::Result<u64> {
    match nodedb_cluster::rpc_codec::remaining_budget_ms(deadline) {
        0 => Err(crate::Error::DataPlane(ErrorCode::ExpiredBeforeExecution)),
        ms => Ok(ms),
    }
}

/// The milliseconds left on the running statement, or on the node default
/// outside a statement.
pub(crate) fn statement_budget_ms(state: &SharedState) -> crate::Result<u64> {
    budget_ms_until(statement_propose_deadline(state))
}

/// [`statement_budget_ms`] as a wait bound for the local half of the request.
pub(crate) fn statement_budget(state: &SharedState) -> crate::Result<Duration> {
    statement_budget_ms(state).map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::control::server::shared::session::{conn_scope, deadline, statement_deadline};

    /// A statement with 100 ms left forwards at most 100 ms, under a node
    /// default of 30 s.
    #[tokio::test]
    async fn the_budget_ends_within_the_statement_deadline() {
        conn_scope::scoped(async {
            let _statement = deadline::enter(Some(Duration::from_millis(100)), 30);
            let at = tokio::time::Instant::from_std(statement_deadline(30));
            let budget = budget_ms_until(at).expect("a live budget");
            assert!(
                budget <= 100,
                "budget {budget} ms exceeds the statement's 100 ms"
            );
            assert!(budget > 0);
        })
        .await;
    }

    /// A spent statement sends nothing. Its refusal is definite.
    #[tokio::test]
    async fn a_spent_statement_is_refused_before_it_goes_out() {
        let error =
            budget_ms_until(tokio::time::Instant::now()).expect_err("a spent budget is refused");
        let crate::Error::DataPlane(code) = &error else {
            panic!("expected a Data-Plane verdict, got {error:?}");
        };
        assert_eq!(code, &ErrorCode::ExpiredBeforeExecution);
        assert!(crate::control::server::dispatch_utils::write_definitely_not_applied(code));
    }
}
