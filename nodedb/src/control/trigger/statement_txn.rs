// SPDX-License-Identifier: BUSL-1.1

//! The transaction a client statement and its synchronous trigger bodies
//! share.
//!
//! A BEFORE, INSTEAD OF or SYNC AFTER body joins the transaction of the
//! statement that fired it, so the statement's write and the body's writes
//! commit together or not at all. Inside a transaction block that is the
//! client's own transaction. Outside one, a statement whose write fires such
//! a body runs in an implicit transaction: its write stages there with the
//! bodies' writes, and the transaction commits when the statement succeeds.
//!
//! The implicit transaction's source is
//! [`EventSource::ImplicitClient`]: the client's rows fire ASYNC triggers, as
//! an autocommit write's do. The bodies' rows commit under `Trigger`.
//!
//! A statement that runs in an implicit transaction for any reason but a
//! joined body runs again when its COMMIT aborts on a serialization
//! conflict ([`ImplicitRetry`]). The direct path never aborts on one,
//! so the client sees no `40001` the direct path would not give.

use std::time::{Duration, Instant};

use crate::control::security::catalog::trigger_types::{TriggerExecutionMode, TriggerTiming};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::conn_scope::scoped_system_txn;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState, statement_deadline};
use crate::control::state::SharedState;
use crate::control::system_txn::{OpenSystemTxn, SystemTxnError};
use crate::event::EventSource;

use super::registry::DmlEvent;
use super::scope::TriggerScope;

/// The first pause before an implicit transaction runs again after a
/// serialization abort. Each later pause doubles, up to [`RETRY_PAUSE_MAX`].
const RETRY_PAUSE_START: Duration = Duration::from_millis(1);

/// The longest pause between two attempts of one implicit transaction.
const RETRY_PAUSE_MAX: Duration = Duration::from_millis(64);

/// Whether a write of `event` to `collection` fires a body that joins its
/// statement's transaction: a BEFORE, INSTEAD OF or SYNC AFTER trigger.
pub fn fires_joined_body(
    state: &SharedState,
    scope: TriggerScope,
    collection: &str,
    event: DmlEvent,
) -> bool {
    state
        .trigger_registry
        .get_matching(
            scope.database_id,
            scope.tenant_id.as_u64(),
            collection,
            event,
        )
        .iter()
        .any(|trigger| match trigger.timing {
            TriggerTiming::Before | TriggerTiming::InsteadOf => true,
            TriggerTiming::After => trigger.execution_mode == TriggerExecutionMode::Sync,
        })
}

/// Whether the session `ctx` names has a transaction block open.
pub fn in_block(ctx: &DmlTxnCtx<'_>) -> bool {
    ctx.sessions.transaction_state(ctx.session_id) == TransactionState::InBlock
}

/// Run `work` on the statement's transaction.
///
/// `implicit` asks for an implicit transaction: the caller found that the
/// statement, outside a transaction block, fires a joined body. `work` then
/// runs on a private session whose transaction commits when `work` succeeds
/// and rolls back when it fails. Otherwise `work` runs on `ctx` itself.
///
/// The implicit transaction runs in its own connection slots, so its DDL
/// buffer never outlives it on the connection.
pub async fn with_statement_txn<T, E>(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    ctx: &DmlTxnCtx<'_>,
    implicit: bool,
    work: impl AsyncFnOnce(&DmlTxnCtx<'_>) -> Result<T, E>,
) -> Result<T, E>
where
    E: From<crate::Error>,
{
    with_statement_txn_lifted(state, identity, ctx, implicit, E::from, work).await
}

/// [`with_statement_txn`] for a caller whose error type takes a
/// `crate::Error` through `lift`.
pub async fn with_statement_txn_lifted<T, E>(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    ctx: &DmlTxnCtx<'_>,
    implicit: bool,
    lift: impl Fn(crate::Error) -> E,
    work: impl AsyncFnOnce(&DmlTxnCtx<'_>) -> Result<T, E>,
) -> Result<T, E> {
    if !implicit {
        return work(ctx).await;
    }
    scoped_system_txn(async {
        let txn = OpenSystemTxn::begin(state, identity.clone(), EventSource::ImplicitClient)
            .map_err(|error| lift(crate::Error::from(error)))?;
        let result = {
            let implicit_ctx = txn.txn_ctx().map_err(&lift)?;
            work(&implicit_ctx).await
        };
        match result {
            Ok(value) => {
                txn.commit()
                    .await
                    .map_err(|error| lift(crate::Error::from(error)))?;
                Ok(value)
            }
            Err(error) => {
                txn.rollback().await;
                Err(error)
            }
        }
    })
    .await
}

/// How one attempt of an implicit transaction ended.
pub enum ImplicitAttempt<T, E> {
    /// The work succeeded and COMMIT applied it.
    Committed(T),
    /// COMMIT aborted on a serialization conflict. Nothing applied.
    Conflict(SystemTxnError),
    /// Any other error. The transaction rolled back.
    Failed(E),
}

/// The pacing of one statement's implicit-transaction attempts.
///
/// A statement that runs in an implicit transaction runs again on a new one
/// when COMMIT aborts on a serialization conflict, until the statement
/// deadline. Each attempt reads on a new snapshot, so a write that committed
/// under it no longer conflicts. Nothing of a failed attempt applied. Its
/// sequence draws stay consumed, and the session's `currval` holds the last
/// attempt's.
///
/// The caller runs the attempts in its own loop: [`begin_implicit`] opens
/// one, [`finish_implicit`] commits or rolls it back, and [`Self::pause`]
/// decides whether a conflict runs again.
pub struct ImplicitRetry {
    retry: bool,
    deadline: Instant,
    pause: Duration,
}

impl ImplicitRetry {
    /// The pacing of a statement on `state`. `retry` is `false` for a
    /// statement whose writes fire a joined trigger body: a body's effects
    /// outside the transaction must not repeat, so its first abort is the
    /// statement's answer.
    pub fn new(state: &SharedState, retry: bool) -> Self {
        Self {
            retry,
            deadline: statement_deadline(state.tuning.network.default_deadline_secs),
            pause: RETRY_PAUSE_START,
        }
    }

    /// After a conflict, wait the next pause and return `true`. `false` when
    /// the statement answers the conflict: retry is off, or the pause would
    /// pass the deadline.
    pub async fn pause(&mut self) -> bool {
        if !self.retry || Instant::now() + self.pause >= self.deadline {
            return false;
        }
        tokio::time::sleep(self.pause).await;
        self.pause = self.pause.saturating_mul(2).min(RETRY_PAUSE_MAX);
        true
    }
}

/// Open one attempt's implicit transaction on a private session, under
/// [`EventSource::ImplicitClient`]. Run it inside `scoped_system_txn`, so
/// its DDL buffer never outlives it on the connection.
pub fn begin_implicit<'s>(
    state: &'s SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<OpenSystemTxn<'s>> {
    OpenSystemTxn::begin(state, identity.clone(), EventSource::ImplicitClient)
        .map_err(crate::Error::from)
}

/// Finish one attempt: commit `txn` when `result` succeeded, roll it back
/// when it failed. A COMMIT error other than a serialization conflict is
/// lifted through `lift`.
pub async fn finish_implicit<T, E>(
    txn: OpenSystemTxn<'_>,
    result: Result<T, E>,
    lift: impl Fn(crate::Error) -> E,
) -> ImplicitAttempt<T, E> {
    match result {
        Ok(value) => match txn.commit().await {
            Ok(()) => ImplicitAttempt::Committed(value),
            Err(error) if error.is_serialization() => ImplicitAttempt::Conflict(error),
            Err(error) => ImplicitAttempt::Failed(lift(crate::Error::from(error))),
        },
        Err(error) => {
            txn.rollback().await;
            ImplicitAttempt::Failed(error)
        }
    }
}
