// SPDX-License-Identifier: BUSL-1.1

//! A system transaction held open across statements.
//!
//! Each statement routes through the same staging gate an explicit client
//! transaction uses, at the statement: a stageable write lands in the
//! transaction's overlay, a read runs against base ∪ overlay, and an
//! in-transaction `MERGE`, `UPDATE ... FROM` or `INSERT ... SELECT` is
//! resolved against the overlay. A later statement therefore sees every
//! earlier statement's writes. COMMIT is the only durable apply.
//!
//! A BEFORE, INSTEAD OF or SYNC AFTER body joins its triggering statement's
//! transaction instead (see [`super::joined`]): it stages into that session
//! behind a savepoint, and the statement's COMMIT commits it.

use std::sync::Arc;

use crate::control::lease::QueryLeaseScope;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::expander_stage::{
    ExpanderOutcome, route_in_tx_expander,
};
use crate::control::server::shared::session::{
    CommitOutcome, DmlTxnCtx, InTxnRoute, SavepointError, SessionId, SessionStore, commit,
    lifecycle, route_in_tx_write, savepoint_ops,
};
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::TenantId;
use crate::wal::CrossShardAppliedKey;
use nodedb_physical::physical_task::PhysicalTask;

use super::data_plane::SystemTxnDataPlane;
use super::run::{
    SystemTxnError, SystemTxnStatement, commit_abort_error, dispatch_read, dispatch_staged,
    refuse_unbufferable, staging_error,
};
use super::scope::SystemTxnScope;

/// An open system transaction. End it with [`Self::commit`] or
/// [`Self::rollback`]: both release its staging overlay. Dropped without
/// either, an owned transaction rolls back on a spawned task.
pub struct OpenSystemTxn<'s> {
    pub(super) state: &'s SharedState,
    /// `None` once commit or rollback took it.
    pub(super) session: Option<TxnSession<'s>>,
    pub(super) identity: AuthenticatedIdentity,
    pub(super) event_source: EventSource,
    /// The cross-shard request this transaction applies, with the vShard the
    /// request addresses.
    applied_key: Option<(CrossShardAppliedKey, u32)>,
}

/// The session a system transaction stages into.
pub(super) enum TxnSession<'s> {
    /// A private session the transaction owns. COMMIT commits it.
    Owned(SystemTxnScope),
    /// The triggering statement's session. The body stages into its
    /// transaction behind `savepoint`: success releases the savepoint, failure
    /// rolls back to it. The statement's COMMIT commits the body's writes.
    Joined {
        ctx: &'s DmlTxnCtx<'s>,
        savepoint: String,
        tenant_id: TenantId,
    },
}

impl TxnSession<'_> {
    pub(super) fn sessions(&self) -> &SessionStore {
        match self {
            Self::Owned(scope) => scope.sessions(),
            Self::Joined { ctx, .. } => ctx.sessions,
        }
    }

    pub(super) fn session_id(&self) -> SessionId {
        match self {
            Self::Owned(scope) => scope.session_id(),
            Self::Joined { ctx, .. } => ctx.session_id,
        }
    }
}

pub(super) fn savepoint_error(error: SavepointError) -> crate::Error {
    match error {
        SavepointError::NoActiveTransaction => crate::Error::Internal {
            detail: "a system transaction savepoint ran outside its block".into(),
        },
        SavepointError::TransactionAborted => crate::Error::BadRequest {
            detail: "a system transaction savepoint ran in an aborted block".into(),
        },
        SavepointError::NotFound { message } => crate::Error::BadRequest { detail: message },
        SavepointError::OverlayDispatch { message } => crate::Error::Internal { detail: message },
    }
}

pub(super) fn ended() -> crate::Error {
    crate::Error::Internal {
        detail: "the system transaction already ended".into(),
    }
}

impl<'s> OpenSystemTxn<'s> {
    /// Open a transaction block on a private session. `identity` commits,
    /// rolls back, and authorizes its DDL.
    pub fn begin(
        state: &'s SharedState,
        identity: AuthenticatedIdentity,
        event_source: EventSource,
    ) -> Result<Self, SystemTxnError> {
        let scope =
            SystemTxnScope::begin(state).map_err(|source| SystemTxnError::Begin { source })?;
        Ok(Self {
            state,
            session: Some(TxnSession::Owned(scope)),
            identity,
            event_source,
            applied_key: None,
        })
    }

    /// A transaction on `session`, for [`super::joined`].
    pub(super) fn on_session(
        state: &'s SharedState,
        session: TxnSession<'s>,
        identity: AuthenticatedIdentity,
        event_source: EventSource,
    ) -> Self {
        Self {
            state,
            session: Some(session),
            identity,
            event_source,
            applied_key: None,
        }
    }

    /// Write `key` into the commit's redo record, so the cross-shard request
    /// it names is recorded as applied in the same durable write. A commit
    /// that spans vShards writes it into the record of `target_vshard`, the
    /// vShard the request addresses.
    pub fn set_applied_key(&mut self, key: CrossShardAppliedKey, target_vshard: u32) {
        self.applied_key = Some((key, target_vshard));
    }

    pub(super) fn dp(&self) -> SystemTxnDataPlane<'s> {
        SystemTxnDataPlane {
            state: self.state,
            event_source: self.event_source,
            applied_key: self.applied_key.clone(),
        }
    }

    fn scope(&self) -> crate::Result<&TxnSession<'s>> {
        self.session.as_ref().ok_or_else(ended)
    }

    /// The session this transaction runs on, for a statement router that
    /// routes its own writes and buffers its DDL through it.
    pub fn txn_ctx(&self) -> crate::Result<DmlTxnCtx<'_>> {
        let session = self.scope()?;
        Ok(DmlTxnCtx {
            sessions: session.sessions(),
            session_id: session.session_id(),
        })
    }

    /// Stage one planned statement now. A statement carrying a write the
    /// transaction cannot buffer is refused before any of its tasks runs.
    /// `index` in a returned error counts tasks within this statement.
    pub async fn stage(&self, statement: SystemTxnStatement) -> Result<(), SystemTxnError> {
        let SystemTxnStatement { tasks, lease_scope } = statement;
        let total = tasks.len();
        refuse_unbufferable(tasks.iter(), 0, total)?;
        for (index, task) in tasks.into_iter().enumerate() {
            if let Err(source) = self.stage_task(task, &lease_scope).await {
                return Err(SystemTxnError::Statement {
                    index,
                    total,
                    source,
                });
            }
        }
        Ok(())
    }

    async fn stage_task(
        &self,
        task: PhysicalTask,
        lease_scope: &Arc<QueryLeaseScope>,
    ) -> crate::Result<()> {
        let state = self.state;
        let event_source = self.event_source;
        let scope = self.scope()?;
        let sessions = scope.sessions();
        let session_id = scope.session_id();
        let buffered_before = sessions.buffered_task_count(session_id);

        let routed = match route_in_tx_expander(state, sessions, session_id, task, |staged| {
            dispatch_staged(state, staged, event_source)
        })
        .await
        {
            Ok(ExpanderOutcome::Handled(route)) => Ok(route),
            Ok(ExpanderOutcome::Passthrough(task)) => {
                route_in_tx_write(state, sessions, session_id, *task, |staged| {
                    dispatch_staged(state, staged, event_source)
                })
                .await
            }
            Err(error) => Err(error),
        };
        match routed {
            Ok(InTxnRoute::Read(task)) => dispatch_read(state, *task).await?,
            // `refuse_unbufferable` rejects these before the statement runs: a
            // write the transaction cannot buffer applies at once and survives
            // a rollback.
            Ok(InTxnRoute::Autocommit(_)) => {
                return Err(crate::Error::Internal {
                    detail: "a write a system transaction cannot buffer reached its staging gate"
                        .into(),
                });
            }
            Ok(InTxnRoute::Buffered | InTxnRoute::Staged(_)) => {}
            Err(error) => return Err(staging_error(error)),
        }

        // Retain the plan's leases on whatever this task buffered, so the
        // COMMIT fence has versions to compare. A refusal means the session
        // left the block underneath us; committing skips the fence.
        if sessions.buffered_task_count(session_id) > buffered_before
            && !sessions.attach_tx_lease_scope_since(
                session_id,
                buffered_before,
                Arc::clone(lease_scope),
            )
        {
            return Err(crate::Error::Internal {
                detail: "retaining descriptor leases for a system transaction failed".into(),
            });
        }
        // A joined body's tasks commit under `Trigger` on the Calvin path.
        if matches!(scope, TxnSession::Joined { .. }) {
            sessions.mark_body_tasks_since(session_id, buffered_before);
        }
        Ok(())
    }

    /// Hold `publishes` for the transaction's COMMIT, which writes them into
    /// its redo record. A ROLLBACK drops them.
    pub fn hold_publishes(&self, publishes: Vec<crate::wal::RedoPublish>) -> crate::Result<()> {
        let scope = self.scope()?;
        scope
            .sessions()
            .buffer_publishes(scope.session_id(), publishes);
        Ok(())
    }

    /// Union read-set entries into the transaction, so COMMIT's conflict
    /// check covers the rows they name.
    pub fn record_reads(
        &self,
        entries: Vec<crate::control::server::shared::session::read_set::ReadSetEntry>,
    ) -> crate::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let scope = self.scope()?;
        scope
            .sessions()
            .record_read_entries(scope.session_id(), entries);
        Ok(())
    }

    /// Mark a savepoint on every vShard the transaction staged to.
    pub async fn savepoint(&self, tenant_id: TenantId, name: &str) -> crate::Result<()> {
        let scope = self.scope()?;
        savepoint_ops::run_savepoint(
            scope.sessions(),
            scope.session_id(),
            tenant_id,
            &self.dp(),
            name,
        )
        .await
        .map_err(savepoint_error)
    }

    /// Rewind buffered and staged writes to a savepoint.
    pub async fn rollback_to(&self, tenant_id: TenantId, name: &str) -> crate::Result<()> {
        let scope = self.scope()?;
        savepoint_ops::run_rollback_to_savepoint(
            scope.sessions(),
            scope.session_id(),
            tenant_id,
            &self.dp(),
            name,
        )
        .await
        .map_err(savepoint_error)
    }

    /// Drop a savepoint, keeping the writes after it.
    pub fn release(&self, name: &str) -> crate::Result<()> {
        let scope = self.scope()?;
        savepoint_ops::run_release_savepoint(scope.sessions(), scope.session_id(), name)
            .map_err(savepoint_error)
    }

    /// Commit every staged and buffered write as one transaction. A write set
    /// on one vShard this node leads commits as one redo record. Any other
    /// shape commits through Calvin as one atomic cross-vShard transaction,
    /// with a redo record per vShard. An applied key rides the record of the
    /// vShard its request addresses. A joined body releases its savepoint
    /// instead: the statement's COMMIT commits it.
    pub async fn commit(mut self) -> Result<(), SystemTxnError> {
        let session = self
            .session
            .take()
            .ok_or_else(ended)
            .map_err(|source| SystemTxnError::CommitFailed { source })?;
        let scope = match session {
            TxnSession::Owned(scope) => scope,
            joined @ TxnSession::Joined { .. } => {
                return self
                    .release_joined(joined)
                    .map_err(|source| SystemTxnError::CommitFailed { source });
            }
        };
        let dp = self.dp();
        match commit::run_commit(
            scope.sessions(),
            scope.session_id(),
            &self.identity,
            self.state,
            &dp,
        )
        .await
        {
            CommitOutcome::Committed => Ok(()),
            CommitOutcome::Aborted { reason } => Err(commit_abort_error(reason)),
        }
    }

    /// Discard every staged and buffered write and release the overlay. A
    /// joined body rolls back to its savepoint instead.
    pub async fn rollback(mut self) {
        let scope = match self.session.take() {
            Some(TxnSession::Owned(scope)) => scope,
            Some(joined @ TxnSession::Joined { .. }) => {
                self.rollback_joined(joined).await;
                return;
            }
            None => return,
        };
        let dp = self.dp();
        lifecycle::run_rollback(
            scope.sessions(),
            scope.session_id(),
            &self.identity,
            self.state,
            &dp,
        )
        .await;
    }
}

impl Drop for OpenSystemTxn<'_> {
    /// A transaction dropped open (its owner's future was cancelled) rolls
    /// back on a spawned task, which releases its staging overlay. With no
    /// runtime or no owned state handle, the Data Plane's overlay lease
    /// reaper reclaims the overlay (`overlay_reap::OVERLAY_LEASE_NS`).
    /// A joined body's transaction belongs to its statement, which owns the
    /// rollback: dropping one does nothing.
    fn drop(&mut self) {
        let Some(TxnSession::Owned(scope)) = self.session.take() else {
            return;
        };
        let (Ok(runtime), Ok(state)) =
            (tokio::runtime::Handle::try_current(), self.state.self_arc())
        else {
            tracing::warn!(
                "system transaction dropped open with no runtime to roll it back; \
                 the overlay lease reaper reclaims it"
            );
            return;
        };
        let identity = self.identity.clone();
        let event_source = self.event_source;
        runtime.spawn(async move {
            let dp = SystemTxnDataPlane {
                state: &state,
                event_source,
                applied_key: None,
            };
            lifecycle::run_rollback(scope.sessions(), scope.session_id(), &identity, &state, &dp)
                .await;
        });
    }
}
