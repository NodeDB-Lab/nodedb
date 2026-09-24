// SPDX-License-Identifier: BUSL-1.1

//! Running a planned set of tasks as one system transaction.

use std::sync::Arc;

use crate::control::lease::QueryLeaseScope;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::server::shared::session::{
    AbortReason, CommitOutcome, StagingGateError, commit, lifecycle, route_in_tx_write,
};
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::TraceId;
use nodedb_physical::physical_task::PhysicalTask;

use super::data_plane::SystemTxnDataPlane;
use super::scope::SystemTxnScope;

/// Why a system transaction did not commit.
#[derive(Debug, thiserror::Error)]
pub enum SystemTxnError {
    /// The transaction block could not be opened.
    #[error("system transaction could not begin: {source}")]
    Begin {
        #[source]
        source: crate::Error,
    },

    /// A statement failed before COMMIT. Nothing durable was written: every
    /// task is buffered or staged until COMMIT, and the block is rolled back.
    #[error("system transaction statement {index} of {total} failed: {source}")]
    Statement {
        index: usize,
        total: usize,
        #[source]
        source: crate::Error,
    },

    /// COMMIT itself aborted. The transaction applied nothing.
    #[error("system transaction aborted at commit: {detail}")]
    Commit { detail: String },
}

/// Run every task as one transaction: all of them apply, or none do.
///
/// This is what makes a deferred action safe to retry. Dispatching the tasks
/// one at a time leaves a failure part-applied, and re-running a part-applied
/// action repeats whatever already landed; a transaction has no such state.
///
/// `lease_scope` is the plan's descriptor lease scope. It is retained on the
/// buffered tasks so COMMIT re-checks the versions the plan was built against
/// before it writes anything.
pub async fn run_tasks_atomically(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tasks: Vec<PhysicalTask>,
    lease_scope: Arc<QueryLeaseScope>,
    event_source: EventSource,
) -> Result<(), SystemTxnError> {
    let scope = SystemTxnScope::begin(state).map_err(|source| SystemTxnError::Begin { source })?;
    let total = tasks.len();
    let dp = SystemTxnDataPlane {
        state,
        event_source,
    };

    for (index, task) in tasks.into_iter().enumerate() {
        let buffered_before = scope.sessions().buffered_task_count(scope.session_id());
        let routed = route_in_tx_write(
            state,
            scope.sessions(),
            scope.session_id(),
            task,
            |staged| dispatch_staged(state, staged, event_source),
        )
        .await;

        if let Err(error) = routed {
            let source = staging_error(error);
            lifecycle::run_rollback(scope.sessions(), scope.session_id(), identity, state, &dp)
                .await;
            return Err(SystemTxnError::Statement {
                index,
                total,
                source,
            });
        }

        // Retain the plan's leases on whatever this task buffered, so the
        // COMMIT fence has versions to compare. A refusal here means the
        // session left the block underneath us; committing anyway would skip
        // the fence entirely.
        if scope.sessions().buffered_task_count(scope.session_id()) > buffered_before
            && !scope.sessions().attach_tx_lease_scope_since(
                scope.session_id(),
                buffered_before,
                Arc::clone(&lease_scope),
            )
        {
            lifecycle::run_rollback(scope.sessions(), scope.session_id(), identity, state, &dp)
                .await;
            return Err(SystemTxnError::Statement {
                index,
                total,
                source: crate::Error::Internal {
                    detail: "retaining descriptor leases for a system transaction failed".into(),
                },
            });
        }
    }

    match commit::run_commit(scope.sessions(), scope.session_id(), identity, state, &dp).await {
        CommitOutcome::Committed => Ok(()),
        CommitOutcome::Aborted { reason } => Err(SystemTxnError::Commit {
            detail: describe(&reason),
        }),
    }
}

/// Apply one stageable write to the transaction's overlay.
async fn dispatch_staged(
    state: &SharedState,
    task: PhysicalTask,
    event_source: EventSource,
) -> crate::Result<crate::bridge::envelope::Response> {
    dispatch_utils::dispatch_trusted_internal_write_to_data_plane(
        state,
        dispatch_utils::WriteDispatch {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            vshard_id: task.vshard_id,
            plan: task.plan,
            trace_id: TraceId::ZERO,
            event_source,
            txn_id: task.txn_id,
            wal_lsn: None,
            resolved_now_ms: None,
            minted: None,
        },
    )
    .await
}

/// Flatten a staging-gate refusal into the crate error type.
fn staging_error(error: StagingGateError) -> crate::Error {
    match error {
        StagingGateError::Dispatch(e) => e,
        StagingGateError::Rejected { code } => match code {
            Some(code) => crate::Error::DataPlane(code),
            None => crate::Error::Internal {
                detail: "a staged write was rejected without an error code".into(),
            },
        },
    }
}

/// Render a commit abort for the caller's log and retry record.
fn describe(reason: &AbortReason) -> String {
    match reason {
        AbortReason::Serialization => "serialization failure against a concurrent write".to_owned(),
        AbortReason::NoTransaction => "the transaction block was already gone".to_owned(),
        AbortReason::BatchRejected { code } => match code {
            Some(code) => format!("the data plane rejected the batch: {code:?}"),
            None => "the data plane rejected the batch".to_owned(),
        },
        AbortReason::CalvinCancelled => "the cross-shard coordinator cancelled".to_owned(),
        AbortReason::CalvinTimeout => "the cross-shard coordinator timed out".to_owned(),
        AbortReason::SchemaChanged { detail } => detail.clone(),
        AbortReason::Dispatch(e) => e.to_string(),
        AbortReason::DdlPropose(e) => e.to_string(),
    }
}
