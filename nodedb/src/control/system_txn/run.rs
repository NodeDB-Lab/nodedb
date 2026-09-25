// SPDX-License-Identifier: BUSL-1.1

//! Running a planned set of tasks as one system transaction.

use std::sync::Arc;

use crate::control::lease::QueryLeaseScope;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::identity::{Permission, required_permission};
use crate::control::server::dispatch_utils;
use crate::control::server::shared::session::{
    AbortReason, CommitOutcome, InTxnRoute, StagingGateError, commit, lifecycle, route_in_tx_write,
};
use crate::control::server::shared::write_admission::plan_requires_txn_buffering;
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

    /// COMMIT itself aborted. The transaction applied nothing. `code` is the
    /// Data Plane's verdict when one decided the abort.
    #[error("system transaction aborted at commit: {detail}")]
    Commit {
        detail: String,
        code: Option<Box<crate::bridge::envelope::ErrorCode>>,
    },
}

impl From<SystemTxnError> for crate::Error {
    fn from(error: SystemTxnError) -> Self {
        match error {
            SystemTxnError::Begin { source } | SystemTxnError::Statement { source, .. } => source,
            SystemTxnError::Commit {
                code: Some(code), ..
            } => crate::Error::DataPlane(*code),
            SystemTxnError::Commit { detail, code: None } => crate::Error::Internal { detail },
        }
    }
}

/// One planned statement of a system transaction, with the descriptor
/// leases its plan was built against.
pub struct SystemTxnStatement {
    pub tasks: Vec<PhysicalTask>,
    pub lease_scope: Arc<QueryLeaseScope>,
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
    run_statements_atomically(
        state,
        identity,
        vec![SystemTxnStatement { tasks, lease_scope }],
        event_source,
    )
    .await
}

/// Run every statement's tasks as one transaction: all of them apply, or
/// none do. Each statement's leases stay on the tasks it buffered, so COMMIT
/// re-checks every version any statement was planned against.
///
/// A task that is neither buffered nor a read, such as a data write the
/// transaction cannot buffer or index DDL, applies at once and survives a
/// rollback, so a statement carrying one is refused before BEGIN with
/// [`crate::Error::NotInTransactionBlock`]. A read runs at once against the
/// transaction's overlay, and its error fails the transaction.
pub async fn run_statements_atomically(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    statements: Vec<SystemTxnStatement>,
    event_source: EventSource,
) -> Result<(), SystemTxnError> {
    let total: usize = statements
        .iter()
        .map(|statement| statement.tasks.len())
        .sum();
    if let Some((index, task)) = statements
        .iter()
        .flat_map(|statement| statement.tasks.iter())
        .enumerate()
        .find(|(_, task)| !runs_in_a_system_transaction(&task.plan))
    {
        return Err(SystemTxnError::Statement {
            index,
            total,
            source: crate::Error::NotInTransactionBlock {
                statement: match task.plan.collection() {
                    Some(collection) => format!("a non-transactional write to '{collection}'"),
                    None => "a non-transactional write".to_owned(),
                },
            },
        });
    }
    let scope = SystemTxnScope::begin(state).map_err(|source| SystemTxnError::Begin { source })?;
    let dp = SystemTxnDataPlane {
        state,
        event_source,
    };
    let tasks = statements.into_iter().flat_map(|statement| {
        let lease_scope = statement.lease_scope;
        statement
            .tasks
            .into_iter()
            .map(move |task| (task, Arc::clone(&lease_scope)))
    });

    for (index, (task, lease_scope)) in tasks.enumerate() {
        let buffered_before = scope.sessions().buffered_task_count(scope.session_id());
        let routed = route_in_tx_write(
            state,
            scope.sessions(),
            scope.session_id(),
            task,
            |staged| dispatch_staged(state, staged, event_source),
        )
        .await;

        let read = match routed {
            Ok(InTxnRoute::Read(task)) => dispatch_read(state, *task).await.err(),
            // Refused before BEGIN by `runs_in_a_system_transaction`: a write
            // the transaction cannot buffer applies at once and survives a
            // rollback.
            Ok(InTxnRoute::Autocommit(_)) => Some(crate::Error::Internal {
                detail: "a write a system transaction cannot buffer reached its staging gate"
                    .into(),
            }),
            Ok(InTxnRoute::Buffered | InTxnRoute::Staged(_)) => None,
            Err(error) => Some(staging_error(error)),
        };
        if let Some(source) = read {
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
            code: abort_code(&reason).map(Box::new),
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

/// Whether a task can run inside a system transaction: it is buffered for
/// COMMIT, or it only reads.
fn runs_in_a_system_transaction(plan: &nodedb_physical::physical_plan::PhysicalPlan) -> bool {
    plan_requires_txn_buffering(plan)
        || matches!(
            required_permission(plan),
            Permission::Read | Permission::Monitor | Permission::Execute
        )
}

/// Run one read of the transaction against its overlay. The rows are not
/// kept: a system transaction answers no rows. Its error fails the
/// transaction.
async fn dispatch_read(state: &SharedState, task: PhysicalTask) -> crate::Result<()> {
    let response = dispatch_utils::dispatch_to_data_plane_with_txn(
        state,
        task.tenant_id,
        task.database_id,
        task.vshard_id,
        task.plan,
        TraceId::ZERO,
        task.txn_id,
    )
    .await?;
    dispatch_utils::reject_data_plane_error(&response)
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

/// The Data-Plane verdict a commit abort carries, so a caller surfaces the
/// same class a client COMMIT would.
fn abort_code(reason: &AbortReason) -> Option<crate::bridge::envelope::ErrorCode> {
    match reason {
        AbortReason::BatchRejected { code } => code.clone(),
        AbortReason::Serialization | AbortReason::SchemaChanged { .. } => {
            Some(crate::bridge::envelope::ErrorCode::ConflictRetry)
        }
        AbortReason::NoTransaction
        | AbortReason::CalvinCancelled
        | AbortReason::CalvinTimeout
        | AbortReason::Dispatch(_)
        | AbortReason::DdlPropose(_) => None,
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
