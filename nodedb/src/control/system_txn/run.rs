// SPDX-License-Identifier: BUSL-1.1

//! Running a planned set of tasks as one system transaction.

use std::sync::Arc;

use crate::control::lease::QueryLeaseScope;
use crate::control::local_dispatch;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::identity::{Permission, required_permission};
use crate::control::server::dispatch_utils;
use crate::control::server::shared::session::conn_scope::scoped_system_txn;
use crate::control::server::shared::session::{AbortReason, StagingGateError};
use crate::control::server::shared::write_admission::plan_requires_txn_buffering;
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::TraceId;
use nodedb_physical::physical_task::PhysicalTask;

use super::live::OpenSystemTxn;

/// Why a system transaction did not commit.
#[derive(Debug, thiserror::Error)]
pub enum SystemTxnError {
    /// The transaction block did not open.
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

    /// COMMIT aborted on a dispatch or DDL-propose error. The transaction
    /// applied nothing. `source` keeps its own class.
    #[error("system transaction aborted at commit: {source}")]
    CommitFailed {
        #[source]
        source: crate::Error,
    },
}

impl From<SystemTxnError> for crate::Error {
    fn from(error: SystemTxnError) -> Self {
        match error {
            SystemTxnError::Begin { source }
            | SystemTxnError::Statement { source, .. }
            | SystemTxnError::CommitFailed { source } => source,
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
///
/// `applied_key` is the key the commit records with its writes, with the
/// vShard whose redo record carries it. A DEFINE EVENT action commits its
/// source event's key, so a later owner that fires the event again finds it
/// applied.
pub async fn run_tasks_atomically(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tasks: Vec<PhysicalTask>,
    lease_scope: Arc<QueryLeaseScope>,
    event_source: EventSource,
    applied_key: Option<(crate::wal::CrossShardAppliedKey, u32)>,
) -> Result<(), SystemTxnError> {
    run_statements_keyed(
        state,
        identity,
        vec![SystemTxnStatement { tasks, lease_scope }],
        event_source,
        applied_key,
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
    run_statements_keyed(state, identity, statements, event_source, None).await
}

/// [`run_statements_atomically`] whose commit records `applied_key`.
async fn run_statements_keyed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    statements: Vec<SystemTxnStatement>,
    event_source: EventSource,
    applied_key: Option<(crate::wal::CrossShardAppliedKey, u32)>,
) -> Result<(), SystemTxnError> {
    let total: usize = statements
        .iter()
        .map(|statement| statement.tasks.len())
        .sum();
    refuse_unbufferable(
        statements
            .iter()
            .flat_map(|statement| statement.tasks.iter()),
        0,
        total,
    )?;
    scoped_system_txn(async move {
        let mut txn = OpenSystemTxn::begin(state, identity.clone(), event_source)?;
        if let Some((key, target_vshard)) = applied_key {
            txn.set_applied_key(key, target_vshard);
        }
        let mut first_task = 0;
        for statement in statements {
            let count = statement.tasks.len();
            if let Err(error) = txn.stage(statement).await {
                txn.rollback().await;
                return Err(match error {
                    SystemTxnError::Statement { index, source, .. } => SystemTxnError::Statement {
                        index: first_task + index,
                        total,
                        source,
                    },
                    other => other,
                });
            }
            first_task += count;
        }
        txn.commit().await
    })
    .await
}

/// Refuse the first task that can run in no system transaction: a write the
/// transaction cannot buffer applies at once and survives a rollback.
/// `first_index` numbers the first task; `total` is the transaction's count.
pub(super) fn refuse_unbufferable<'t>(
    tasks: impl Iterator<Item = &'t PhysicalTask>,
    first_index: usize,
    total: usize,
) -> Result<(), SystemTxnError> {
    for (offset, task) in tasks.enumerate() {
        if !runs_in_a_system_transaction(&task.plan) {
            return Err(SystemTxnError::Statement {
                index: first_index + offset,
                total,
                source: crate::Error::NotInTransactionBlock {
                    statement: crate::control::server::shared::session::staging_gate::non_transactional_write(
                        &task.plan,
                    ),
                },
            });
        }
    }
    Ok(())
}

/// The error a commit abort answers with. A dispatch or DDL-propose error
/// keeps its own class. Every other abort carries its Data-Plane verdict
/// when one decided it.
pub(super) fn commit_abort_error(reason: AbortReason) -> SystemTxnError {
    match reason {
        AbortReason::Dispatch(source) | AbortReason::DdlPropose(source) => {
            SystemTxnError::CommitFailed { source }
        }
        reason @ (AbortReason::Serialization
        | AbortReason::NoTransaction
        | AbortReason::BatchRejected { .. }
        | AbortReason::CalvinCancelled
        | AbortReason::CalvinTimeout
        | AbortReason::SchemaChanged { .. }) => SystemTxnError::Commit {
            detail: describe(&reason),
            code: abort_code(&reason).map(Box::new),
        },
    }
}

/// Apply one stageable write to the transaction's overlay.
pub(super) async fn dispatch_staged(
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
            system_ordinal: None,
        },
    )
    .await
}

/// Whether a task can run inside a system transaction: it is buffered for
/// COMMIT, or it only reads.
pub(super) fn runs_in_a_system_transaction(
    plan: &nodedb_physical::physical_plan::PhysicalPlan,
) -> bool {
    plan_requires_txn_buffering(plan)
        || matches!(
            required_permission(plan),
            Permission::Read | Permission::Monitor | Permission::Execute
        )
}

/// Run one read of the transaction against its overlay. The rows are not
/// kept: a system transaction answers no rows. Its error fails the
/// transaction.
pub(super) async fn dispatch_read(state: &SharedState, task: PhysicalTask) -> crate::Result<()> {
    // A cluster array read has no Data-Plane handler. This node's array
    // coordinator gathers it from the shards that own its cells, folding in
    // the transaction's staged cells.
    if crate::control::server::shared::cluster_array_dispatch::is_cluster_array(&task.plan) {
        let shared = state.self_arc()?;
        crate::control::server::shared::cluster_array_dispatch::run_trusted_cluster_array(
            &shared, task,
        )
        .await?;
        return Ok(());
    }
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
    local_dispatch::reject_data_plane_error(&response)
}

/// Flatten a staging-gate refusal into the crate error type.
pub(super) fn staging_error(error: StagingGateError) -> crate::Error {
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
/// same class a client COMMIT does.
fn abort_code(reason: &AbortReason) -> Option<crate::bridge::envelope::ErrorCode> {
    match reason {
        AbortReason::BatchRejected { code } => code.clone(),
        AbortReason::Serialization | AbortReason::SchemaChanged { .. } => {
            Some(crate::bridge::envelope::ErrorCode::ConflictRetry)
        }
        // The cross-shard coordinator ran out of time or was cancelled at
        // its deadline: the deadline class, `57014`.
        AbortReason::CalvinCancelled | AbortReason::CalvinTimeout => {
            Some(crate::bridge::envelope::ErrorCode::DeadlineExceeded)
        }
        AbortReason::NoTransaction | AbortReason::Dispatch(_) | AbortReason::DdlPropose(_) => None,
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
