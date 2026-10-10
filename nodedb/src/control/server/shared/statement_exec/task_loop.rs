// SPDX-License-Identifier: BUSL-1.1

//! The per-task loop of a planned statement.
//!
//! A statement in a transaction (the client's block, or the implicit
//! transaction a statement runs in when [`statement_needs_implicit_txn`]
//! flags it) routes each task through the shared `txn_route`, the route
//! pgwire takes too: the write's trigger bodies join the transaction, a
//! shadowed clone takes its copy-on-write steps, and the write stages. An
//! autocommit statement dispatches each task directly.
//!
//! [`statement_needs_implicit_txn`]: crate::control::server::shared::txn_route::statement_needs_implicit_txn

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Status};
use crate::control::lease::QueryLeaseScope;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::sequence::SessionSequenceAccess;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{describe_plan, replaced_write_outcome};
use crate::control::server::shared::cluster_array_dispatch::{
    ClusterArrayShaped, execute_cluster_array,
};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::server::shared::session::staging_gate::StagingGateError;
use crate::control::server::shared::session::{
    DmlTxnCtx, ResponseReads, record_reads_for_response,
};
use crate::control::server::shared::txn_route::{
    StatementEvents, TxnTaskContext, TxnTaskOutcome, route_txn_task,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::answer::{StatementAnswer, StatementFold};
use super::dispatch::{DispatchedTask, authorize_one_task, dispatch_statement_task};
use super::error::StatementError;

/// Who runs a statement, and what its answer shapes against.
pub(crate) struct StatementExec<'a> {
    pub(crate) state: &'a Arc<SharedState>,
    pub(crate) identity: &'a AuthenticatedIdentity,
    /// The request's auth scope: RLS context, redaction, metering and quota.
    pub(crate) scope: &'a RequestAuthScope<'a>,
    /// The statement's announced output columns, when it announced any.
    pub(crate) output_schema: Option<&'a OutputSchema>,
    pub(crate) database_id: DatabaseId,
    /// The sequence access the statement's Control-Plane computed columns
    /// read: the registry, plus the session's `currval` map when it has one.
    pub(crate) sequences: &'a SessionSequenceAccess<'a>,
}

impl StatementExec<'_> {
    pub(crate) fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id
    }
}

/// A planned statement's tasks and what they carry into dispatch.
pub(crate) struct PlannedStatement {
    pub(crate) tasks: Vec<PhysicalTask>,
    /// The statement's descriptor leases, kept on every task it buffers.
    pub(crate) plan_lease_scope: Arc<QueryLeaseScope>,
    /// The row images the statement's cross-shard balances were settled
    /// from. A statement in a transaction adds them to its read set.
    pub(crate) sum_target_reads: Vec<ReadSetEntry>,
}

/// Run every task of `statement` and fold their answers into one.
/// `txn` is the statement's transaction, `None` for an autocommit statement.
///
/// The answer's tag is one [`crate::control::server::response_shape::types::StatementTag`]
/// folded over every task, the same fold pgwire renders as its command tag:
/// a count-bearing task contributes its reported count and verb, an opaque
/// task (buffered write, index or graph maintenance, computed-value payload,
/// derived balance or edge write) contributes nothing. Neither is ever
/// synthesised from the task count.
pub(crate) async fn run_statement_loop(
    exec: &StatementExec<'_>,
    statement: PlannedStatement,
    txn: Option<&DmlTxnCtx<'_>>,
) -> Result<StatementAnswer, StatementError> {
    let PlannedStatement {
        tasks,
        plan_lease_scope,
        sum_target_reads,
    } = statement;
    if let Some(txn) = txn
        && !sum_target_reads.is_empty()
    {
        txn.sessions
            .record_read_entries(txn.session_id, sum_target_reads);
    }
    let state = exec.state;
    let mut fold = StatementFold::for_tasks(&tasks);
    // Checked once rather than per task: metering is disabled by default, so
    // the per-task extraction below stays a no-op on the hot path.
    let metering_enabled = state.metering_config.enabled;
    let route_ctx = txn.map(|txn| TxnTaskContext {
        state,
        identity: exec.identity,
        auth: exec.scope.auth(),
        txn,
        lease_scope: &plan_lease_scope,
        fire_triggers: true,
    });
    let mut statement_events = StatementEvents::default();

    for task in tasks {
        if task.tenant_id != exec.tenant_id() {
            return Err(StatementError::TenantIsolation);
        }

        // Cloned before routing consumes `task`: a staged write's rows and
        // computed-value payload shape against it.
        let plan = task.plan.clone();
        // Metering covers the direct dispatch below. A staged write meters
        // itself inside the staging gate, and a buffered write meters at
        // COMMIT.
        let plan_metering_info = metering_enabled.then(|| PlanMeteringInfo::extract(&plan));

        // A spent hard quota refuses the task before it runs. The charging
        // call at the end of this loop is on the success path and never
        // refuses.
        if let Some(info) = &plan_metering_info {
            admit_quota_for_dispatch(state, exec.scope, info).map_err(StatementError::Quota)?;
        }

        // The task to dispatch below: the input task, or the one routing
        // hands back. Every other routing outcome answers the task here.
        let task = match route_ctx.as_ref() {
            Some(route) => {
                let routed = route_txn_task(
                    route,
                    task,
                    &mut statement_events,
                    |stage_task| async move {
                        dispatch_statement_task(state, exec.identity, stage_task)
                            .await
                            .map(|dispatched| dispatched.response)
                    },
                )
                .await;
                match routed {
                    Ok(TxnTaskOutcome::Dispatch(routed_task)) => *routed_task,
                    Ok(TxnTaskOutcome::Buffered) => {
                        // Applied at COMMIT: no count and no verb yet.
                        fold.tag.fold_opaque();
                        continue;
                    }
                    Ok(TxnTaskOutcome::Staged(outcome)) => {
                        fold.fold_staged(exec, &plan, outcome)?;
                        continue;
                    }
                    Ok(TxnTaskOutcome::CloneHandled(clone_response)) => {
                        fold.fold_answer(exec, &plan, clone_response.payload.as_ref())?;
                        continue;
                    }
                    Ok(TxnTaskOutcome::InsteadOf) => {
                        match replaced_write_outcome(describe_plan(&plan)) {
                            Some(outcome) => fold.fold_dml(&plan, outcome)?,
                            None => fold.tag.fold_opaque(),
                        }
                        continue;
                    }
                    Err(StagingGateError::Dispatch(error)) => {
                        return Err(StatementError::Error(error));
                    }
                    Err(StagingGateError::Rejected { code }) => {
                        return Err(StatementError::Rejected(code));
                    }
                }
            }
            None => task,
        };

        // `ClusterArray` plans run entirely on the Control Plane through the
        // `ArrayCoordinator`, never through the SPSC bridge or the trigger/DML
        // machinery. A write in a transaction reshapes into per-shard
        // `ArrayOp` tasks inside the staging gate above, so only reads
        // (`Slice`/`Agg`) and autocommit `Put`/`Delete` arrive here. Metering
        // is not applied here, matching pgwire's `ClusterArray` short-circuit.
        if matches!(task.plan, PhysicalPlan::ClusterArray(_)) {
            let authorized =
                authorize_one_task(state, exec.identity, &task).map_err(StatementError::Error)?;
            match execute_cluster_array(state, exec.scope.auth(), authorized, exec.output_schema)
                .await
                .map_err(StatementError::Error)?
            {
                ClusterArrayShaped::Rows(shaped) => {
                    fold.fold_shaped(shaped);
                }
                ClusterArrayShaped::Affected(outcome) => fold.fold_dml(&plan, outcome)?,
            }
            continue;
        }

        let task_vshard = task.vshard_id;
        let task_database_id = task.database_id;
        let DispatchedTask {
            response: task_response,
            shard_watermarks,
            distributed_reads,
        } = dispatch_statement_task(state, exec.identity, task)
            .await
            .map_err(StatementError::Error)?;

        // Track reads for snapshot-isolation / cross-shard conflict detection
        // into the statement's transaction. Recorded BEFORE the error
        // short-circuit so an absent-key point read (a `NotFound` from the
        // Data Plane) is captured too: a "not found" is a validatable phantom
        // observation. A multi-core fan read records one entry per
        // participating shard from the gather's per-shard watermarks. A
        // single read falls back to its one watermark.
        let records_read = task_response.status == Status::Ok
            || task_response.error_code.as_deref() == Some(&ErrorCode::NotFound);
        if records_read && let Some(txn) = txn {
            let watermarks = if shard_watermarks.is_empty() {
                vec![(task_vshard, task_response.watermark_lsn)]
            } else {
                shard_watermarks
            };
            record_reads_for_response(
                state,
                txn.sessions,
                txn.session_id,
                exec.tenant_id(),
                ResponseReads {
                    plan: &plan,
                    watermarks: &watermarks,
                    read_version_lsn: task_response.read_version_lsn,
                    found: task_response.status == Status::Ok,
                    distributed_reads: &distributed_reads,
                    read_lsn_vshard: task_vshard,
                },
            )
            .await;
        }

        if task_response.status == Status::Error {
            return Err(StatementError::Response(Box::new(task_response)));
        }

        // TRUNCATE RESTART IDENTITY. Autocommit only: a buffered truncate
        // restarts its sequences at COMMIT.
        if let Some((collection, true)) = plan.truncate_target() {
            state.sequence_registry.restart_sequences_for_collection(
                task_database_id.as_u64(),
                exec.tenant_id().as_u64(),
                collection.as_str(),
            );
        }

        fold.last_lsn = task_response.watermark_lsn.as_u64();
        let task_rows = fold.fold_answer(exec, &plan, task_response.payload.as_ref())?;

        // Metered once per successfully dispatched task.
        if let Some(info) = &plan_metering_info {
            meter_dispatch(state, exec.scope, info, task_rows);
        }
    }

    if let Some(route) = route_ctx.as_ref() {
        statement_events
            .fire(route)
            .await
            .map_err(StatementError::Error)?;
    }

    Ok(fold.finish())
}

/// Run a statement in an implicit transaction: it commits when every task
/// succeeds, and rolls back when one fails. `client` is the session the
/// statement arrived on. The transaction runs on a private session of its
/// own, so a transport with no session passes a detached one.
pub(crate) async fn run_implicit_statement(
    exec: &StatementExec<'_>,
    statement: PlannedStatement,
    client: &DmlTxnCtx<'_>,
) -> Result<StatementAnswer, StatementError> {
    crate::control::trigger::statement_txn::with_statement_txn_lifted(
        exec.state,
        exec.identity,
        client,
        true,
        StatementError::Error,
        async |txn: &DmlTxnCtx<'_>| run_statement_loop(exec, statement, Some(txn)).await,
    )
    .await
}
