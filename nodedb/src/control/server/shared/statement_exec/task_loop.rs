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
//! A request of several statements runs as one task set. Each task folds
//! into the answer of the statement the statement map names for it.
//!
//! [`statement_needs_implicit_txn`]: crate::control::server::shared::txn_route::statement_needs_implicit_txn

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Status};
use crate::control::lease::QueryLeaseScope;
use crate::control::planner::calvin::node_delete_txn::derives_node_delete_reads;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::sequence::SessionSequenceAccess;
use crate::control::sequence::error_map::sequence_error_to_error;
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
    StatementEvents, TxnTaskContext, TxnTaskOutcome, route_txn_task, statement_fires_joined_body,
};
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::answer::{RequestAnswer, RequestFold};
use super::dispatch::{DispatchedTask, authorize_one_task, dispatch_statement_task};
use super::error::StatementError;
use super::statement_map::StatementMap;

/// Who runs a request, and what its answer shapes against.
pub(crate) struct StatementExec<'a> {
    pub(crate) state: &'a Arc<SharedState>,
    pub(crate) identity: &'a AuthenticatedIdentity,
    /// The request's auth scope: RLS context, redaction, metering and quota.
    pub(crate) scope: &'a RequestAuthScope<'a>,
    /// Each statement's announced output columns. Index `k` is statement
    /// `k`, one entry per statement of the request. A statement with no
    /// entry announced none.
    pub(crate) schemas: &'a [OutputSchema],
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
    /// The statement of each task in `tasks`, one entry per task.
    pub(crate) statement_map: StatementMap,
    /// The statement's descriptor leases, kept on every task it buffers.
    pub(crate) plan_lease_scope: Arc<QueryLeaseScope>,
    /// The row images the statement's cross-shard balances were settled
    /// from. A statement in a transaction adds them to its read set.
    pub(crate) sum_target_reads: Vec<ReadSetEntry>,
}

/// Run every task of `statement` and fold each task's answer into the
/// answer of the statement it belongs to. `txn` is the request's
/// transaction, `None` for an autocommit request.
///
/// Each statement's tag is one [`crate::control::server::response_shape::types::StatementTag`]
/// folded over that statement's own tasks. pgwire renders the same fold as
/// its command tag. A count-bearing task contributes its reported count and
/// verb. An opaque task contributes nothing: a buffered write, index or
/// graph maintenance, a computed-value payload, a derived balance or edge
/// write. Neither is ever synthesised from the task count. Each statement's
/// statement triggers fire once, after that statement's last task. A
/// statement's derived tasks sit at the end of the set, so its triggers fire
/// after every later statement's tasks.
///
/// In a request of several statements, an error names the statement that
/// raised it.
pub(crate) async fn run_statement_loop(
    exec: &StatementExec<'_>,
    statement: PlannedStatement,
    txn: Option<&DmlTxnCtx<'_>>,
) -> Result<RequestAnswer, StatementError> {
    let PlannedStatement {
        tasks,
        statement_map,
        plan_lease_scope,
        sum_target_reads,
    } = statement;
    statement_map.debug_check_covers(&tasks);
    if let Some(txn) = txn
        && !sum_target_reads.is_empty()
    {
        txn.sessions
            .record_read_entries(txn.session_id, sum_target_reads);
    }
    let state = exec.state;
    let mut request = RequestFold::for_tasks(&tasks, &statement_map, exec.schemas.len())?;
    let route_ctx = txn.map(|txn| TxnTaskContext {
        state,
        identity: exec.identity,
        auth: exec.scope.auth(),
        txn,
        lease_scope: &plan_lease_scope,
        fire_triggers: true,
    });
    // One event set per statement: each statement fires its own SYNC AFTER
    // STATEMENT bodies once, after its own last task.
    let mut statement_events: Vec<StatementEvents> = (0..request.statement_count())
        .map(|_| StatementEvents::default())
        .collect();

    for (index, task) in tasks.into_iter().enumerate() {
        let Some(events) = request
            .statement_of_task(index)
            .and_then(|statement| statement_events.get_mut(statement))
        else {
            return Err(StatementError::Error(crate::Error::Internal {
                detail: format!("task {index} has no statement event set"),
            }));
        };
        let ran = run_task(
            exec,
            txn,
            route_ctx.as_ref(),
            events,
            &mut request,
            index,
            task,
        )
        .await;
        if let Err(error) = ran {
            return Err(request.in_statement(index, error));
        }
        if let Some(route) = route_ctx.as_ref()
            && let Some(ended) = request.statement_ending_at(index)
            && let Some(events) = statement_events.get_mut(ended)
        {
            let fired = std::mem::take(events).fire(route).await;
            if let Err(error) = fired {
                return Err(request.in_statement(index, StatementError::Error(error)));
            }
        }
    }

    Ok(request.finish())
}

/// Run task `index` of a request and fold its answer into the fold of its
/// statement. `route_ctx` is the request's transaction route, `None` for an
/// autocommit request. `statement_events` is the event set of the task's
/// statement.
async fn run_task(
    exec: &StatementExec<'_>,
    txn: Option<&DmlTxnCtx<'_>>,
    route_ctx: Option<&TxnTaskContext<'_>>,
    statement_events: &mut StatementEvents,
    request: &mut RequestFold,
    index: usize,
    task: PhysicalTask,
) -> Result<(), StatementError> {
    if task.tenant_id != exec.tenant_id() {
        return Err(StatementError::TenantIsolation);
    }
    let state = exec.state;
    let fold = request.fold_of(index)?;

    // Cloned before routing consumes `task`: a staged write's rows and
    // computed-value payload shape against it.
    let plan = task.plan.clone();
    // Metering covers the direct dispatch below. A staged write meters
    // itself inside the staging gate, and a buffered write meters at
    // COMMIT. Metering is disabled by default, so the extraction stays a
    // no-op on the hot path.
    let plan_metering_info = state
        .metering_config
        .enabled
        .then(|| PlanMeteringInfo::extract(&plan));

    // A spent hard quota refuses the task before it runs. The charging
    // call at the end of this function is on the success path and never
    // refuses.
    if let Some(info) = &plan_metering_info {
        admit_quota_for_dispatch(state, exec.scope, info).map_err(StatementError::Quota)?;
    }

    // The task to dispatch below: the input task, or the one routing
    // hands back. Every other routing outcome answers the task here.
    let task = match route_ctx {
        Some(route) => {
            let routed = route_txn_task(route, task, statement_events, |stage_task| async move {
                dispatch_statement_task(state, exec.identity, stage_task)
                    .await
                    .map(|dispatched| dispatched.response)
            })
            .await;
            match routed {
                Ok(TxnTaskOutcome::Dispatch(routed_task)) => *routed_task,
                Ok(TxnTaskOutcome::Buffered) => {
                    // Applied at COMMIT: no count and no verb yet.
                    fold.tag.fold_opaque();
                    return Ok(());
                }
                Ok(TxnTaskOutcome::Staged(outcome)) => {
                    return fold.fold_staged(exec, &plan, outcome).await;
                }
                Ok(TxnTaskOutcome::CloneHandled(clone_response)) => {
                    fold.fold_answer(exec, &plan, clone_response.payload.as_ref())
                        .await?;
                    return Ok(());
                }
                Ok(TxnTaskOutcome::InsteadOf) => {
                    match replaced_write_outcome(describe_plan(&plan)) {
                        Some(outcome) => fold.fold_dml(&plan, outcome)?,
                        None => fold.tag.fold_opaque(),
                    }
                    return Ok(());
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
    // `ArrayOp` tasks inside the staging gate above. So only reads
    // (`Slice`/`Agg`) and autocommit `Put`/`Delete` arrive here. Metering
    // is not applied here, matching pgwire's `ClusterArray` short-circuit.
    if matches!(task.plan, PhysicalPlan::ClusterArray(_)) {
        let authorized =
            authorize_one_task(state, exec.identity, &task).map_err(StatementError::Error)?;
        match execute_cluster_array(state, exec.scope.auth(), authorized, fold.projection(exec))
            .await
            .map_err(StatementError::Error)?
        {
            ClusterArrayShaped::Rows(shaped) => {
                fold.fold_shaped(shaped);
            }
            ClusterArrayShaped::Affected(outcome) => fold.fold_dml(&plan, outcome)?,
        }
        return Ok(());
    }

    let task_database_id = task.database_id;
    let DispatchedTask {
        response: task_response,
        distributed_reads,
    } = dispatch_statement_task(state, exec.identity, task)
        .await
        .map_err(StatementError::Error)?;

    // Track reads for commit-time conflict detection into the request's
    // transaction. Recorded BEFORE the error short-circuit so an absent-key
    // point read (a `NotFound` from the Data Plane) is captured too: a "not
    // found" is a validatable phantom observation.
    let records_read = task_response.status == Status::Ok
        || task_response.error_code.as_deref() == Some(&ErrorCode::NotFound);
    if records_read && let Some(txn) = txn {
        record_reads_for_response(
            state,
            txn.sessions,
            txn.session_id,
            exec.tenant_id(),
            ResponseReads {
                plan: &plan,
                read_vshards: &task_response.read_vshards,
                found: task_response.status == Status::Ok,
                distributed_reads: &distributed_reads,
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
        state
            .sequence_registry
            .restart_sequences_for_collection(
                task_database_id.as_u64(),
                exec.tenant_id().as_u64(),
                collection.as_str(),
            )
            .map_err(|error| {
                StatementError::Error(sequence_error_to_error(collection.as_str(), error))
            })?;
    }

    let task_rows = fold
        .fold_answer(exec, &plan, task_response.payload.as_ref())
        .await?;
    request.last_lsn = task_response.watermark_lsn.as_u64();

    // Metered once per successfully dispatched task.
    if let Some(info) = &plan_metering_info {
        meter_dispatch(state, exec.scope, info, task_rows);
    }
    Ok(())
}

/// Whether a request holds a read, or a delete that reads the edges of the
/// nodes it deletes when it stages.
fn request_reads(state: &SharedState, tasks: &[PhysicalTask]) -> crate::Result<bool> {
    for task in tasks {
        if !plan_is_write(&task.plan) || derives_node_delete_reads(state, task)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Run a request in an implicit transaction: it commits when every task
/// succeeds, and rolls back when one fails. `client` is the session the
/// request arrived on. The transaction runs on a private session of its
/// own, so a transport with no session passes a detached one.
///
/// A request that holds a read takes the transaction's snapshot before its
/// first task. Its reads join the read set, and COMMIT checks each one at
/// that snapshot. A delete of nodes with edges reads those edges when it
/// stages, so it takes the snapshot too. A request of other writes takes no
/// snapshot.
pub(crate) async fn run_implicit_statement(
    exec: &StatementExec<'_>,
    mut statement: PlannedStatement,
    client: &DmlTxnCtx<'_>,
) -> Result<RequestAnswer, StatementError> {
    // A derived task (a balance move, an implicit edge) runs right after its
    // statement's own tasks, so a later statement reads what it wrote.
    statement.tasks = statement
        .statement_map
        .group_by_statement(std::mem::take(&mut statement.tasks));
    let cuts_at_commit = reads_nothing_after_truncate(exec.state, &statement.tasks);
    let reads = request_reads(exec.state, &statement.tasks).map_err(StatementError::Error)?;
    crate::control::trigger::statement_txn::with_statement_txn_lifted(
        exec.state,
        exec.identity,
        client,
        true,
        StatementError::Error,
        async |txn: &DmlTxnCtx<'_>| {
            if cuts_at_commit {
                txn.sessions.allow_tx_edge_truncate(txn.session_id);
            }
            if reads {
                txn.txn_snapshot(exec.state)
                    .await
                    .map_err(StatementError::Error)?;
            }
            run_statement_loop(exec, statement, Some(txn)).await
        },
    )
    .await
}

/// Whether an implicit transaction of `tasks` reads no edge after a TRUNCATE:
/// no task after it reads, and no joined body runs. Its edge shares then cut the
/// edges at the transaction's turn, and nothing observes them before that.
fn reads_nothing_after_truncate(state: &SharedState, tasks: &[PhysicalTask]) -> bool {
    tasks
        .iter()
        .skip_while(|task| !crate::control::planner::calvin::edge_truncate::is_truncate(&task.plan))
        .all(|task| plan_is_write(&task.plan))
        && !statement_fires_joined_body(state, tasks)
}
