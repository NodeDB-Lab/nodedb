// SPDX-License-Identifier: BUSL-1.1

//! The routes that commit a planned statement's tasks together, taken ahead
//! of its per-task loop. Native, HTTP, WebSocket RPC and NDJSON call
//! [`route_atomic_statement`] for a planned SQL statement, so the same
//! statement takes the same route over each of them, as it does over pgwire.
//!
//! In order:
//!
//! 1. A statement outside a transaction block that
//!    [`implicit_txn_route`](super::route_decision::implicit_txn_route)
//!    flags runs in an implicit transaction: its writes stage on their
//!    vShards' leaders and commit together at its end, through Calvin when
//!    they span vShards. A request of several statements with a write whose
//!    writes all stage runs in one too (`super::implicit_request`).
//! 2. A dependent predicate on an edge-bearing collection runs through the
//!    implicit-edge OLLP/Calvin coordinator.
//! 3. A statement on one vShard that holds settlement reads commits through
//!    the Calvin sequencer: a recon plan through the coordinator, any other
//!    write as a static transaction. Its stage checks the reads.
//! 4. A write that spans vShards commits through the Calvin sequencer, so a
//!    derived task (a cross-shard balance move, an implicit edge) commits
//!    with the user's write. A predicate write among them takes the
//!    coordinator, which reads the rows it matched.
//!
//! A request whose writes settle one balance from the same rows is refused
//! ahead of every route.
//!
//! Which of routes 1, 3 and 4 applies is [`super::route_decision`]'s call,
//! shared with the pgwire handler. This module runs the answer.

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::lease::QueryLeaseScope;
use crate::control::planner::calvin::{
    CrossShardTxnMode, TxnDispatchPosition, dispatch_authorized_tasks_to_calvin,
    refuse_several_settling_writes,
};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::response_shape::calvin_fold::{
    CalvinFoldCtx, CalvinFoldError, fold_calvin_batch,
};
use crate::control::server::shared::authorization::{AuthorizedTaskSet, authorize_task_set};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::plan_admission_request::folded_schema;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::trigger::statement_txn::in_block;

use super::answer::{AFFECTED_KEY, RequestAnswer, StatementAnswer};
use super::edge_recon::{
    CalvinApplied, EdgeRecon, try_edge_recon, try_multi_shard_predicate_recon, try_settled_recon,
};
use super::error::StatementError;
use super::implicit_request::request_txn_route;
use super::route_decision::{AutocommitRoute, ImplicitTxnRoute, autocommit_route};
use super::statement_map::StatementMap;
use super::task_loop::{PlannedStatement, StatementExec, run_implicit_statement};

/// A planned statement before it takes a route.
pub(crate) struct AtomicStatement {
    pub(crate) tasks: Vec<PhysicalTask>,
    /// The statement of each task in `tasks`, one entry per task.
    pub(crate) statement_map: StatementMap,
    /// The statement's descriptor leases. They stay alive through dispatch.
    pub(crate) lease_scope: QueryLeaseScope,
    /// The row images the statement's cross-shard balances were settled
    /// from. They widen Calvin's read set, so its OCC check aborts a balance
    /// folded from an image that has since moved.
    pub(crate) sum_target_reads: Vec<ReadSetEntry>,
}

/// The route a statement took.
pub(crate) enum AtomicRoute {
    /// The request ran in its implicit transaction. Its answer holds one
    /// part per statement.
    Implicit(RequestAnswer),
    /// Calvin applied the statement's tasks as one transaction.
    Calvin(CalvinApplied),
    /// No atomic route applies. The caller runs its per-task loop.
    PerTask(AtomicStatement),
}

/// Route `statement`, planned on `session`, through the first atomic route
/// that applies. A transport with no session passes a detached one, which is
/// never in a transaction block and takes the default cross-shard mode.
pub(crate) async fn route_atomic_statement(
    exec: &StatementExec<'_>,
    statement: AtomicStatement,
    session: &DmlTxnCtx<'_>,
) -> Result<AtomicRoute, StatementError> {
    let AtomicStatement {
        tasks,
        statement_map,
        lease_scope,
        sum_target_reads,
    } = statement;
    statement_map.debug_check_covers(&tasks);
    let state = exec.state;

    // Calvin dispatch and edge reconciliation are trusted internal mechanisms
    // that never reach the clone-checked dispatch boundary, so one batch
    // authorize is correct for them. The per-task loop authorizes each task
    // itself, immediately before its own dispatch.
    let authorized = authorize_task_set(
        exec.identity,
        &tasks,
        &state.permissions,
        &state.roles,
        &ArcAuditEmitter(Arc::clone(&state.audit)),
    )
    .map_err(|error| StatementError::Error(crate::Error::from(error)))?;

    refuse_several_settling_writes(&tasks).map_err(StatementError::Error)?;

    let (route, inserted) = request_txn_route(state, &tasks, &statement_map, session);
    match route {
        ImplicitTxnRoute::Implicit => {
            let mut answer = run_implicit_statement(
                exec,
                PlannedStatement {
                    tasks,
                    statement_map,
                    plan_lease_scope: Arc::new(lease_scope),
                    sum_target_reads,
                },
                session,
            )
            .await?;
            inserted.apply(&mut answer);
            return Ok(AtomicRoute::Implicit(answer));
        }
        ImplicitTxnRoute::Refuse(error) => return Err(StatementError::Error(error)),
        ImplicitTxnRoute::Autocommit => {}
    }
    let in_txn_block = in_block(session);

    let (tasks, authorized) = match try_edge_recon(
        state,
        exec.identity,
        in_txn_block,
        tasks,
        authorized,
        &sum_target_reads,
    )
    .await
    .map_err(StatementError::Error)?
    {
        EdgeRecon::Applied(applied) => {
            meter_applied(exec, &applied);
            return Ok(AtomicRoute::Calvin(applied));
        }
        EdgeRecon::NotFired(tasks, authorized) => (tasks, authorized),
    };
    let (tasks, authorized) = match try_settled_recon(
        state,
        exec.identity,
        in_txn_block,
        tasks,
        authorized,
        &sum_target_reads,
    )
    .await
    .map_err(StatementError::Error)?
    {
        EdgeRecon::Applied(applied) => {
            meter_applied(exec, &applied);
            return Ok(AtomicRoute::Calvin(applied));
        }
        EdgeRecon::NotFired(tasks, authorized) => (tasks, authorized),
    };
    // A gate that does not fire hands the tasks back unchanged.
    statement_map.debug_check_covers(&tasks);

    // An autocommit statement has no session read set to widen with. The
    // reads to widen with are those materialized-sum settlement stamped.
    match autocommit_route(&tasks, &sum_target_reads, session) {
        AutocommitRoute::PerTask => {}
        AutocommitRoute::Refuse(error) => return Err(StatementError::Error(error)),
        // A zero-net settlement on one vShard. The per-task loop checks no read.
        AutocommitRoute::SettledCalvin => {
            let mode = session.sessions.cross_shard_txn_mode(session.session_id);
            let applied = calvin_apply(exec, tasks, authorized, mode, &sum_target_reads).await?;
            return Ok(AtomicRoute::Calvin(applied));
        }
        AutocommitRoute::Calvin => {
            // A predicate write names the rows it matched only after the
            // coordinator reads them, so it takes the route pgwire takes.
            let (tasks, authorized) = match try_multi_shard_predicate_recon(
                state,
                exec.identity,
                tasks,
                authorized,
                &sum_target_reads,
            )
            .await
            .map_err(StatementError::Error)?
            {
                EdgeRecon::Applied(applied) => {
                    meter_applied(exec, &applied);
                    return Ok(AtomicRoute::Calvin(applied));
                }
                EdgeRecon::NotFired(tasks, authorized) => (tasks, authorized),
            };
            let mode = session.sessions.cross_shard_txn_mode(session.session_id);
            let applied = calvin_apply(exec, tasks, authorized, mode, &sum_target_reads).await?;
            return Ok(AtomicRoute::Calvin(applied));
        }
    }

    statement_map.debug_check_covers(&tasks);
    Ok(AtomicRoute::PerTask(AtomicStatement {
        tasks,
        statement_map,
        lease_scope,
        sum_target_reads,
    }))
}

/// Commit `tasks` through the Calvin sequencer as one autocommit transaction,
/// and meter each task.
async fn calvin_apply(
    exec: &StatementExec<'_>,
    tasks: Vec<PhysicalTask>,
    authorized: AuthorizedTaskSet,
    mode: CrossShardTxnMode,
    sum_target_reads: &[ReadSetEntry],
) -> Result<CalvinApplied, StatementError> {
    let plans = tasks.iter().map(|task| task.plan.clone()).collect();
    let apply_result = dispatch_authorized_tasks_to_calvin(
        exec.state,
        authorized,
        exec.tenant_id(),
        mode,
        TxnDispatchPosition::Autocommit,
        sum_target_reads,
        None,
    )
    .await
    .map_err(StatementError::Error)?;
    let applied = CalvinApplied {
        apply_result,
        plans,
        database_id: exec.database_id,
    };
    meter_applied(exec, &applied);
    Ok(applied)
}

/// Fold a statement Calvin applied into its answer: the RETURNING rows, when
/// a task carried them, and its one tag. The fold is the one pgwire renders.
/// The batch carries no statement of a plan, so the rows shape against the
/// request's folded schema.
pub(crate) fn calvin_answer(
    exec: &StatementExec<'_>,
    applied: CalvinApplied,
) -> Result<StatementAnswer, StatementError> {
    let CalvinApplied {
        apply_result,
        plans,
        database_id,
    } = applied;
    let plan_refs: Vec<_> = plans.iter().collect();
    let fold = fold_calvin_batch(
        &plan_refs,
        apply_result.as_ref(),
        &CalvinFoldCtx {
            projection: folded_schema(exec.schemas),
            state: exec.state,
            tenant_id: exec.tenant_id(),
            database_id,
            auth: exec.scope.auth(),
            sequences: Some(exec.sequences),
        },
    )
    .map_err(|error| match error {
        CalvinFoldError::Verb(error) => StatementError::DmlFold(error),
        CalvinFoldError::Shape(error) => StatementError::Error(error),
    })?;
    let mut warnings = Vec::new();
    let rows = match fold.rows {
        Some(mut shaped) => {
            if let Some(notice) = shaped.notice.take() {
                warnings.push(notice);
            }
            vec![shaped]
        }
        None => Vec::new(),
    };
    Ok(StatementAnswer {
        rows,
        warnings,
        tag: fold.tag,
        count_key: AFFECTED_KEY,
        last_lsn: apply_result.map_or(0, |response| response.watermark_lsn.as_u64()),
    })
}

/// Meter each task of a statement Calvin applied, as pgwire does.
fn meter_applied(exec: &StatementExec<'_>, applied: &CalvinApplied) {
    if !exec.state.metering_config.enabled {
        return;
    }
    for plan in &applied.plans {
        meter_dispatch(
            exec.state,
            exec.scope,
            &PlanMeteringInfo::extract(plan),
            None,
        );
    }
}
