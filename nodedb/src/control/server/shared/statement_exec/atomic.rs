// SPDX-License-Identifier: BUSL-1.1

//! The routes that commit a planned statement's tasks together, taken ahead
//! of its per-task loop. Native, HTTP, WebSocket RPC and NDJSON call
//! [`route_atomic_statement`] for a planned SQL statement, so the same
//! statement takes the same route over each of them, as it does over pgwire.
//!
//! In order:
//!
//! 1. A statement outside a transaction block that
//!    [`statement_needs_implicit_txn`] flags runs in an implicit
//!    transaction: its writes stage on their vShards' leaders and commit
//!    together at its end, through Calvin when they span vShards.
//! 2. A dependent predicate on an edge-bearing collection runs through the
//!    implicit-edge OLLP/Calvin coordinator.
//! 3. A statement on one vShard that holds settlement reads commits through
//!    the Calvin sequencer: a recon plan through the coordinator, any other
//!    write as a static transaction. Its stage checks the reads.
//! 4. An autocommit statement whose `RETURNING` list can fail commits
//!    through the Calvin sequencer, on one vShard or several and in every
//!    cross-shard mode. Its participant evaluates the list before it votes,
//!    so an error in the list stores nothing. A list that cannot fail keeps
//!    the direct path. An `INSERT ... SELECT`, `UPDATE ... FROM` or `MERGE`
//!    whose list can fail takes route 1 instead: Calvin keys none of them.
//! 5. A write that spans vShards commits through the Calvin sequencer, so a
//!    derived task (a cross-shard balance move, an implicit edge) commits
//!    with the user's write. A predicate write among them takes the
//!    coordinator, which reads the rows it matched.
//!
//! A request whose writes settle one balance from the same rows is refused
//! ahead of every route.

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::lease::QueryLeaseScope;
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, classify_dispatch,
    dispatch_authorized_tasks_to_calvin, read_vshards_of, refuse_several_settling_writes,
};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::response_shape::calvin_fold::{
    CalvinFoldCtx, CalvinFoldError, fold_calvin_batch,
};
use crate::control::server::shared::authorization::{AuthorizedTaskSet, authorize_task_set};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_tasks;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::server::shared::txn_route::{
    statement_needs_implicit_txn, unbufferable_joined_statement,
};
use crate::control::server::shared::write_admission::all_writes_bufferable;
use crate::control::trigger::statement_txn::in_block;

use super::answer::StatementAnswer;
use super::edge_recon::{
    CalvinApplied, EdgeRecon, try_edge_recon, try_multi_shard_predicate_recon, try_settled_recon,
};
use super::error::StatementError;
use super::fallible_returning::returning_commits_through_calvin;
use super::task_loop::{PlannedStatement, StatementExec, run_implicit_statement};

/// A planned statement before it takes a route.
pub(crate) struct AtomicStatement {
    pub(crate) tasks: Vec<PhysicalTask>,
    /// The statement's descriptor leases. They stay alive through dispatch.
    pub(crate) lease_scope: QueryLeaseScope,
    /// The row images the statement's cross-shard balances were settled
    /// from. They widen Calvin's read set, so its OCC check aborts a balance
    /// folded from an image that has since moved.
    pub(crate) sum_target_reads: Vec<ReadSetEntry>,
}

/// The route a statement took.
pub(crate) enum AtomicRoute {
    /// The statement ran in its implicit transaction.
    Implicit(StatementAnswer),
    /// Calvin applied the statement's tasks as one transaction.
    Calvin(CalvinApplied),
    /// No atomic route applies. The caller runs its per-task loop.
    PerTask(AtomicStatement),
}

/// Route `statement`, planned on `session`, through the first atomic route
/// that applies. A transport with no session passes a detached one, which is
/// never in a transaction block and takes the default cross-shard mode.
///
/// A Calvin write's participant answers the `RETURNING` columns the
/// statement announced, so every route runs with them in scope.
pub(crate) async fn route_atomic_statement(
    exec: &StatementExec<'_>,
    statement: AtomicStatement,
    session: &DmlTxnCtx<'_>,
) -> Result<AtomicRoute, StatementError> {
    crate::control::server::shared::session::announced_returning::with_announced_returning(
        exec.output_schema,
        route_atomic(exec, statement, session),
    )
    .await
}

async fn route_atomic(
    exec: &StatementExec<'_>,
    statement: AtomicStatement,
    session: &DmlTxnCtx<'_>,
) -> Result<AtomicRoute, StatementError> {
    let AtomicStatement {
        tasks,
        lease_scope,
        sum_target_reads,
    } = statement;
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

    let in_txn_block = in_block(session);
    if !in_txn_block
        && statement_needs_implicit_txn(state, &tasks).map_err(StatementError::Error)?
    {
        if !all_writes_bufferable(&tasks) {
            return Err(StatementError::Error(unbufferable_joined_statement()));
        }
        let answer = run_implicit_statement(
            exec,
            PlannedStatement {
                tasks,
                plan_lease_scope: Arc::new(lease_scope),
                sum_target_reads,
            },
        )
        .await?;
        return Ok(AtomicRoute::Implicit(answer));
    }

    // A Calvin route below commits the tasks with no per-task step, so a
    // spent hard quota refuses them here, before any submit. The per-task
    // loop a statement falls back to admits each task again.
    admit_quota_for_tasks(state, exec.scope, &tasks).map_err(StatementError::Quota)?;

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

    // An autocommit statement has no session read set to widen with. The
    // reads to widen with are those materialized-sum settlement stamped.
    let sum_read_vshards = read_vshards_of(&sum_target_reads).map_err(StatementError::Error)?;
    match classify_dispatch(&tasks, &sum_read_vshards) {
        // A zero-net settlement on one vShard. The per-task loop checks no read.
        DispatchClass::SingleShard { .. } if !in_txn_block && !sum_target_reads.is_empty() => {
            let applied = commit_static(
                exec,
                &tasks,
                authorized,
                session.sessions.cross_shard_txn_mode(session.session_id),
                &sum_target_reads,
            )
            .await?;
            return Ok(AtomicRoute::Calvin(applied));
        }
        // The direct path evaluates `RETURNING` after the write is stored.
        // A list that can fail commits through Calvin, whose participant
        // evaluates it before it votes, so its error stores nothing.
        DispatchClass::SingleShard { .. }
            if !in_txn_block
                && returning_commits_through_calvin(state, exec.tenant_id(), &tasks) =>
        {
            let applied = commit_through_calvin(exec, tasks, authorized, &sum_target_reads).await?;
            return Ok(AtomicRoute::Calvin(applied));
        }
        DispatchClass::SingleShard { .. } => {}
        DispatchClass::MultiShard { .. } => {
            // Calvin applies the statement durably at statement time,
            // outside any transaction buffer. Inside a block the per-task
            // loop buffers each write, and COMMIT flushes the buffer through
            // Calvin. A write the gate cannot buffer is refused.
            if in_txn_block && !all_writes_bufferable(&tasks) {
                return Err(StatementError::Error(
                    crate::Error::CrossShardInExplicitTransaction,
                ));
            }
            let cross_shard_mode = session.sessions.cross_shard_txn_mode(session.session_id);
            // `BestEffortNonAtomic` falls through to the per-task loop,
            // unless a `RETURNING` list that can fail needs the vote.
            if !in_txn_block
                && (cross_shard_mode == CrossShardTxnMode::Strict
                    || returning_commits_through_calvin(state, exec.tenant_id(), &tasks))
            {
                let applied =
                    commit_through_calvin(exec, tasks, authorized, &sum_target_reads).await?;
                return Ok(AtomicRoute::Calvin(applied));
            }
        }
    }

    Ok(AtomicRoute::PerTask(AtomicStatement {
        tasks,
        lease_scope,
        sum_target_reads,
    }))
}

/// Commit `tasks`, an autocommit statement, as one Calvin transaction. A
/// predicate write names the rows it matched only after the coordinator
/// reads them, so it takes the route pgwire takes. Every other write commits
/// as a static transaction. `reads` are the images settlement folded the
/// statement's balances from.
async fn commit_through_calvin(
    exec: &StatementExec<'_>,
    tasks: Vec<PhysicalTask>,
    authorized: AuthorizedTaskSet,
    reads: &[ReadSetEntry],
) -> Result<CalvinApplied, StatementError> {
    match try_multi_shard_predicate_recon(exec.state, exec.identity, tasks, authorized, reads)
        .await
        .map_err(StatementError::Error)?
    {
        EdgeRecon::Applied(applied) => {
            meter_applied(exec, &applied);
            Ok(applied)
        }
        EdgeRecon::NotFired(tasks, authorized) => {
            commit_static(exec, &tasks, authorized, CrossShardTxnMode::Strict, reads).await
        }
    }
}

/// Commit `tasks`, an autocommit statement, as one static Calvin
/// transaction under `cross_shard_mode`, guarded by `reads`.
async fn commit_static(
    exec: &StatementExec<'_>,
    tasks: &[PhysicalTask],
    authorized: AuthorizedTaskSet,
    cross_shard_mode: CrossShardTxnMode,
    reads: &[ReadSetEntry],
) -> Result<CalvinApplied, StatementError> {
    let plans = tasks.iter().map(|task| task.plan.clone()).collect();
    let apply_result = dispatch_authorized_tasks_to_calvin(
        exec.state,
        authorized,
        exec.tenant_id(),
        cross_shard_mode,
        TxnDispatchPosition::Autocommit,
        reads,
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
    // The writes ran in `database_id`, so the session's values land there.
    let session = exec.sequences.in_database(database_id);
    let fold = fold_calvin_batch(
        &plan_refs,
        apply_result.as_ref(),
        &CalvinFoldCtx { session: &session },
    )
    .map_err(|error| match error {
        CalvinFoldError::Verb(error) => StatementError::DmlFold(error),
        CalvinFoldError::Shape(error) => StatementError::Error(error),
    })?;
    // One result set per RETURNING plan, as the per-task loop folds one per
    // task.
    let mut warnings = Vec::new();
    let mut rows = fold.rows;
    for shaped in &mut rows {
        if let Some(notice) = shaped.notice.take() {
            warnings.push(notice);
        }
    }
    Ok(StatementAnswer {
        rows,
        warnings,
        tag: fold.tag,
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
