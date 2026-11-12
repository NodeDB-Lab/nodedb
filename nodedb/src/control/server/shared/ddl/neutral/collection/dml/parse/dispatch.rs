// SPDX-License-Identifier: BUSL-1.1

//! Plan and dispatch one rebuilt collection-DML statement.

use std::sync::Arc;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::shared::authorization::{AuthorizedTaskSet, authorize_task_set};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::quota_admission::admit_quota_for_tasks;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::statement_exec::{SeveralWrites, several_writes_route};
use crate::control::server::shared::write_admission::all_writes_bufferable;
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::dispatch_calvin::{AutocommitRoute, AutocommitStatement, route_autocommit};
use super::dispatch_loop::{TaskLoop, run_implicit_task_loop, run_task_loop};
use super::dispatch_write::{ReturningShape, authorization_error_to_ddl};
use super::plan_statement::{PlanBuilder, PlannedStatement, StatementSource, plan_statement};
use super::types::ddl_err;

/// Plan SQL through nodedb-sql, authorize the final task set, and dispatch it.
///
/// Returns the rows a `RETURNING` clause on `sql` produced, empty when the
/// statement carries none.
///
/// Inside a transaction block every task takes the shared `txn_route`: its
/// BEFORE, INSTEAD OF and SYNC AFTER bodies join the transaction when
/// `fire_triggers` is set (a caller that fired them around the statement
/// itself clears it), a shadowed clone takes its copy-on-write steps, and
/// the write stages. The rows are decoded from the Data Plane's own
/// response — the STORED post-image — and are redacted before they leave, so
/// this path answers `RETURNING` exactly as the pgwire planner does rather than
/// echoing back the values the caller submitted.
pub(in crate::control::server::shared::ddl::neutral::collection) async fn plan_and_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    database_id: crate::types::DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
    fire_triggers: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    let sql = sql.to_owned();
    let source = StatementSource::Sql { sql: sql.as_str() };
    let planned = plan_statement(state, identity, tenant_id, database_id, source, txn_ctx).await?;
    dispatch_planned(
        state,
        identity,
        tenant_id,
        database_id,
        planned,
        txn_ctx,
        fire_triggers,
    )
    .await
}

/// [`plan_and_dispatch`] for a statement whose plans `build` makes, instead
/// of SQL text: a bulk load has rows, not SQL. The plans take the same
/// policy pass, authorization, transaction routing and durable dispatch.
/// The statement carries no `RETURNING`.
pub(in crate::control::server::shared::ddl::neutral::collection) async fn plan_and_dispatch_built(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    database_id: crate::types::DatabaseId,
    build: PlanBuilder<'_>,
    txn_ctx: &DmlTxnCtx<'_>,
    fire_triggers: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    let source = StatementSource::Built(build);
    let planned = plan_statement(state, identity, tenant_id, database_id, source, txn_ctx).await?;
    dispatch_planned(
        state,
        identity,
        tenant_id,
        database_id,
        planned,
        txn_ctx,
        fire_triggers,
    )
    .await
}

/// Authorize a planned statement's final task set and dispatch it: staged
/// inside a transaction block, through Calvin when it spans shards, holds
/// settlement reads or carries a `RETURNING` list that can fail, and
/// through the durable route otherwise.
async fn dispatch_planned(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    database_id: crate::types::DatabaseId,
    planned: PlannedStatement,
    txn_ctx: &DmlTxnCtx<'_>,
    fire_triggers: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    let PlannedStatement {
        mut tasks,
        output_schema,
        versions,
        has_returning,
    } = planned;

    // Extraction marks catalog state and allocates surrogates. Reject an
    // unauthorized original DML task set before either side effect can occur.
    let _preauthorized_tasks = authorize_final_task_set(state, identity, &tasks)?;

    // The final set includes implicit graph-edge writes and must be authorized
    // before descriptor admission, Calvin classification, transaction staging,
    // or local dispatch.
    crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        state,
        &mut tasks,
        tenant_id,
        database_id,
        TraceId::ZERO,
    )
    .await
    .map_err(|error| {
        let (_, sqlstate, message) = error_to_sqlstate(&error);
        ddl_err(sqlstate, message)
    })?;

    // The entries cover the row images every cross-shard balance this pass
    // settled was folded from. They travel on the dispatch read-set so the
    // Calvin OCC check aborts, before any row moves, if those images have been
    // written since. The source rows are read through the open transaction's
    // staging overlay, so they include this transaction's earlier statements.
    let read_txn = txn_ctx.sessions.tx_id(txn_ctx.session_id);
    let sum_target_reads =
        crate::control::planner::materialized_sum::resolve_materialized_sum_targets(
            state,
            &mut tasks,
            tenant_id,
            database_id,
            read_txn,
            TraceId::ZERO,
        )
        .await
        .map_err(|error| {
            let (_, sqlstate, message) = error_to_sqlstate(&error);
            ddl_err(sqlstate, message)
        })?;

    crate::control::planner::materialized_sum::append_cross_shard_balance_tasks(
        state,
        &mut tasks,
        tenant_id,
        database_id,
    )
    .map_err(|error| {
        let (_, sqlstate, message) = error_to_sqlstate(&error);
        ddl_err(sqlstate, message)
    })?;

    crate::control::planner::period_lock::resolve_period_lock_targets(
        state,
        &mut tasks,
        tenant_id,
        database_id,
        read_txn,
        TraceId::ZERO,
    )
    .await
    .map_err(|error| {
        let (_, sqlstate, message) = error_to_sqlstate(&error);
        ddl_err(sqlstate, message)
    })?;

    let authorized_tasks = authorize_final_task_set(state, identity, &tasks)?;
    // Admission follows final authorization so an implicit-edge target denied
    // by policy does not consume a descriptor lease. The scope remains live
    // through expansion's successors, transaction staging, and dispatch below.
    let plan_lease_scope = Arc::new(state.acquire_plan_lease_scope(&versions).await.map_err(
        |error| {
            let (_, sqlstate, message) = error_to_sqlstate(&error);
            ddl_err(sqlstate, message)
        },
    )?);

    // A statement dispatched to Calvin as autocommit escapes the transaction
    // buffer entirely: it applies durably at statement time and ROLLBACK cannot
    // undo it. Inside a block the per-task staging gate below buffers each task
    // instead, and COMMIT flushes the whole buffer through Calvin. A write the
    // gate cannot buffer is refused rather than silently applied.
    let in_txn_block = txn_ctx.sessions.transaction_state(txn_ctx.session_id)
        == crate::control::server::shared::session::TransactionState::InBlock;
    if in_txn_block && !all_writes_bufferable(&tasks) {
        let (_, sqlstate, message) =
            error_to_sqlstate(&crate::Error::CrossShardInExplicitTransaction);
        return Err(ddl_err(sqlstate, message));
    }

    // A spent hard quota refuses the statement before any task runs, on the
    // Calvin route and the per-task route alike.
    let auth_scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id)?;
    admit_quota_for_tasks(state, &auth_scope, &tasks)
        .map_err(|error| ddl_err("53400", error.to_string()))?;

    let several_writes = if in_txn_block {
        None
    } else {
        several_writes_route(
            state,
            &tasks,
            txn_ctx.sessions.cross_shard_txn_mode(txn_ctx.session_id),
        )
    };
    let statement = TaskLoop {
        state,
        identity,
        tenant_id,
        database_id,
        auth_scope: &auth_scope,
        plan_lease_scope: &plan_lease_scope,
        output_schema: &output_schema,
        fire_triggers,
        has_returning,
        tasks,
        sum_target_reads,
    };

    // An autocommit statement of several writes that Calvin cannot carry, or
    // that writes a clone still reading its source, stages in an implicit
    // transaction and commits at its end: an error in any write stores none.
    if several_writes == Some(SeveralWrites::ImplicitTxn) {
        drop(authorized_tasks);
        return run_implicit_task_loop(&statement).await;
    }

    let returning_shape = ReturningShape {
        state,
        identity,
        tenant_id,
        database_id,
        txn_ctx,
        output_schema: &output_schema,
    };
    // An autocommit statement that spans vShards, holds settlement reads,
    // carries a `RETURNING` list that can fail, or holds several writes
    // commits through Calvin.
    let authorized_tasks = if in_txn_block {
        authorized_tasks
    } else {
        match route_autocommit(
            state,
            tenant_id,
            &returning_shape,
            AutocommitStatement {
                tasks: &statement.tasks,
                authorized: authorized_tasks,
                reads: &statement.sum_target_reads,
                has_returning,
                several_writes: several_writes == Some(SeveralWrites::Calvin),
            },
        )
        .await?
        {
            AutocommitRoute::Calvin(results) => return Ok(results),
            AutocommitRoute::Direct(authorized) => authorized,
        }
    };

    drop(authorized_tasks);
    run_task_loop(&statement, txn_ctx, in_txn_block).await
}

fn authorize_final_task_set(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tasks: &[nodedb_physical::physical_task::PhysicalTask],
) -> Result<AuthorizedTaskSet, DdlError> {
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_task_set(identity, tasks, &state.permissions, &state.roles, &emitter)
        .map_err(authorization_error_to_ddl)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::authorization::AuthorizationError;
    use crate::types::TenantId;

    #[test]
    fn authorization_denial_preserves_insufficient_privilege_sqlstate() {
        let error = AuthorizationError::new(
            TenantId::new(1),
            "permission denied on collection".to_owned(),
        );
        let ddl_error = authorization_error_to_ddl(error);

        assert_eq!(ddl_error.sqlstate, "42501");
        assert!(ddl_error.message.contains("permission denied"));
    }
}
