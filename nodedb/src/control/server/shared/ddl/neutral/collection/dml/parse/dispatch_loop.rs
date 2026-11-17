// SPDX-License-Identifier: BUSL-1.1

//! The per-task loop of one authorized collection-DML statement.
//!
//! In a transaction (the client's block, or the implicit transaction a
//! statement of several writes runs in, see [`run_implicit_task_loop`])
//! every task takes the shared
//! `txn_route`: its joined trigger bodies run, a shadowed clone takes its
//! copy-on-write steps, and the write stages. An autocommit statement
//! dispatches each task on the durable route.

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::lease::QueryLeaseScope;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::authorization::authorize_task_set;
use crate::control::server::shared::clone_write::{
    CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use crate::control::server::shared::cluster_array_dispatch::{is_cluster_array, run_cluster_array};
use crate::control::server::shared::ddl::neutral::refusal::coded_refusal;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::conn_scope::scoped_system_txn;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::server::shared::txn_route::{
    StatementEvents, TxnTaskContext, TxnTaskOutcome, route_txn_task, statement_fires_joined_body,
};
use crate::control::state::SharedState;
use crate::control::trigger::statement_txn::{
    ImplicitAttempt, ImplicitRetry, begin_implicit, finish_implicit,
};
use crate::types::TraceId;

use super::dispatch_write::{
    ReturningShape, authorization_error_to_ddl, dispatch_staged, error_to_ddl, staging_error_to_ddl,
};

/// One authorized statement and what its loop needs.
pub(super) struct TaskLoop<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub tenant_id: nodedb_types::TenantId,
    pub database_id: crate::types::DatabaseId,
    pub auth_scope: &'a RequestAuthScope<'a>,
    pub plan_lease_scope: &'a Arc<QueryLeaseScope>,
    pub output_schema: &'a OutputSchema,
    pub fire_triggers: bool,
    pub has_returning: bool,
    pub tasks: Vec<PhysicalTask>,
    /// The row images the statement's cross-shard balances were settled
    /// from. A statement in a transaction adds them to its read set.
    pub sum_target_reads: Vec<ReadSetEntry>,
}

/// Run `statement` in an implicit transaction: it commits when every task
/// succeeds and rolls back when one fails. A COMMIT that aborts on a
/// serialization conflict runs the statement again on a new transaction,
/// until the statement deadline, unless its writes fire a joined trigger
/// body here.
pub(super) async fn run_implicit_task_loop(
    statement: &TaskLoop<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let retry = !(statement.fire_triggers
        && statement_fires_joined_body(statement.state, &statement.tasks));
    let mut pacing = ImplicitRetry::new(statement.state, retry);
    loop {
        match scoped_system_txn(implicit_attempt(statement)).await {
            ImplicitAttempt::Committed(results) => return Ok(results),
            ImplicitAttempt::Failed(error) => return Err(error),
            ImplicitAttempt::Conflict(conflict) => {
                if !pacing.pause().await {
                    return Err(DdlError::from(crate::Error::from(conflict)));
                }
            }
        }
    }
}

/// One attempt of [`run_implicit_task_loop`].
async fn implicit_attempt(statement: &TaskLoop<'_>) -> ImplicitAttempt<Vec<DdlResult>, DdlError> {
    let txn = match begin_implicit(statement.state, statement.identity) {
        Ok(txn) => txn,
        Err(error) => return ImplicitAttempt::Failed(DdlError::from(error)),
    };
    let result = match txn.txn_ctx() {
        Ok(implicit_ctx) => run_task_loop(statement, &implicit_ctx, true).await,
        Err(error) => Err(DdlError::from(error)),
    };
    finish_implicit(txn, result, DdlError::from).await
}

/// Run every task of `statement` on `txn_ctx`. `staged` is `true` when
/// `txn_ctx` holds an open transaction the writes stage into, `false` for an
/// autocommit statement. Returns the rows a `RETURNING` clause produced.
pub(super) async fn run_task_loop(
    statement: &TaskLoop<'_>,
    txn_ctx: &DmlTxnCtx<'_>,
    staged: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    let TaskLoop {
        state,
        identity,
        tenant_id,
        database_id,
        auth_scope,
        plan_lease_scope,
        output_schema,
        fire_triggers,
        has_returning,
        tasks,
        sum_target_reads,
    } = statement;
    let (state, identity, tenant_id, database_id) = (*state, *identity, *tenant_id, *database_id);
    let (fire_triggers, has_returning) = (*fire_triggers, *has_returning);
    let (auth_scope, plan_lease_scope, output_schema) =
        (*auth_scope, *plan_lease_scope, *output_schema);

    // The row images the statement's cross-shard balances were settled from
    // join the transaction's read set, so COMMIT's conflict check covers
    // them.
    if staged && !sum_target_reads.is_empty() {
        txn_ctx
            .sessions
            .record_read_entries(txn_ctx.session_id, sum_target_reads.clone());
    }

    let returning_shape = ReturningShape {
        state,
        identity,
        tenant_id,
        database_id,
        txn_ctx,
        output_schema,
    };
    let route_ctx = staged.then(|| TxnTaskContext {
        state,
        identity,
        auth: auth_scope.auth(),
        txn: txn_ctx,
        lease_scope: plan_lease_scope,
        fire_triggers,
    });
    let mut statement_events = StatementEvents::default();
    let mut returned_rows: Option<ShapedRows> = None;
    for task in tasks.iter().cloned() {
        let task = match route_ctx.as_ref() {
            Some(route) => {
                let plan = task.plan.clone();
                match route_txn_task(route, task, &mut statement_events, |staged| {
                    dispatch_staged(state, identity, staged)
                })
                .await
                .map_err(staging_error_to_ddl)?
                {
                    TxnTaskOutcome::Dispatch(task) => *task,
                    TxnTaskOutcome::Staged(staged) => {
                        if has_returning {
                            for rows in &staged.returning_rows {
                                returning_shape.fold(&plan, rows, &mut returned_rows)?;
                            }
                        }
                        continue;
                    }
                    TxnTaskOutcome::CloneHandled(resp) => {
                        if has_returning {
                            returning_shape.fold(
                                &plan,
                                resp.payload.as_bytes(),
                                &mut returned_rows,
                            )?;
                        }
                        continue;
                    }
                    TxnTaskOutcome::Buffered | TxnTaskOutcome::InsteadOf => continue,
                }
            }
            None => task,
        };

        // A `ClusterArray` plan runs on the Control Plane through the array
        // coordinator, as every transport's dispatch loop runs it. Each shard
        // proposes its cells through its own replicated entry, so the plan
        // never takes the local funnel route. Inside a transaction the
        // staging gate above reshapes a write into per-shard `ArrayOp` tasks,
        // so only an autocommit write or a read arrives here.
        if is_cluster_array(&task.plan) {
            let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
            let authorized = authorize_task_set(
                identity,
                std::slice::from_ref(&task),
                &state.permissions,
                &state.roles,
                &emitter,
            )
            .map_err(authorization_error_to_ddl)?
            .into_tasks()
            .into_iter()
            .next()
            .ok_or_else(|| DdlError::internal("authorization returned no task"))?;
            let shared = state.self_arc().map_err(|error| error_to_ddl(&error))?;
            let payload = run_cluster_array(&shared, authorized)
                .await
                .map_err(|error| error_to_ddl(&error))?;
            if has_returning {
                returning_shape.fold(&task.plan, &payload, &mut returned_rows)?;
            }
            continue;
        }

        let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
        let response = match intercept_and_authorize(InterceptAndAuthorizeParams {
            state,
            task: task.clone(),
            identity,
            tenant_id,
            permissions: &state.permissions,
            roles: &state.roles,
            emitter: &emitter,
        })
        .await
        .map_err(|error| error_to_ddl(&error))?
        {
            CloneCheckedOutcome::Handled(resp) => resp,
            // A write takes the durable route: Raft in cluster mode, else the
            // funnel's `AppendHere`. A read takes the read route.
            CloneCheckedOutcome::Proceed(checked) => {
                crate::control::server::dispatch_utils::dispatch_authorized_task_by_class(
                    state,
                    checked,
                    TraceId::ZERO,
                )
                .await
                .map_err(|error| error_to_ddl(&error))?
            }
        };

        if response.status == crate::bridge::envelope::Status::Error {
            return Err(match response.error_code.as_deref() {
                Some(code) => coded_refusal(code),
                // With no code, the payload holds the Data Plane's message.
                None => DdlError::internal(String::from_utf8_lossy(&response.payload)),
            });
        }

        if has_returning {
            returning_shape.fold(&task.plan, response.payload.as_bytes(), &mut returned_rows)?;
        }
    }
    if let Some(route) = route_ctx.as_ref() {
        statement_events
            .fire(route)
            .await
            .map_err(|error| error_to_ddl(&error))?;
    }
    Ok(returned_rows.map(DdlResult::Rows).into_iter().collect())
}
