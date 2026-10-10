// SPDX-License-Identifier: BUSL-1.1

//! Plan and dispatch one rebuilt collection-DML statement.

use std::sync::Arc;

use crate::control::planner::context::PlanSecurityContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::authorization::{AuthorizedTaskSet, authorize_task_set};
use crate::control::server::shared::clone_write::{
    CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::returning;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::txn_route::{
    StatementEvents, TxnTaskContext, TxnTaskOutcome, route_txn_task,
};
use crate::control::server::shared::write_admission::all_writes_bufferable;
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::dispatch_write::{
    ReturningShape, authorization_error_to_ddl, dispatch_staged, error_to_ddl, staging_error_to_ddl,
};
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
    // The clause is stripped from the rebuilt statement before planning. The
    // planner resolves the item text against the planned target and attaches
    // the Data-Plane spec to every task before they come back here.
    let (sql, returning_items) = returning::strip_returning(sql).map_err(|error| {
        let (_, sqlstate, message) = error_to_sqlstate(&error);
        ddl_err(sqlstate, message)
    })?;
    let sql = sql.as_str();
    // This is a client statement — the object-literal `INSERT INTO c { … }` and
    // `UPSERT` forms land here after being rewritten to standard SQL — so it
    // plans under the requester's own scope, the same one it is authorized and
    // metered as below. Planning it as the system will apply no row policy to
    // it: read filters will not be injected, and the write gates will decide
    // nothing, on a transport a client can reach directly.
    //
    // Injection happens inside planning, before the task set is consumed:
    // implicit-edge extraction, authorization, staging, and dispatch all read
    // `tasks` after this point, and injecting later will hand them
    // un-injected copies.
    let (mut tasks, output_schema, versions) = {
        let scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id);
        crate::control::security::auth_fence::admit_permission_view(state, tenant_id)
            .await
            .map_err(|error| DdlError::from_error(&error))?;
        let sec = PlanSecurityContext {
            identity,
            auth: scope.auth(),
            rls_store: &state.rls,
            redaction_store: &state.redaction,
            permissions: &state.permissions,
            roles: &state.roles,
            permission_tree: crate::control::planner::context::PermissionTreeSource::Live(
                &state.permission_cache,
            ),
        };
        let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
        let (tasks, output_schema, versions, _) = query_ctx
            .plan_sql_with_rls_and_versions(
                sql,
                tenant_id,
                database_id,
                &sec,
                returning_items.as_deref(),
            )
            .await
            .map_err(|error| {
                let (_, sqlstate, message) = error_to_sqlstate(&error);
                ddl_err(sqlstate, message)
            })?;
        (tasks, output_schema, versions)
    };
    let has_returning = returning_items.is_some();

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

    let sum_read_vshards = crate::control::planner::calvin::read_vshards_of(&sum_target_reads)
        .map_err(|error| DdlError::from_error(&error))?;
    if !in_txn_block
        && matches!(
            crate::control::planner::calvin::classify_dispatch(&tasks, &sum_read_vshards),
            crate::control::planner::calvin::DispatchClass::MultiShard { .. }
        )
    {
        crate::control::planner::calvin::dispatch_authorized_tasks_to_calvin(
            state,
            authorized_tasks,
            tenant_id,
            crate::control::planner::calvin::CrossShardTxnMode::Strict,
            crate::control::planner::calvin::TxnDispatchPosition::Autocommit,
            &sum_target_reads,
            None,
        )
        .await
        .map_err(|error| {
            let (_, sqlstate, message) = error_to_sqlstate(&error);
            ddl_err(sqlstate, message)
        })?;
        // A cross-shard Calvin dispatch returns no per-task payload here, so
        // there is no stored row to project. Refused rather than answered with
        // an empty row set, which will read as "the write matched nothing".
        if has_returning {
            return Err(ddl_err(
                "0A000",
                "RETURNING is not supported on a write that spans multiple shards",
            ));
        }
        return Ok(Vec::new());
    }

    // The row images the statement's cross-shard balances were settled from
    // join the transaction's read set, so COMMIT's conflict check covers
    // them.
    if in_txn_block && !sum_target_reads.is_empty() {
        txn_ctx
            .sessions
            .record_read_entries(txn_ctx.session_id, sum_target_reads);
    }

    let auth_scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id);
    let route_ctx = in_txn_block.then(|| TxnTaskContext {
        state,
        identity,
        auth: auth_scope.auth(),
        txn: txn_ctx,
        lease_scope: &plan_lease_scope,
        fire_triggers,
    });
    let returning_shape = ReturningShape {
        state,
        identity,
        database_id,
        txn_ctx,
        output_schema: &output_schema,
    };
    let mut statement_events = StatementEvents::default();
    let mut returned_rows: Option<ShapedRows> = None;
    for (task, initial_authorized) in tasks.into_iter().zip(authorized_tasks.into_tasks()) {
        drop(initial_authorized);
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
                Some(code) => {
                    let (_, sqlstate, message) = error_code_to_sqlstate(code);
                    ddl_err(sqlstate, message)
                }
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
