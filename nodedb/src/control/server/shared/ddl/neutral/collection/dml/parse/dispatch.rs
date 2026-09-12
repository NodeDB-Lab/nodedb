// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use crate::control::planner::context::PlanSecurityContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::{PlanKind, ShapedRows};
use crate::control::server::shared::authorization::{
    AuthorizationError, AuthorizedTaskSet, authorize_collection, authorize_task_set,
};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::returning;
use crate::control::server::shared::session::{
    DmlTxnCtx, InTxnRoute, StagingGateError, route_in_tx_write,
};
use crate::control::server::shared::write_admission::all_writes_bufferable;
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::types::ddl_err;

/// Dispatch a plan to WAL + Data Plane, returning an error response on failure.
pub(in crate::control::server::shared::ddl::neutral::collection) async fn dispatch_plan(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    vshard_id: crate::types::VShardId,
    plan: crate::bridge::envelope::PhysicalPlan,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    let task = nodedb_physical::physical_task::PhysicalTask {
        tenant_id: identity.tenant_id,
        database_id,
        vshard_id,
        plan,
        post_set_op: nodedb_physical::physical_task::PostSetOp::None,
        txn_id: None,
    };
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    let checked = match crate::control::server::shared::clone_write::intercept_and_authorize(
        crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
            state,
            task,
            identity,
            tenant_id: identity.tenant_id,
            permissions: &state.permissions,
            roles: &state.roles,
            emitter: &emitter,
        },
    )
    .await
    {
        Ok(crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(_)) => {
            return None;
        }
        Ok(crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(checked)) => {
            checked
        }
        Err(error) => {
            let (_, sqlstate, message) = error_to_sqlstate(&error);
            return Some(Err(ddl_err(sqlstate, message)));
        }
    };

    if let Err(error) =
        crate::control::server::dispatch_utils::dispatch_authorized_autocommit_write(
            state,
            checked,
            TraceId::ZERO,
        )
        .await
    {
        let (_, sqlstate, message) = error_to_sqlstate(&error);
        return Some(Err(ddl_err(sqlstate, message)));
    }
    None
}

/// Authorize a write target before triggers, sequences, or catalog reads run.
pub(in crate::control::server::shared::ddl::neutral::collection) fn authorize_write_target(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    collection: &str,
) -> Result<(), DdlError> {
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        collection,
        Permission::Write,
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(authorization_error_to_ddl)
}

/// Plan SQL through nodedb-sql, authorize the final task set, and dispatch it.
///
/// Returns the rows a `RETURNING` clause on `sql` produced, empty when the
/// statement carries none. The rows are decoded from the Data Plane's own
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
) -> Result<Vec<DdlResult>, DdlError> {
    // The clause is stripped from the rebuilt statement before planning and
    // re-attached to each plan below — the planner itself does not parse it.
    let (sql, returning_spec) = returning::strip_returning(sql).map_err(|error| {
        let (_, sqlstate, message) = error_to_sqlstate(&error);
        ddl_err(sqlstate, message)
    })?;
    let sql = sql.as_str();
    // This is a client statement — the object-literal `INSERT INTO c { … }` and
    // `UPSERT` forms land here after being rewritten to standard SQL — so it
    // plans under the requester's own scope, the same one it is authorized and
    // metered as below. Planning it as the system would apply no row policy to
    // it: read filters would not be injected, and the write gates would decide
    // nothing, on a transport a client can reach directly.
    //
    // Injection happens HERE, before the task set is consumed: implicit-edge
    // extraction, authorization, staging, and dispatch all read `tasks` after
    // this point, and injecting later would hand them un-injected copies.
    let (mut tasks, output_schema, versions) = {
        let scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id);
        let permission_cache = state.permission_cache.read().await;
        let sec = PlanSecurityContext {
            identity,
            auth: scope.auth(),
            rls_store: &state.rls,
            redaction_store: &state.redaction,
            permissions: &state.permissions,
            roles: &state.roles,
            permission_cache: Some(&*permission_cache),
        };
        let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
        let (tasks, output_schema, versions, _) = query_ctx
            .plan_sql_with_rls_and_versions(
                sql,
                tenant_id,
                database_id,
                &sec,
                returning_spec.as_ref(),
            )
            .await
            .map_err(|error| {
                let (_, sqlstate, message) = error_to_sqlstate(&error);
                ddl_err(sqlstate, message)
            })?;
        (tasks, output_schema, versions)
    };

    // Attach the projection to every planned write, refusing any insert shape
    // that has nowhere to carry it rather than dropping the clause in silence.
    if let Some(ref spec) = returning_spec {
        for task in &mut tasks {
            returning::refuse_unprojectable_insert_returning(&task.plan).map_err(|error| {
                let (_, sqlstate, message) = error_to_sqlstate(&error);
                ddl_err(sqlstate, message)
            })?;
            returning::inject_returning_spec(&mut task.plan, spec.clone());
        }
    }

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
    // written since.
    let sum_target_reads =
        crate::control::planner::materialized_sum::resolve_materialized_sum_targets(
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
    let plan_lease_scope =
        Arc::new(state.acquire_plan_lease_scope(&versions).map_err(|error| {
            let (_, sqlstate, message) = error_to_sqlstate(&error);
            ddl_err(sqlstate, message)
        })?);

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

    if !in_txn_block
        && state.sequencer_inbox.get().is_some()
        && matches!(
            crate::control::planner::calvin::classify_dispatch(
                &tasks,
                &crate::control::planner::calvin::read_vshards_of(&sum_target_reads),
            ),
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
        // an empty row set, which would read as "the write matched nothing".
        if returning_spec.is_some() {
            return Err(ddl_err(
                "0A000",
                "RETURNING is not supported on a write that spans multiple shards",
            ));
        }
        return Ok(Vec::new());
    }

    let mut returned_rows: Option<ShapedRows> = None;
    let statement_buffer_start = txn_ctx.sessions.buffered_task_count(txn_ctx.session_id);
    for (task, initial_authorized) in tasks.into_iter().zip(authorized_tasks.into_tasks()) {
        let routed = route_in_tx_write(
            state,
            txn_ctx.sessions,
            txn_ctx.session_id,
            task,
            |staged| async move {
                let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
                match crate::control::server::shared::clone_write::intercept_and_authorize(
                    crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                        state,
                        task: staged,
                        identity,
                        tenant_id,
                        permissions: &state.permissions,
                        roles: &state.roles,
                        emitter: &emitter,
                    },
                )
                .await?
                {
                    crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(
                        resp,
                    ) => Ok(resp),
                    crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(
                        checked,
                    ) => {
                        crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
                            state,
                            checked,
                            TraceId::ZERO,
                        )
                        .await
                    }
                }
            },
        )
        .await;

        if txn_ctx.sessions.buffered_task_count(txn_ctx.session_id) > statement_buffer_start
            && !txn_ctx.sessions.attach_tx_lease_scope_since(
                txn_ctx.session_id,
                statement_buffer_start,
                Arc::clone(&plan_lease_scope),
            )
        {
            return Err(ddl_err(
                "XX000",
                "internal error: failed to retain descriptor leases for buffered transaction tasks",
            ));
        }

        let task = match routed {
            Ok(InTxnRoute::Read(task)) => *task,
            Ok(InTxnRoute::Buffered) | Ok(InTxnRoute::Staged(_)) => {
                drop(initial_authorized);
                // A buffered/staged write produces its rows at COMMIT, not
                // here, so the clause cannot be answered on this path. Refused
                // through the shared rule so this transport's message is the
                // one the pgwire and native loops give for the same limitation.
                if returning_spec.is_some() {
                    let (_, sqlstate, message) =
                        error_to_sqlstate(&returning::in_transaction_returning_unsupported());
                    return Err(ddl_err(sqlstate, message));
                }
                continue;
            }
            Err(StagingGateError::Dispatch(error)) => {
                let (_, sqlstate, message) = error_to_sqlstate(&error);
                return Err(ddl_err(sqlstate, message));
            }
            Err(StagingGateError::Rejected { code }) => {
                let (_, sqlstate, message) = match code {
                    Some(code) => error_code_to_sqlstate(&code),
                    None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
                };
                return Err(ddl_err(sqlstate, message));
            }
        };

        drop(initial_authorized);
        let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
        let response = match crate::control::server::shared::clone_write::intercept_and_authorize(
            crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                state,
                task: task.clone(),
                identity,
                tenant_id,
                permissions: &state.permissions,
                roles: &state.roles,
                emitter: &emitter,
            },
        )
        .await
        .map_err(|error| {
            let (_, sqlstate, message) = error_to_sqlstate(&error);
            ddl_err(sqlstate, message)
        })? {
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(resp) => resp,
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(checked) => {
                crate::control::server::dispatch_utils::dispatch_authorized_autocommit_write(
                    state,
                    checked,
                    TraceId::ZERO,
                )
                .await
                .map_err(|error| {
                    let (_, sqlstate, message) = error_to_sqlstate(&error);
                    ddl_err(sqlstate, message)
                })?
            }
        };

        if response.status == crate::bridge::envelope::Status::Error {
            let (_, sqlstate, message) = match response.error_code.as_deref() {
                Some(code) => error_code_to_sqlstate(code),
                None => (
                    "ERROR",
                    "XX000",
                    String::from_utf8_lossy(&response.payload).into_owned(),
                ),
            };
            return Err(ddl_err(sqlstate, message));
        }

        // Shape the STORED rows the write returned, redacted for the caller —
        // the same choke point the pgwire dispatch loop uses, so a redaction
        // policy masks identically on both transports.
        if returning_spec.is_some() {
            let scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id);
            let redaction = QueryRedaction::for_plan(tenant_id, scope.auth(), &task.plan);
            let outcome = shape_response_materialized(MaterializedShapeRequest {
                payload: response.payload.as_bytes(),
                plan: &task.plan,
                plan_kind: PlanKind::ReturningRows,
                // The statement's announced `RETURNING` columns, so this
                // transport renders a returned cell exactly as pgwire does.
                projection: Some(&output_schema),
                state,
                database_id,
                tenant_id,
                redaction: Some(redaction.ctx(&state.redaction)),
            })
            .map_err(|error| ddl_err("XX000", error.message().to_string()))?;
            // Folded rather than pushed: a statement is ONE result set, however
            // many tasks it planned to.
            if let ShapeOutcome::Rows(shaped) = outcome {
                match returned_rows {
                    Some(ref mut accumulated) => accumulated.append(shaped),
                    None => returned_rows = Some(shaped),
                }
            }
        }
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

fn authorization_error_to_ddl(error: AuthorizationError) -> DdlError {
    DdlError::new(
        nodedb_types::error::sqlstate::INSUFFICIENT_PRIVILEGE,
        error.resource().to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
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
