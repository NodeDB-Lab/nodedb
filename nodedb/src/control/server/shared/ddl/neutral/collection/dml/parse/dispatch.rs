// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::{
    AuthorizationError, authorize_collection, authorize_task_set,
};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::{
    DmlTxnCtx, InTxnRoute, StagingGateError, route_in_tx_write,
};
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
        plan: plan.clone(),
        post_set_op: nodedb_physical::physical_task::PostSetOp::None,
        txn_id: None,
    };
    if let Err(error) = authorize_final_task_set(state, identity, std::slice::from_ref(&task)) {
        return Some(Err(error));
    }

    if let Err(error) = crate::control::server::dispatch_utils::dispatch_autocommit_write(
        state,
        crate::control::server::dispatch_utils::AutocommitWrite {
            tenant_id: identity.tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id: TraceId::ZERO,
            event_source: crate::event::EventSource::User,
            txn_id: None,
        },
    )
    .await
    {
        return Some(Err(ddl_err("XX000", error.to_string())));
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
pub(in crate::control::server::shared::ddl::neutral::collection) async fn plan_and_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    database_id: crate::types::DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<(), DdlError> {
    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
    let (mut tasks, _output_schema) = query_ctx
        .plan_sql(sql, tenant_id, database_id)
        .await
        .map_err(|error| ddl_err("XX000", error.to_string()))?;

    // The final set includes implicit graph-edge writes and must be authorized
    // before Calvin classification, transaction staging, or local dispatch.
    crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        state,
        &mut tasks,
        tenant_id,
        database_id,
        TraceId::ZERO,
    )
    .await
    .map_err(|error| ddl_err("XX000", error.to_string()))?;

    authorize_final_task_set(state, identity, &tasks)?;

    if state.sequencer_inbox.get().is_some()
        && matches!(
            crate::control::planner::calvin::classify_dispatch(
                &tasks,
                &std::collections::BTreeSet::new(),
            ),
            crate::control::planner::calvin::DispatchClass::MultiShard { .. }
        )
    {
        crate::control::planner::calvin::dispatch_tasks_to_calvin(
            state,
            &tasks,
            tenant_id,
            crate::control::planner::calvin::CrossShardTxnMode::Strict,
            false,
        )
        .await
        .map_err(|error| ddl_err("XX000", error.to_string()))?;
        return Ok(());
    }

    for task in tasks {
        let task_vshard_id = task.vshard_id;
        let task_database_id = task.database_id;

        let routed = route_in_tx_write(txn_ctx.sessions, txn_ctx.addr, task, |staged| {
            crate::control::server::dispatch_utils::dispatch_to_data_plane_with_txn(
                state,
                staged.tenant_id,
                staged.database_id,
                staged.vshard_id,
                staged.plan,
                TraceId::ZERO,
                staged.txn_id,
            )
        })
        .await;

        let task = match routed {
            Ok(InTxnRoute::Read(task)) => *task,
            Ok(InTxnRoute::Buffered) | Ok(InTxnRoute::Staged(_)) => continue,
            Err(StagingGateError::Dispatch(error)) => {
                return Err(ddl_err("XX000", error.to_string()));
            }
            Err(StagingGateError::Rejected { code }) => {
                let (_, sqlstate, message) = match code {
                    Some(code) => error_code_to_sqlstate(&code),
                    None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
                };
                return Err(ddl_err(sqlstate, message));
            }
        };

        let response = crate::control::server::dispatch_utils::dispatch_autocommit_write(
            state,
            crate::control::server::dispatch_utils::AutocommitWrite {
                tenant_id,
                database_id: task_database_id,
                vshard_id: task_vshard_id,
                plan: task.plan,
                trace_id: TraceId::ZERO,
                event_source: crate::event::EventSource::User,
                txn_id: None,
            },
        )
        .await
        .map_err(|error| ddl_err("XX000", error.to_string()))?;

        if response.status == crate::bridge::envelope::Status::Error {
            let detail = match &response.error_code {
                Some(crate::bridge::envelope::ErrorCode::Internal { detail, .. }) => detail.clone(),
                Some(other) => format!("{other:?}"),
                None => String::from_utf8_lossy(&response.payload).into_owned(),
            };
            let sqlstate = if detail.to_lowercase().contains("unique") {
                "23505"
            } else {
                "XX000"
            };
            return Err(ddl_err(sqlstate, detail));
        }
    }
    Ok(())
}

fn authorize_final_task_set(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tasks: &[nodedb_physical::physical_task::PhysicalTask],
) -> Result<(), DdlError> {
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_task_set(identity, tasks, &state.permissions, &state.roles, &emitter)
        .map_err(authorization_error_to_ddl)
}

fn authorization_error_to_ddl(error: AuthorizationError) -> DdlError {
    DdlError {
        sqlstate: nodedb_types::error::sqlstate::INSUFFICIENT_PRIVILEGE.to_owned(),
        message: error.resource().to_owned(),
    }
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
