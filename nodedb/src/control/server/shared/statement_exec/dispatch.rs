// SPDX-License-Identifier: BUSL-1.1

//! The single-task dispatch primitive a planned statement's loop calls.
//!
//! Decides HOW one already-planned task reaches an engine: a Control-Plane
//! orchestrator, Exchange resolution, then the gateway. The statement loop
//! decides what to do with the answers.

use std::sync::Arc;

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::gateway::core::QueryContext as GatewayQueryContext;
use crate::control::gateway::router::is_task_vshard_scoped;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::resolve::{Resolved, resolve_and_materialize};
use crate::control::server::exchange::{DistributedReadCapture, ReadScope};
use crate::control::server::shared::authorization::{AuthorizedTask, authorize_task_set};
use crate::control::server::shared::clone_write::{
    CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use crate::control::state::SharedState;
use crate::types::TraceId;

/// One dispatched task's answer.
pub(crate) struct DispatchedTask {
    pub(crate) response: Response,
    /// The reads a distributed gather captured.
    pub(crate) distributed_reads: Vec<DistributedReadCapture>,
}

impl DispatchedTask {
    fn answered(response: Response) -> Self {
        Self {
            response,
            distributed_reads: Vec::new(),
        }
    }
}

/// Authorize one task with no clone-write check. Used only for the
/// Control-Plane orchestrated plans (`InsertSelect`, `Merge`,
/// `UpdateFromJoin`, a governed predicate resolution, array DDL, a cluster
/// array op), which are never clone-write shapes.
pub(crate) fn authorize_one_task(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    task: &PhysicalTask,
) -> crate::Result<AuthorizedTask> {
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_task_set(
        identity,
        std::slice::from_ref(task),
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| crate::Error::Internal {
        detail: "authorization returned an empty capability set".into(),
    })
}

/// Dispatch one `PhysicalTask` and return its answer.
///
/// Multi-arm writes run through Control-Plane orchestrators. Everything else
/// resolves its Exchange nodes, then routes through
/// [`dispatch_task_via_gateway`].
pub(crate) async fn dispatch_statement_task(
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    mut task: PhysicalTask,
) -> crate::Result<DispatchedTask> {
    if let PhysicalPlan::Document(DocumentOp::InsertSelect { .. }) = &task.plan {
        let authorized = authorize_one_task(state, identity, &task)?;
        let response =
            crate::control::insert_select::run_authorized_insert_select(state, authorized).await?;
        return Ok(DispatchedTask::answered(response));
    }

    // Autocommit `MERGE` orchestrates on the Control Plane.
    if let PhysicalPlan::Document(DocumentOp::Merge {
        target_collection: _,
        source_collection: _,
        source_alias: _,
        target_join_col: _,
        source_join_col: _,
        clauses: _,
        returning: _,
        resolved_inserts: None,
        resolved_insert_identities: _,
        source_rows: _,
        rls_filters: _,
        rls_write_check: _,
        resolved_sum_targets: _,
        declared_primary_key: _,
    }) = &task.plan
    {
        let authorized = authorize_one_task(state, identity, &task)?;
        let response =
            crate::control::merge_orchestrator::run_authorized_merge(state, authorized).await?;
        return Ok(DispatchedTask::answered(response));
    }

    // Autocommit `UPDATE ... FROM <source>` scans the source on its own core
    // and ships it into the plan, since the source's vShard can live on a
    // different core.
    if let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection: _,
        source_collection: _,
        source_alias: _,
        target_join_col: _,
        source_join_col: _,
        updates: _,
        target_filters: _,
        returning: _,
        source_rows: None,
        rls_filters: _,
        rls_write_check: _,
        resolved_sum_targets: _,
        declared_primary_key: _,
    }) = &task.plan
    {
        let authorized = authorize_one_task(state, identity, &task)?;
        let response =
            crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                state, authorized,
            )
            .await?;
        return Ok(DispatchedTask::answered(response));
    }

    // A governed predicate resolves to a concrete row set before proposing.
    if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&task.plan) {
        let authorized = authorize_one_task(state, identity, &task)?;
        let response = crate::control::write_resolve::run_authorized_write_resolve(
            state, authorized, resolver,
        )
        .await?;
        return Ok(DispatchedTask::answered(response));
    }

    // Array DDL proposes a replicated catalog entry.
    if crate::control::array_catalog::ddl::is_array_ddl(&task.plan) {
        let authorized = authorize_one_task(state, identity, &task)?;
        let response =
            crate::control::array_catalog::ddl::run_authorized_array_ddl(state, authorized).await?;
        return Ok(DispatchedTask::answered(response));
    }

    // Materialize catalog providers and resolve Exchange nodes before
    // dispatch. Every statement read is strong, so every leg confirms its
    // group where it is served.
    let scope = ReadScope {
        database_id: task.database_id,
        tenant_id: task.tenant_id,
        trace_id: TraceId::ZERO,
        txn_id: task.txn_id,
        linearizable: true,
    };
    match resolve_and_materialize(state, identity, task.plan, scope).await? {
        Resolved::Gathered(response, _watermarks, distributed_reads) => {
            return Ok(DispatchedTask {
                response,
                distributed_reads,
            });
        }
        Resolved::Plan(resolved_plan) => {
            task.plan = *resolved_plan;
        }
        // A materialized statement gathers the stream into one response.
        Resolved::Stream(stream) => {
            let response =
                crate::control::server::exchange::gather::stream_to_response(stream).await?;
            return Ok(DispatchedTask::answered(response));
        }
    }

    let response = dispatch_task_via_gateway(state, identity, task).await?;
    Ok(DispatchedTask::answered(response))
}

/// Dispatch one `PhysicalTask` through the gateway.
///
/// The task takes the clone copy-on-write gate and authorization first. A
/// staged write and the other transaction meta-ops run on the core of the
/// task's own vShard. Every other task routes through
/// `Gateway::execute_response`, which handles cluster-aware routing, typed
/// `NotLeader` retry and plan caching. A `NotFound` verdict comes back as an
/// error status.
pub(crate) async fn dispatch_task_via_gateway(
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    task: PhysicalTask,
) -> crate::Result<Response> {
    let tenant_id = task.tenant_id;
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    let checked = match intercept_and_authorize(InterceptAndAuthorizeParams {
        state,
        task,
        identity,
        tenant_id,
        permissions: &state.permissions,
        roles: &state.roles,
        emitter: &emitter,
    })
    .await?
    {
        CloneCheckedOutcome::Handled(response) => return Ok(response),
        CloneCheckedOutcome::Proceed(checked) => checked,
    };
    let database_id = checked.database_id();
    let txn_id = checked.txn_id();

    let gateway = state.installed_gateway()?;
    // The gateway routes a vShard-scoped meta-op to vShard 0. A write takes
    // the durable route, a read the read route.
    if is_task_vshard_scoped(checked.plan()) {
        return crate::control::server::dispatch_utils::dispatch_authorized_task_by_class(
            state,
            checked,
            TraceId::generate(),
        )
        .await;
    }
    let gw_ctx = GatewayQueryContext {
        tenant_id,
        trace_id: TraceId::generate(),
        database_id,
        // The in-block transaction id lets local dispatch resolve the
        // per-transaction staging overlay.
        txn_id,
        linearizable: true,
    };
    gateway.execute_response(&gw_ctx, checked).await
}
