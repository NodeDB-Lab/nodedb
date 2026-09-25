// SPDX-License-Identifier: BUSL-1.1

//! Raw native-operation dispatch shared by direct-op handlers.

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use std::sync::Arc;

use crate::control::gateway::core::QueryContext as GatewayQueryContext;
use crate::control::gateway::router::is_task_vshard_scoped;
use crate::control::server::shared::clone_write::CloneCheckedOutcome;
use crate::types::{Lsn, RequestId, TenantId, TraceId, TxnId, VShardId};

use super::super::super::dispatch_utils;
use super::DispatchCtx;

pub(super) async fn dispatch_authorized_single_task(
    ctx: &DispatchCtx<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    match &plan {
        PhysicalPlan::Crdt(nodedb_physical::physical_plan::CrdtOp::Apply { .. }) => {
            return dispatch_external_crdt_apply(ctx, tenant_id, plan, txn_id).await;
        }
        PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
            | nodedb_physical::physical_plan::CrdtOp::ImportSnapshot { .. },
        ) => return Err(crate::Error::CrdtApplyRequiresAdmission),
        _ => {}
    }
    let task = nodedb_physical::physical_task::PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: ctx.database_id(),
        plan,
        post_set_op: nodedb_physical::physical_task::PostSetOp::None,
        txn_id,
    };
    // Clone CoW write-path interception, then authorization, run once per
    // task before dispatch — same protocol-neutral gate pgwire and RESP run.
    let emitter = crate::control::security::audit::ArcAuditEmitter(Arc::clone(&ctx.state.audit));
    let checked = match crate::control::server::shared::clone_write::intercept_and_authorize(
        crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
            state: ctx.state,
            task,
            identity: ctx.identity,
            tenant_id,
            permissions: &ctx.state.permissions,
            roles: &ctx.state.roles,
            emitter: &emitter,
        },
    )
    .await?
    {
        CloneCheckedOutcome::Handled(resp) => return Ok(resp),
        CloneCheckedOutcome::Proceed(checked) => checked,
    };
    // A write whose RLS write policy is decided per row cannot be proposed
    // bare: a follower has no writing identity to decide it against. It
    // resolves to a concrete row set here, while the identity is live, the
    // way the planned native and pgwire writes resolve.
    if checked.txn_id().is_none()
        && ctx.state.async_raft_proposer().is_some()
        && let Some(resolver) = crate::control::write_resolve::resolver_for_plan(checked.plan())
    {
        return crate::control::write_resolve::run_authorized_write_resolve(
            ctx.state,
            checked.into_authorized(),
            resolver,
        )
        .await;
    }
    // A staged write and the other transaction meta-ops run on the core of
    // the task's own vShard. The gateway would route them to vShard 0.
    let gateway = ctx
        .state
        .gateway
        .get()
        .filter(|_| !is_task_vshard_scoped(checked.plan()));
    match gateway {
        Some(gateway) => {
            let query = GatewayQueryContext {
                tenant_id,
                trace_id: TraceId::generate(),
                database_id: ctx.database_id(),
                txn_id,
            };
            // The typed error passes through unchanged. The native frame
            // renders its SQLSTATE and numeric code from it.
            gateway.execute_response(&query, checked).await
        }
        None => dispatch_without_gateway(ctx, checked).await,
    }
}

async fn dispatch_external_crdt_apply(
    ctx: &DispatchCtx<'_>,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    if txn_id.is_some() {
        return Err(crate::Error::CrdtApplyForbiddenInTransaction);
    }
    let PhysicalPlan::Crdt(nodedb_physical::physical_plan::CrdtOp::Apply { collection, .. }) =
        &plan
    else {
        return Err(crate::Error::CrdtApplyRequiresAdmission);
    };
    let collection = collection.clone();
    let audit =
        crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&ctx.state.audit));
    crate::control::server::shared::authorization::authorize_collection(
        ctx.identity,
        ctx.database_id(),
        collection.as_str(),
        crate::control::security::identity::Permission::Write,
        &ctx.state.permissions,
        &ctx.state.roles,
        &audit,
    )
    .map_err(crate::Error::from)?;
    let task = nodedb_physical::physical_task::PhysicalTask {
        tenant_id,
        vshard_id: VShardId::from_collection_in_database(ctx.database_id(), collection.as_str()),
        database_id: ctx.database_id(),
        plan,
        post_set_op: nodedb_physical::physical_task::PostSetOp::None,
        txn_id: None,
    };
    let authorized = crate::control::server::shared::authorization::authorize_task_set(
        ctx.identity,
        std::slice::from_ref(&task),
        &ctx.state.permissions,
        &ctx.state.roles,
        &audit,
    )
    .map_err(crate::Error::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| crate::Error::Internal {
        detail: "authorization returned no capability".into(),
    })?;
    // RLS write policies are stored keyed by the database-qualified
    // collection, so the policy is handed that same key or it silently
    // misses a policy on a non-default database. `collection` is already
    // qualified here (the plan builder qualifies it against `ctx.database_id()`
    // before this dispatch runs), so no re-qualification happens — doing so
    // would double-prefix the name.
    let qualified_collection = collection.as_str();
    let policy = crate::control::crdt_post_image_policy::ExternalCrdtPostImagePolicy::from_identity(
        tenant_id,
        ctx.database_id(),
        qualified_collection,
        ctx.identity,
        "native".into(),
        &ctx.state.rls,
        &audit,
    );
    let outcome = crate::control::crdt_admission::dispatch_authorized_crdt_apply_admitted_outcome(
        ctx.state,
        crate::control::crdt_admission::AuthorizedCrdtApplyAdmissionRequest {
            authorized,
            collection: collection.as_str(),
            timeout: std::time::Duration::from_secs(ctx.state.tuning.network.default_deadline_secs),
            event_source: crate::event::EventSource::User,
            policy: &policy,
        },
    )
    .await?;
    Ok(Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Payload::from_vec(outcome.payload),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        read_set_valid: None,
        read_version_lsn: outcome.write_version,
        write_set: Vec::new(),
    })
}

pub(super) async fn dispatch_without_gateway(
    ctx: &DispatchCtx<'_>,
    checked: crate::control::server::shared::clone_write::CloneCheckedTask,
) -> crate::Result<Response> {
    let vshard_id = checked.vshard_id();
    let frontier_mutation = checked.txn_id().is_none()
        && matches!(
            checked.plan(),
            PhysicalPlan::Crdt(op)
                if crate::control::crdt_admission::changes_crdt_frontier(op)
        );
    let write = || async move {
        dispatch_utils::dispatch_authorized_durable_write(ctx.state, checked, TraceId::ZERO).await
    };
    if frontier_mutation {
        ctx.state
            .vshard_admission_sequencer
            .run(vshard_id, write)
            .await
    } else {
        write().await
    }
}
