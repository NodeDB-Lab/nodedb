// SPDX-License-Identifier: BUSL-1.1

//! Gateway-based SQL task dispatch for the native protocol.
//!
//! When `SharedState.gateway` is `Some`, tasks are routed through
//! `Gateway::execute_response` which handles cluster-aware routing, typed `NotLeader`
//! retry, and plan caching. The `None` fallback retains the original
//! `dispatch_to_data_plane` path for single-node boot before the gateway is
//! wired. This is native's SQL-TEXT opcode path — distinct from
//! `raw_dispatch.rs`, which serves only native's direct-op opcodes.

use crate::bridge::envelope::Response;
use std::sync::Arc;

use crate::control::gateway::core::QueryContext as GatewayQueryContext;
use crate::control::gateway::router::is_task_vshard_scoped;
use crate::control::server::shared::clone_write::CloneCheckedOutcome;
use crate::types::TraceId;
use nodedb_physical::physical_task::PhysicalTask;

use super::DispatchCtx;

/// Authorize one task with no clone-write check — used only by the
/// Control-Plane orchestrator branches ahead of this file's gateway dispatch,
/// whose plan shapes (`InsertSelect`, `Merge`, `UpdateFromJoin`, a governed
/// predicate resolution, `DropArray`) are never clone-write shapes.
pub(super) fn authorize_native_task(
    ctx: &DispatchCtx<'_>,
    task: &PhysicalTask,
) -> crate::Result<crate::control::server::shared::authorization::AuthorizedTask> {
    let emitter = crate::control::security::audit::ArcAuditEmitter(Arc::clone(&ctx.state.audit));
    crate::control::server::shared::authorization::authorize_task_set(
        ctx.identity,
        std::slice::from_ref(task),
        &ctx.state.permissions,
        &ctx.state.roles,
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

/// Dispatch a single `PhysicalTask` through the gateway when available,
/// falling back to the local SPSC path.
///
/// Both paths return the Data-Plane `Response` shape, with a `NotFound`
/// verdict as an error status.
pub(super) async fn dispatch_task_via_gateway(
    ctx: &DispatchCtx<'_>,
    task: PhysicalTask,
) -> crate::Result<Response> {
    let tenant_id = task.tenant_id;
    // Clone CoW write-path interception, then authorization, run once per
    // task before dispatch — same protocol-neutral gate raw_dispatch runs.
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
    let database_id = checked.database_id();
    let txn_id = checked.txn_id();

    // A staged write and the other transaction meta-ops run on the core of
    // the task's own vShard. The gateway would route them to vShard 0.
    let gateway = ctx
        .state
        .gateway
        .get()
        .filter(|_| !is_task_vshard_scoped(checked.plan()));
    match gateway {
        Some(gw) => {
            let gw_ctx = GatewayQueryContext {
                tenant_id,
                trace_id: TraceId::generate(),
                database_id,
                // Propagate the in-block transaction id so gateway local
                // dispatch resolves the per-txn staging overlay.
                txn_id,
            };
            // The typed error passes through unchanged. The native frame
            // renders its SQLSTATE and numeric code from it.
            gw.execute_response(&gw_ctx, checked).await
        }
        // A write takes the durable route, a read the read route.
        None => {
            crate::control::server::dispatch_utils::dispatch_authorized_task_by_class(
                ctx.state,
                checked,
                TraceId::generate(),
            )
            .await
        }
    }
}
