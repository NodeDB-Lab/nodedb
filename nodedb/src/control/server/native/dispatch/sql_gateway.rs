// SPDX-License-Identifier: BUSL-1.1

//! Gateway-based SQL task dispatch for the native protocol.
//!
//! When `SharedState.gateway` is `Some`, tasks are routed through
//! `Gateway::execute` which handles cluster-aware routing, typed `NotLeader`
//! retry, and plan caching. The `None` fallback retains the original
//! `dispatch_to_data_plane` path for single-node boot before the gateway is
//! wired. This is native's SQL-TEXT opcode path — distinct from
//! `raw_dispatch.rs`, which serves only native's direct-op opcodes.

use crate::bridge::envelope::{Payload, Response, Status};
use std::sync::Arc;

use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext as GatewayQueryContext;
use crate::control::gateway::router::is_task_vshard_scoped;
use crate::control::server::shared::clone_write::CloneCheckedOutcome;
use crate::types::{Lsn, RequestId, TraceId};
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
/// Returns a synthetic `Response` shaped identically to the SPSC path so that
/// the calling code in `sql.rs` is unchanged.
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
            gw.execute(&gw_ctx, checked)
                .await
                .map_err(|e| {
                    let (code, msg) = GatewayErrorMap::to_native(&e);
                    crate::Error::Internal {
                        detail: format!("gateway error {code}: {msg}"),
                    }
                })
                .map(payloads_to_response)
        }
        None => {
            crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
                ctx.state,
                checked,
                TraceId::generate(),
            )
            .await
        }
    }
}

/// Convert gateway `Vec<Vec<u8>>` payloads into a synthetic `Response`.
///
/// Mirrors the same conversion used in the RESP gateway_dispatch module:
/// the first payload is used as the response body; an empty `Vec` yields an
/// empty payload with `Status::Ok`.
fn payloads_to_response(payloads: Vec<Vec<u8>>) -> Response {
    let payload = payloads
        .into_iter()
        .next()
        .map(Payload::from_vec)
        .unwrap_or_else(Payload::empty);
    Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 0,
        partial: false,
        payload,
        watermark_lsn: Lsn::new(0),
        error_code: None,
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    }
}
