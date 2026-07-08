// SPDX-License-Identifier: BUSL-1.1

//! Gateway-based SQL task dispatch for the native protocol.
//!
//! When `SharedState.gateway` is `Some`, tasks are routed through
//! `Gateway::execute` which handles cluster-aware routing, typed `NotLeader`
//! retry, and plan caching. The `None` fallback retains the original
//! `dispatch_to_data_plane` path for single-node boot before the gateway is
//! wired.

use crate::bridge::envelope::{Payload, Response, Status};
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext as GatewayQueryContext;
use crate::control::server::{dispatch_utils, wal_dispatch};
use crate::types::{Lsn, RequestId, TraceId};
use nodedb_physical::physical_task::PhysicalTask;

use super::DispatchCtx;

/// Dispatch a single `PhysicalTask` through the gateway when available,
/// falling back to the local SPSC path.
///
/// Returns a synthetic `Response` shaped identically to the SPSC path so that
/// the calling code in `sql.rs` is unchanged.
pub(super) async fn dispatch_task_via_gateway(
    ctx: &DispatchCtx<'_>,
    task: PhysicalTask,
) -> crate::Result<Response> {
    // Pre-compute routing identity before plan is moved.
    let vshard_id = task.vshard_id;
    let tenant_id = task.tenant_id;
    let database_id = task.database_id;
    let txn_id = task.txn_id;
    let plan = task.plan;

    match ctx.state.gateway.as_ref() {
        Some(gw) => {
            let gw_ctx = GatewayQueryContext {
                tenant_id,
                trace_id: TraceId::generate(),
                database_id,
                txn_id,
            };
            gw.execute(&gw_ctx, plan)
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
            // Boot fallback: no gateway yet, dispatch locally.
            let wal_lsn = wal_dispatch::wal_append_if_write(
                &ctx.state.wal,
                tenant_id,
                vshard_id,
                database_id,
                &plan,
            )?;
            dispatch_utils::dispatch_write_to_data_plane(
                ctx.state,
                dispatch_utils::WriteDispatch {
                    tenant_id,
                    database_id,
                    vshard_id,
                    plan,
                    trace_id: TraceId::ZERO,
                    event_source: crate::event::EventSource::User,
                    txn_id,
                    wal_lsn,
                },
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
    }
}
