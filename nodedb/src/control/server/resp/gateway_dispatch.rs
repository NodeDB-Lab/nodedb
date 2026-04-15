//! RESP gateway dispatch helpers.
//!
//! Routes KV operations through `Gateway::execute` when the gateway is
//! available (cluster-aware routing), falling back to direct local SPSC
//! dispatch on single-node boot.
//!
//! All helpers return `crate::Result<Response>` so the existing sub-handler
//! code (`handler_kv`, `handler_hash`, `handler_sorted`) is unchanged.

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::server::dispatch_utils;
use crate::control::server::wal_dispatch;
use crate::control::state::SharedState;
use crate::types::{Lsn, RequestId, VShardId};

use super::session::RespSession;

/// Dispatch a read-only KV operation.
///
/// Routes through the gateway when available (cluster-aware routing), falling
/// back to direct local SPSC dispatch on single-node boot.
///
/// Bridge/dispatch errors are mapped to `Error::Bridge` with a `BUSY` detail
/// so the RESP handler can return `-BUSY` to the Redis client.
pub(super) async fn dispatch_kv(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    match state.gateway.as_ref() {
        Some(gw) => {
            let gw_ctx = QueryContext {
                tenant_id: session.tenant_id,
                trace_id: 0,
            };
            gw.execute(&gw_ctx, plan)
                .await
                .map_err(|e| crate::Error::Bridge {
                    detail: GatewayErrorMap::to_resp(&e),
                })
                .map(gateway_payloads_to_response)
        }
        None => {
            let vshard = VShardId::from_collection(&session.collection);
            dispatch_utils::dispatch_to_data_plane(state, session.tenant_id, vshard, plan, 0)
                .await
                .map_err(map_busy_error)
        }
    }
}

/// Dispatch a KV write operation: WAL append first, then gateway or Data Plane.
///
/// Routes through the gateway when available (cluster-aware routing), falling
/// back to direct local SPSC dispatch on single-node boot.
pub(super) async fn dispatch_kv_write(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    let vshard = VShardId::from_collection(&session.collection);
    wal_dispatch::wal_append_if_write(&state.wal, session.tenant_id, vshard, &plan)?;
    match state.gateway.as_ref() {
        Some(gw) => {
            let gw_ctx = QueryContext {
                tenant_id: session.tenant_id,
                trace_id: 0,
            };
            gw.execute(&gw_ctx, plan)
                .await
                .map_err(|e| crate::Error::Bridge {
                    detail: GatewayErrorMap::to_resp(&e),
                })
                .map(gateway_payloads_to_response)
        }
        None => dispatch_utils::dispatch_to_data_plane(state, session.tenant_id, vshard, plan, 0)
            .await
            .map_err(map_busy_error),
    }
}

/// Convert gateway `Vec<Vec<u8>>` payloads into a synthetic `Response`.
///
/// The RESP sub-handlers inspect `resp.status` and `resp.payload`; we
/// synthesise a `Status::Ok` response carrying the first payload so that all
/// existing sub-handler logic continues to work without modification.
fn gateway_payloads_to_response(payloads: Vec<Vec<u8>>) -> Response {
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

/// Map bridge/dispatch errors to a BUSY error for Redis client compatibility.
///
/// When the SPSC ring buffer is full or the Data Plane core is overloaded,
/// the Redis client receives `-BUSY NodeDB is processing requests, retry later`
/// which Redis clients handle with automatic retry (same as Redis Cluster BUSY).
fn map_busy_error(e: crate::Error) -> crate::Error {
    match &e {
        crate::Error::Bridge { .. } | crate::Error::Dispatch { .. } => crate::Error::Bridge {
            detail: "BUSY NodeDB is processing requests, retry later".into(),
        },
        _ => e,
    }
}

/// Parse a JSON payload and extract an integer field.
pub(super) fn parse_json_field_i64(
    payload: &crate::bridge::envelope::Payload,
    field: &str,
) -> Option<i64> {
    let json: serde_json::Value = sonic_rs::from_slice(payload).ok()?;
    json.get(field)?.as_i64()
}
