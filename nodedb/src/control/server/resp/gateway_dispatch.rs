// SPDX-License-Identifier: BUSL-1.1

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
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, RequestId, TraceId, VShardId};

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
    // RESP protocol carries no database selector; all ops target DatabaseId::DEFAULT.
    match state.gateway.as_ref() {
        Some(gw) => {
            let gw_ctx = QueryContext {
                tenant_id: session.tenant_id,
                trace_id: TraceId::generate(),
                database_id: DatabaseId::DEFAULT,
                txn_id: None,
            };
            gw.execute(&gw_ctx, plan)
                .await
                .map_err(|e| crate::Error::Bridge {
                    detail: GatewayErrorMap::to_resp(&e),
                })
                .map(gateway_payloads_to_response)
        }
        None => {
            let vshard =
                VShardId::from_collection_in_database(DatabaseId::DEFAULT, &session.collection);
            dispatch_utils::dispatch_to_data_plane(
                state,
                session.tenant_id,
                DatabaseId::DEFAULT,
                vshard,
                plan,
                TraceId::ZERO,
            )
            .await
            .map_err(map_busy_error)
        }
    }
}

/// Dispatch a KV write operation through the gateway or the local Data Plane.
///
/// Routes through the gateway when available (cluster-aware routing) — where the
/// gateway owns WAL durability on the target node — falling back to direct local
/// SPSC dispatch on single-node boot. On the local path the WAL append is
/// performed inside the dispatch core, under the write-admission guard and just
/// before the enqueue, so LSN order matches apply order.
pub(super) async fn dispatch_kv_write(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &session.collection);
    match state.gateway.as_ref() {
        Some(gw) => {
            let gw_ctx = QueryContext {
                tenant_id: session.tenant_id,
                trace_id: TraceId::generate(),
                database_id: DatabaseId::DEFAULT,
                txn_id: None,
            };
            gw.execute(&gw_ctx, plan)
                .await
                .map_err(|e| crate::Error::Bridge {
                    detail: GatewayErrorMap::to_resp(&e),
                })
                .map(gateway_payloads_to_response)
        }
        None => dispatch_utils::dispatch_autocommit_write(
            state,
            dispatch_utils::AutocommitWrite {
                tenant_id: session.tenant_id,
                database_id: DatabaseId::DEFAULT,
                vshard_id: vshard,
                plan,
                trace_id: TraceId::ZERO,
                event_source: crate::event::EventSource::User,
                txn_id: None,
            },
        )
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
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
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
