// SPDX-License-Identifier: BUSL-1.1

//! SQL execution via the SPSC gateway for WebSocket RPC.

use crate::control::gateway::core::QueryContext;
use crate::control::state::SharedState;
use crate::types::{TenantId, TraceId};

/// Execute SQL and return result as JSON.
///
/// Routes through the gateway when available (cluster-aware dispatch);
/// falls back to direct local SPSC dispatch on single-node boot before
/// the gateway is initialised.
pub async fn execute_sql(
    shared: &SharedState,
    query_ctx: &crate::control::planner::context::QueryContext,
    tenant_id: TenantId,
    sql: &str,
    trace_id: TraceId,
) -> crate::Result<serde_json::Value> {
    // Quota enforcement — reject before planning or dispatch.
    shared.check_tenant_quota(tenant_id)?;

    let (tasks, _output_schema) = query_ctx
        .plan_sql(sql, tenant_id, crate::types::DatabaseId::DEFAULT)
        .await?;

    shared.tenant_request_start(tenant_id);

    let mut results = Vec::new();
    for task in tasks {
        let payloads: crate::Result<Vec<Vec<u8>>> = match shared.gateway.as_ref() {
            Some(gw) => {
                let gw_ctx = QueryContext {
                    tenant_id: task.tenant_id,
                    trace_id,
                    database_id: nodedb_types::id::DatabaseId::DEFAULT,
                    // WebSocket RPC SQL exec is autocommit; no session txn block.
                    txn_id: None,
                };
                gw.execute(&gw_ctx, task.plan).await
            }
            None => {
                // Single-node boot: gateway not yet initialised — dispatch locally.
                crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    shared,
                    task.tenant_id,
                    task.database_id,
                    task.vshard_id,
                    task.plan,
                    trace_id,
                )
                .await
                .map(|r| vec![r.payload.to_vec()])
            }
        };

        match payloads {
            Ok(vecs) => {
                for payload in vecs {
                    if !payload.is_empty() {
                        let json =
                            crate::data::executor::response_codec::decode_payload_to_json(&payload);
                        match sonic_rs::from_str::<serde_json::Value>(&json) {
                            Ok(v) => results.push(v),
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
            }
            Err(e) => {
                shared.tenant_request_end(tenant_id);
                return Err(e);
            }
        }
    }

    shared.tenant_request_end(tenant_id);

    match results.len() {
        0 => Ok(serde_json::Value::Null),
        1 => Ok(results
            .into_iter()
            .next()
            .unwrap_or(serde_json::Value::Null)),
        _ => Ok(serde_json::Value::Array(results)),
    }
}
