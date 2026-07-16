// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::authorization::{authorize_database, authorize_task_set};

use super::super::super::auth::{ApiError, AppState, resolve_identity};
use super::super::result_shape::{HttpShaped, passthrough_to_ndjson, shape_http_payload};
use super::{DatabaseQueryParam, resolve_database_id};

/// POST /v1/query/stream — execute SQL and return results as NDJSON (newline-delimited JSON).
///
/// Each result row is a separate JSON line terminated by `\n`.
/// Content-Type: application/x-ndjson
///
/// This is suitable for streaming large result sets without buffering
/// the entire response. Clients can process each line as it arrives.
pub async fn query_ndjson(
    State(state): State<AppState>,
    headers: HeaderMap,
    QueryParams(db_param): QueryParams<DatabaseQueryParam>,
    axum::Json(body): axum::Json<crate::control::server::http::types::HttpQueryStreamRequest>,
) -> impl IntoResponse {
    use axum::response::Response;

    let identity = match resolve_identity(&headers, &state, "http") {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let database_id = match resolve_database_id(&headers, &db_param, &state) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    let emitter = ArcAuditEmitter(Arc::clone(&state.shared.audit));
    if let Err(error) = authorize_database(&identity, database_id, &emitter) {
        return ApiError::from(crate::Error::from(error)).into_response();
    }

    let sql = body.sql.trim();
    if sql.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty SQL").into_response();
    }

    let tenant_id = identity.tenant_id;

    // Quota enforcement — reject before any planning or dispatch.
    if let Err(e) = state.shared.check_tenant_quota(tenant_id) {
        let body = serde_json::json!({ "error": e.to_string() });
        return Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header("Retry-After", "1")
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response()
            });
    }

    let query_ctx = &state.query_ctx;

    let auth_ctx = crate::control::server::session_auth::build_auth_context(&identity);
    let perm_cache = state.shared.permission_cache.read().await;
    let sec = crate::control::planner::context::PlanSecurityContext {
        identity: &identity,
        auth: &auth_ctx,
        rls_store: &state.shared.rls,
        permissions: &state.shared.permissions,
        roles: &state.shared.roles,
        permission_cache: Some(&*perm_cache),
    };
    let (mut tasks, output_schema) = match query_ctx
        .plan_sql_with_rls(crate::control::planner::context::PlanSqlWithRlsParams {
            sql,
            tenant_id,
            database_id,
            sec: &sec,
        })
        .await
    {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    if let Err(error) = crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        &state.shared,
        &mut tasks,
        tenant_id,
        database_id,
        crate::types::TraceId::ZERO,
    )
    .await
    {
        return ApiError::from(error).into_response();
    }

    if let Err(error) = authorize_task_set(
        &identity,
        &tasks,
        &state.shared.permissions,
        &state.shared.roles,
        &emitter,
    ) {
        return ApiError::from(crate::Error::from(error)).into_response();
    }

    let trace_id = crate::control::trace_context::generate_trace_id();

    // Lazy fast path: an eligible single-task, unordered, multi-row SELECT
    // streams its rows straight off a `ResultStream` as NDJSON lines instead
    // of materializing the whole result first. HTTP is stateless, so there is
    // no autocommit / transaction-block gate (cf. native + pgwire). The
    // streaming body outlives this handler, so request-accounting is not
    // bracketed around it — admission was already gated by `check_tenant_quota`
    // above, matching the pgwire lazy path which also does not request-account
    // the streamed `QueryResponse`.
    match super::super::query_stream::try_open_stream(&state, &tasks, database_id, trace_id).await {
        Ok(Some((stream, limit))) => {
            let body =
                axum::body::Body::from_stream(super::super::query_stream::ndjson_body_stream(
                    stream,
                    limit,
                    Some(output_schema.clone()),
                ));
            return Response::builder()
                .header("Content-Type", "application/x-ndjson")
                .body(body)
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response()
                });
        }
        Ok(None) => {
            // Not streamable — fall through to the materialized path below.
        }
        Err(e) => {
            let (_status, msg) = GatewayErrorMap::to_http(&e);
            return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
        }
    }

    state.shared.tenant_request_start(tenant_id);

    let mut ndjson = String::new();
    for task in tasks {
        // Captured before dispatch moves `task.plan` — needed by the
        // protocol-neutral shaping core below.
        let plan_kind = describe_plan(&task.plan);
        let plan_for_shape = task.plan.clone();

        let dispatch_result: crate::Result<Vec<Vec<u8>>> = match state.shared.gateway.as_ref() {
            Some(gw) => {
                let gw_ctx = QueryContext {
                    tenant_id: task.tenant_id,
                    trace_id,
                    database_id,
                    txn_id: None,
                };
                gw.execute(&gw_ctx, task.plan).await
            }
            None => {
                // Single-node boot: gateway not yet initialised — dispatch locally.
                crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    &state.shared,
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

        match dispatch_result {
            Ok(payloads) => {
                for payload in &payloads {
                    if payload.is_empty() {
                        continue;
                    }
                    match shape_http_payload(
                        payload,
                        &plan_for_shape,
                        plan_kind,
                        Some(&output_schema),
                        &state.shared,
                        database_id,
                        tenant_id,
                    ) {
                        Ok(HttpShaped::Rows(rows)) => {
                            for row in rows {
                                ndjson.push_str(&row.to_string());
                                ndjson.push('\n');
                            }
                        }
                        Ok(HttpShaped::Passthrough) => {
                            passthrough_to_ndjson(payload, &mut ndjson);
                        }
                        Err(e) => {
                            ndjson.push_str(&serde_json::json!({"error": e.message()}).to_string());
                            ndjson.push('\n');
                        }
                    }
                }
            }
            Err(e) => {
                let (_status, msg) = GatewayErrorMap::to_http(&e);
                ndjson.push_str(&serde_json::json!({"error": msg}).to_string());
                ndjson.push('\n');
            }
        }
    }

    state.shared.tenant_request_end(tenant_id);

    Response::builder()
        .header("Content-Type", "application/x-ndjson")
        .body(axum::body::Body::from(ndjson))
        .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response())
}
