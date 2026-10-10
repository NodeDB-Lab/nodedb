// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::sequence::SessionSequenceAccess;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::cluster_array_dispatch::{is_cluster_array, run_cluster_array};
use crate::control::server::shared::metering::{
    DetachedMeterGuard, PlanMeteringInfo, meter_dispatch,
};
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::statement_exec::{
    AtomicStatement, StatementExec, authorize_one_task,
};

use super::super::super::auth::{ApiError, AppState, build_request_scope, resolve_auth_parts};
use super::super::super::peer::PeerAddr;
use super::super::super::transport::ClientTransport;
use super::super::atomic_statement::{RoutedStatement, route_http_statement};
use super::super::query_stream::{NdjsonBody, ndjson_body_stream, try_open_stream};
use super::super::result_shape::{HttpShaped, passthrough_to_ndjson, shape_http_payload};
use super::{DatabaseQueryParam, resolve_database_id};

/// POST /v1/query/stream — execute SQL, return results as NDJSON.
///
/// Each row is a separate JSON line (`\n`-terminated), Content-Type
/// `application/x-ndjson`, so clients can process large result sets unbuffered.
pub async fn query_ndjson(
    State(state): State<AppState>,
    headers: HeaderMap,
    peer: PeerAddr,
    transport: ClientTransport,
    QueryParams(db_param): QueryParams<DatabaseQueryParam>,
    axum::Json(body): axum::Json<crate::control::server::http::types::HttpQueryStreamRequest>,
) -> impl IntoResponse {
    use axum::response::Response;

    let (identity, verified_jwt) =
        match resolve_auth_parts(&headers, &state, peer.as_str(), transport.security()).await {
            Ok(auth) => auth,
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

    // Passing database as the session database makes `scope.database_id()` resolve to it.
    // NDJSON does not extract a per-query `ON DENY` clause (unlike `/v1/query`).
    let request = build_request_scope(
        &identity,
        verified_jwt.as_ref(),
        &headers,
        &state,
        database_id,
        peer.as_str(),
    );

    // Admission gate (exemption, blacklist, account status, rate limit) runs
    // before planning/dispatch, so load sheds before it's spent.
    let rate_limit_result = match crate::control::server::session_auth::check_request_admission(
        &state.shared,
        &request,
        "sql",
    ) {
        Ok(result) => result,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let scope = request.into_resolved_scope();
    let rate_limit_headers =
        super::super::super::rate_limit_headers::rate_limit_headers(&rate_limit_result);

    // Planning and lease admission run as one retried unit; a denied request never
    // acquires a lease.
    let admission = match plan_authorize_and_admit(PlanAdmissionRequest {
        state: &state.shared,
        query_ctx,
        scope: &scope,
        sql,
        trace_id: crate::types::TraceId::ZERO,
    })
    .await
    {
        Ok(admission) => admission,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let output_schema = admission.output_schema;

    let trace_id = crate::control::trace_context::generate_trace_id();

    let _request = state.shared.tenant_request_guard(tenant_id);

    // The shared atomic routes run a statement whose tasks commit together:
    // the implicit transaction, the implicit-edge gate, and a Calvin commit
    // for a write that spans vShards. HTTP carries no session: `nextval`
    // advances the registry, `currval` reports "not yet called in this
    // session".
    let sequences = SessionSequenceAccess::for_session(&state.shared, None, database_id, tenant_id);
    let exec = StatementExec {
        state: &state.shared,
        identity: &identity,
        scope: &scope,
        output_schema: Some(&output_schema),
        database_id,
        sequences: &sequences,
    };
    let routed = route_http_statement(
        &exec,
        AtomicStatement {
            tasks: admission.tasks,
            lease_scope: admission.lease_scope,
            sum_target_reads: admission.sum_target_reads,
        },
    )
    .await;
    let (tasks, lease_scope) = match routed {
        Ok(RoutedStatement::PerTask(statement)) => (statement.tasks, statement.lease_scope),
        Ok(RoutedStatement::Answered(rows)) => {
            let mut ndjson = String::new();
            for row in rows {
                ndjson.push_str(&row.to_string());
                ndjson.push('\n');
            }
            return ndjson_response(ndjson, rate_limit_headers);
        }
        // A statement that stopped answers one error line, as a task that
        // stopped in the loop below does.
        Err(error) => {
            let (_status, message) = GatewayErrorMap::to_http(&error);
            let line = format!("{}\n", serde_json::json!({ "error": message }));
            return ndjson_response(line, rate_limit_headers);
        }
    };
    let mut lease_scope = Some(lease_scope);

    // `Body::from_stream` polls the data-plane stream under normal HTTP backpressure
    // while its captured lease scope stays alive until body completion or disconnect.
    match try_open_stream(
        &state,
        &tasks,
        &identity,
        database_id,
        &output_schema,
        trace_id,
    )
    .await
    {
        Ok(Some((stream, limit))) => {
            let Some(lease_scope) = lease_scope.take() else {
                return ApiError::from(crate::Error::Internal {
                    detail: "query lease scope missing before NDJSON stream dispatch".into(),
                })
                .into_response();
            };
            // Streaming body owns the guard, so only rows actually sent are billed;
            // see `DetachedMeterGuard`.
            let stream_meter_guard = if state.shared.metering_config.enabled
                && let [task] = tasks.as_slice()
            {
                let info = PlanMeteringInfo::extract(&task.plan);
                DetachedMeterGuard::new(&state.shared, &scope, &info)
            } else {
                None
            };
            let mut response = Response::builder()
                .header("Content-Type", "application/x-ndjson")
                .body(axum::body::Body::from_stream(ndjson_body_stream(
                    NdjsonBody {
                        stream,
                        limit,
                        projection: Some(output_schema.clone()),
                        // `try_open_stream` returns `Some` only for a single-task plan.
                        redaction: tasks.first().map(|task| {
                            QueryRedaction::for_plan(tenant_id, scope.auth(), &task.plan)
                        }),
                        state: Arc::clone(&state.shared),
                        lease_scope,
                        meter_guard: stream_meter_guard,
                    },
                )))
                .unwrap_or_else(|_| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response()
                });
            response.headers_mut().extend(rate_limit_headers);
            return response;
        }
        Ok(None) => {}
        Err(error) => return ApiError::from(error).into_response(),
    }

    // Held until the materialized body is built.
    let Some(lease_scope) = lease_scope.take() else {
        return ApiError::from(crate::Error::Internal {
            detail: "query lease scope missing before NDJSON dispatch".into(),
        })
        .into_response();
    };
    let mut ndjson = String::new();
    // Checked once, not per task: keeps per-task extraction a no-op when metering is
    // disabled. This fallback fully materializes the body, so it meters like `/v1/query`.
    let metering_enabled = state.shared.metering_config.enabled;
    for task in tasks {
        // A lease this node lost ends the body with a retryable error line
        // before the next task dispatches.
        if let Err(revoked) = lease_scope.check_not_revoked() {
            ndjson.push_str(&serde_json::json!({"error": revoked.to_string()}).to_string());
            ndjson.push('\n');
            break;
        }
        // A spent hard quota refuses the task before it runs; reported as an error
        // line and the task skipped, matching this stream's error reporting.
        let plan_metering_info = metering_enabled.then(|| PlanMeteringInfo::extract(&task.plan));
        if let Some(info) = &plan_metering_info
            && let Err(e) = admit_quota_for_dispatch(&state.shared, &scope, info)
        {
            ndjson.push_str(&serde_json::json!({"error": e.to_string()}).to_string());
            ndjson.push('\n');
            continue;
        }

        // Captured before dispatch moves `task.plan` — needed by shaping below.
        let plan_kind = describe_plan(&task.plan);
        let plan_for_shape = task.plan.clone();
        // Resolved once per task, reused for every payload it produced.
        let redaction = QueryRedaction::for_plan(tenant_id, scope.auth(), &plan_for_shape);

        // A read is cancelled mid-flight once this node loses a lease it
        // holds. A write is not: its outcome will be unknown to the client.
        let task_is_write =
            crate::control::server::shared::write_admission::plan_is_write(&task.plan);
        let dispatch = async {
            let result: crate::Result<Vec<Vec<u8>>> =
                if crate::control::array_catalog::ddl::is_array_ddl(&task.plan) {
                    match authorize_one_task(&state.shared, &identity, &task) {
                        Ok(authorized_task) => {
                            crate::control::array_catalog::ddl::run_authorized_array_ddl(
                                &state.shared,
                                authorized_task,
                            )
                            .await
                            .map(|response| vec![response.payload.to_vec()])
                        }
                        Err(e) => Err(e),
                    }
                } else if is_cluster_array(&task.plan) {
                    // This node's array coordinator routes the op to the
                    // shards that own its cells.
                    match authorize_one_task(&state.shared, &identity, &task) {
                        Ok(authorized_task) => run_cluster_array(&state.shared, authorized_task)
                            .await
                            .map(|payload| vec![payload]),
                        Err(e) => Err(e),
                    }
                } else if matches!(
                    &task.plan,
                    crate::bridge::envelope::PhysicalPlan::Document(
                        nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. }
                    )
                ) {
                    match authorize_one_task(&state.shared, &identity, &task) {
                        Ok(authorized_task) => {
                            crate::control::insert_select::run_authorized_insert_select(
                                &state.shared,
                                authorized_task,
                            )
                            .await
                            .map(|response| vec![response.payload.to_vec()])
                        }
                        Err(e) => Err(e),
                    }
                } else if matches!(
                    &task.plan,
                    crate::bridge::envelope::PhysicalPlan::Document(
                        nodedb_physical::physical_plan::DocumentOp::Merge {
                            resolved_inserts: None,
                            ..
                        }
                    )
                ) {
                    match authorize_one_task(&state.shared, &identity, &task) {
                        Ok(authorized_task) => {
                            crate::control::merge_orchestrator::run_authorized_merge(
                                &state.shared,
                                authorized_task,
                            )
                            .await
                            .map(|response| vec![response.payload.to_vec()])
                        }
                        Err(e) => Err(e),
                    }
                } else if matches!(
                    &task.plan,
                    crate::bridge::envelope::PhysicalPlan::Document(
                        nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                            source_rows: None,
                            ..
                        }
                    )
                ) {
                    match authorize_one_task(&state.shared, &identity, &task) {
                    Ok(authorized_task) => {
                        crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                            &state.shared,
                            authorized_task,
                        )
                        .await
                        .map(|response| vec![response.payload.to_vec()])
                    }
                    Err(e) => Err(e),
                }
                } else if let Some(resolver) =
                    crate::control::write_resolve::resolver_for_plan(&task.plan)
                {
                    // A governed columnar predicate UPDATE/DELETE resolves to a concrete row set
                    // before proposing.
                    match authorize_one_task(&state.shared, &identity, &task) {
                        Ok(authorized_task) => {
                            crate::control::write_resolve::run_authorized_write_resolve(
                                &state.shared,
                                authorized_task,
                                resolver,
                            )
                            .await
                            .map(|response| vec![response.payload.to_vec()])
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    // Clone CoW write-path interception, then authorization, run once
                    // per task before dispatch — same protocol-neutral gate every
                    // transport runs.
                    let emitter = crate::control::security::audit::ArcAuditEmitter(
                        std::sync::Arc::clone(&state.shared.audit),
                    );
                    match crate::control::server::shared::clone_write::intercept_and_authorize(
                    crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                        state: &state.shared,
                        task,
                        identity: &identity,
                        tenant_id,
                        permissions: &state.shared.permissions,
                        roles: &state.shared.roles,
                        emitter: &emitter,
                    },
                )
                .await
                {
                    Ok(crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(
                        resp,
                    )) => Ok(vec![resp.payload.to_vec()]),
                    Ok(crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(
                        checked,
                    )) => match state.shared.installed_gateway() {
                        Ok(gateway) => {
                            let gw_ctx = QueryContext {
                                tenant_id: checked.tenant_id(),
                                trace_id,
                                database_id,
                                txn_id: None,
                                linearizable: true,
                            };
                            gateway.execute(&gw_ctx, checked).await
                        }
                        Err(error) => Err(error),
                    },
                    Err(e) => Err(e),
                }
                };
            result
        };
        let dispatch_result = if task_is_write {
            dispatch.await
        } else {
            lease_scope.guard(dispatch).await.and_then(|result| result)
        };

        match dispatch_result {
            Ok(payloads) => {
                // Row count for metering below — a per-row shaping error doesn't
                // change whether the task is billed, only how many rows count.
                let mut task_rows: u64 = 0;
                // HTTP carries no session: `nextval` advances the registry,
                // `currval` reports "not yet called in this session".
                let sequences = crate::control::sequence::SessionSequenceAccess::for_session(
                    &state.shared,
                    None,
                    database_id,
                    tenant_id,
                );
                for payload in &payloads {
                    if payload.is_empty() {
                        continue;
                    }
                    match shape_http_payload(MaterializedShapeRequest {
                        payload,
                        plan: &plan_for_shape,
                        plan_kind,
                        projection: Some(&output_schema),
                        state: &state.shared,
                        database_id,
                        tenant_id,
                        redaction: Some(redaction.ctx(&state.shared.redaction)),
                        sequences: Some(&sequences),
                    }) {
                        Ok(HttpShaped::Rows(rows)) => {
                            task_rows += rows.len() as u64;
                            for row in rows {
                                ndjson.push_str(&row.to_string());
                                ndjson.push('\n');
                            }
                        }
                        Ok(HttpShaped::Passthrough) => {
                            task_rows += 1;
                            passthrough_to_ndjson(payload, &mut ndjson);
                        }
                        Err(e) => {
                            ndjson.push_str(&serde_json::json!({"error": e.message()}).to_string());
                            ndjson.push('\n');
                        }
                    }
                }
                if let Some(info) = &plan_metering_info {
                    meter_dispatch(&state.shared, &scope, info, Some(task_rows));
                }
            }
            Err(e) => {
                let (_status, msg) = GatewayErrorMap::to_http(&e);
                ndjson.push_str(&serde_json::json!({"error": msg}).to_string());
                ndjson.push('\n');
            }
        }
    }

    ndjson_response(ndjson, rate_limit_headers)
}

/// A materialized NDJSON body with the request's rate-limit headers.
fn ndjson_response(
    ndjson: String,
    rate_limit_headers: axum::http::HeaderMap,
) -> axum::response::Response {
    let mut response = axum::response::Response::builder()
        .header("Content-Type", "application/x-ndjson")
        .body(axum::body::Body::from(ndjson))
        .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "encoding error").into_response());
    response.headers_mut().extend(rate_limit_headers);
    response
}
