// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::sequence::SessionSequenceAccess;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::session::{read_consistency, read_scope};
use crate::control::server::shared::statement_exec::{AtomicStatement, StatementAt, StatementExec};

use super::super::super::auth::{ApiError, AppState, build_request_scope, resolve_auth_parts};
use super::super::super::peer::PeerAddr;
use super::super::super::transport::ClientTransport;
use super::super::atomic_statement::{RoutedStatement, route_http_statement};
use super::super::ndjson_error::{error_line, statement_error_line};
use super::super::query_stream::{NdjsonBody, bound_ndjson_body, stream_binding, try_open_stream};
use super::super::result_shape::append_ndjson_payload;
use super::ndjson_response::{
    ndjson_response, quota_rejection, rows_to_ndjson, stream_meter_guard, sync_timeout_line,
};
use super::ndjson_task::dispatch_ndjson_task;
use super::{DatabaseQueryParam, resolve_database_id};

/// POST /v1/query/stream — execute SQL, return results as NDJSON.
///
/// Each row is a separate JSON line (`\n`-terminated), Content-Type
/// `application/x-ndjson`, so clients can process large result sets unbuffered.
/// An error is an in-band `{"error": "...", "code": "NDB-<n>"}` line. The body's
/// optional `read_consistency` defaults to `strong`.
pub async fn query_ndjson(
    state: State<AppState>,
    headers: HeaderMap,
    peer: PeerAddr,
    transport: ClientTransport,
    db_param: QueryParams<DatabaseQueryParam>,
    axum::Json(body): axum::Json<crate::control::server::http::types::HttpQueryStreamRequest>,
) -> Response {
    match read_consistency::parse_request_field(body.read_consistency.as_deref()) {
        Ok(level) => read_scope::with_request_consistency(
            level,
            ndjson_at_consistency(state, headers, peer, transport, db_param, body),
        )
        .await
        .into_response(),
        Err(error) => ApiError::from(error).into_response(),
    }
}

async fn ndjson_at_consistency(
    State(state): State<AppState>,
    headers: HeaderMap,
    peer: PeerAddr,
    transport: ClientTransport,
    QueryParams(db_param): QueryParams<DatabaseQueryParam>,
    body: crate::control::server::http::types::HttpQueryStreamRequest,
) -> impl IntoResponse {
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
    // The statement's DDL proposals carry its text. Array DDL is the DDL this route runs.
    let _audit = crate::control::server::shared::session::audit_context::AuditScope::statement(
        &identity, sql,
    );

    let tenant_id = identity.tenant_id;

    // Quota enforcement — reject before any planning or dispatch.
    if let Err(e) = state.shared.check_tenant_quota(tenant_id) {
        return quota_rejection(&e);
    }

    let query_ctx = &state.query_ctx;

    // Passing database as the session database makes `scope.database_id()` resolve to it.
    // NDJSON does not extract a per-query `ON DENY` clause (unlike `/v1/query`).
    let request = match build_request_scope(
        &identity,
        verified_jwt.as_ref(),
        &headers,
        &state,
        database_id,
        peer.as_str(),
    ) {
        Ok(request) => request,
        Err(error) => return ApiError::from(error).into_response(),
    };

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
        txn_wrapper: state.shared.sql_policy.txn_wrapper_stateless,
    })
    .await
    {
        Ok(admission) => admission,
        Err(error) => return ApiError::from_request(error, &body.sql).into_response(),
    };
    if admission.rolls_back {
        return ndjson_response(String::new(), rate_limit_headers);
    }
    let several_statements = admission.holds_several_statements();
    let output_schema = admission.output_schema;
    let output_schemas = admission.output_schemas;

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
        schemas: &output_schemas,
        database_id,
        sequences: &sequences,
    };
    let routed = route_http_statement(
        &exec,
        AtomicStatement {
            tasks: admission.tasks,
            statement_map: admission.statement_map,
            lease_scope: admission.lease_scope,
            sum_target_reads: admission.sum_target_reads,
        },
    )
    .await;
    let (tasks, statement_map, lease_scope) = match routed {
        Ok(RoutedStatement::PerTask(statement)) => (
            statement.tasks,
            statement.statement_map,
            statement.lease_scope,
        ),
        Ok(RoutedStatement::Answered(rows)) => {
            return ndjson_response(rows_to_ndjson(rows), rate_limit_headers);
        }
        // A statement that stopped answers one error line, as a task that
        // stopped in the loop below does.
        Err(error) => {
            state.shared.database_metrics.record_error(database_id);
            let line = statement_error_line(&error.error, error.at, &body.sql);
            return ndjson_response(line, rate_limit_headers);
        }
    };
    let mut lease_scope = Some(lease_scope);

    // `Body::from_stream` polls the data-plane stream under normal HTTP backpressure
    // while its captured lease scope stays alive until body completion or disconnect.
    // A request of several statements answers one materialized body.
    let opened = if several_statements {
        Ok(None)
    } else {
        try_open_stream(
            &state,
            &tasks,
            &identity,
            database_id,
            &output_schema,
            trace_id,
        )
        .await
    };
    match opened {
        Ok(Some((stream, limit))) => {
            let Some(lease_scope) = lease_scope.take() else {
                return ApiError::from(crate::Error::Internal {
                    detail: "query lease scope missing before NDJSON stream dispatch".into(),
                })
                .into_response();
            };
            let stream_meter_guard = stream_meter_guard(&state.shared, &scope, &tasks);
            let binding = match stream_binding(&state.shared, &identity, peer.as_str(), database_id)
            {
                Ok(binding) => binding,
                Err(error) => return ApiError::from(error).into_response(),
            };
            let mut response = Response::builder()
                .header("Content-Type", "application/x-ndjson")
                .body(axum::body::Body::from_stream(bound_ndjson_body(
                    NdjsonBody {
                        stream,
                        limit,
                        projection: Some(output_schema.clone()),
                        // `try_open_stream` returns `Some` only for a single-task plan.
                        redaction: tasks.first().map(|task| {
                            QueryRedaction::for_plan(
                                tenant_id,
                                task.database_id,
                                scope.auth(),
                                &task.plan,
                            )
                        }),
                        state: Arc::clone(&state.shared),
                        lease_scope,
                        database_id,
                        meter_guard: stream_meter_guard,
                    },
                    binding,
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
    let sync_database =
        crate::control::mirror::source::sync::sync_database_of(&state.shared, &tasks);
    let mut ndjson = String::new();
    let mut failed = false;
    // Checked once, not per task: keeps per-task extraction a no-op when metering is
    // disabled. This fallback fully materializes the body, so it meters like `/v1/query`.
    let metering_enabled = state.shared.metering_config.enabled;
    for (index, task) in tasks.into_iter().enumerate() {
        // Each task shapes against its own statement's schema.
        let projection = statement_map
            .statement_of(index)
            .and_then(|statement| output_schemas.get(statement as usize))
            .unwrap_or(&output_schema);
        // A lease this node lost ends the body with a retryable error line
        // before the next task dispatches.
        if let Err(revoked) = lease_scope.check_not_revoked() {
            ndjson.push_str(&error_line(&revoked));
            failed = true;
            break;
        }
        // A spent hard quota refuses the task before it runs; reported as an error
        // line and the task skipped, matching this stream's error reporting.
        let plan_metering_info = metering_enabled.then(|| PlanMeteringInfo::extract(&task.plan));
        if let Some(info) = &plan_metering_info
            && let Err(e) = admit_quota_for_dispatch(&state.shared, &scope, info)
        {
            ndjson.push_str(&error_line(&e));
            failed = true;
            continue;
        }

        // Captured before dispatch moves `task.plan` — needed by shaping below.
        let plan_kind = describe_plan(&task.plan);
        let plan_for_shape = task.plan.clone();
        // Resolved once per task, reused for every payload it produced.
        let redaction =
            QueryRedaction::for_plan(tenant_id, task.database_id, scope.auth(), &plan_for_shape);

        // A read is cancelled mid-flight once this node loses a lease it
        // holds. A write is not: its outcome will be unknown to the client.
        let task_is_write =
            crate::control::server::shared::write_admission::plan_is_write(&task.plan);
        let dispatch = dispatch_ndjson_task(
            &state.shared,
            &identity,
            task,
            tenant_id,
            database_id,
            trace_id,
        );
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
                    task_rows += append_ndjson_payload(
                        &mut ndjson,
                        MaterializedShapeRequest {
                            payload,
                            plan: &plan_for_shape,
                            plan_kind,
                            projection: Some(projection),
                            state: &state.shared,
                            database_id,
                            tenant_id,
                            redaction: Some(redaction.ctx(&state.shared.redaction)),
                            sequences: Some(&sequences),
                        },
                    )
                    .await;
                }
                if let Some(info) = &plan_metering_info {
                    meter_dispatch(&state.shared, &scope, info, Some(task_rows));
                }
            }
            Err(e) => {
                let at = StatementAt::of_task(&statement_map, index, output_schemas.len());
                ndjson.push_str(&statement_error_line(&e, at, &body.sql));
                failed = true;
            }
        }
    }
    if !failed && let Some(line) = sync_timeout_line(&state.shared, sync_database).await {
        ndjson.push_str(&line);
        failed = true;
    }
    if failed {
        state.shared.database_metrics.record_error(database_id);
    }

    ndjson_response(ndjson, rate_limit_headers)
}
