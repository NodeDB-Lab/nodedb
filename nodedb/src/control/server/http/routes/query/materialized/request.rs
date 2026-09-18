// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};

use super::super::super::super::auth::{
    ApiError, AppState, build_request_scope, resolve_auth_parts,
};
use super::super::super::super::peer::PeerAddr;
use super::super::super::super::transport::ClientTransport;
use super::super::super::super::types::{HttpQueryRequest, HttpQueryResponse};
use super::super::super::result_shape::ddl_results_to_json;
use super::super::{DatabaseQueryParam, resolve_database_id};
use super::encode::ddl_error_to_api;
use super::shape::{TaskLoopParams, run_task_loop};

/// POST /v1/query — execute a SQL/DDL statement.
///
/// Body: `{ "sql": "..." }`. Database context via `X-NodeDB-Database` header
/// or `?database=` param.
pub async fn query(
    headers: HeaderMap,
    peer: PeerAddr,
    transport: ClientTransport,
    QueryParams(db_param): QueryParams<DatabaseQueryParam>,
    State(state): State<AppState>,
    axum::Json(body): axum::Json<HttpQueryRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let (identity, verified_jwt) =
        resolve_auth_parts(&headers, &state, peer.as_str(), transport.security()).await?;
    let database_id = resolve_database_id(&headers, &db_param, &state)?;
    let trace_id = crate::control::trace_context::extract_from_headers(&headers);
    let emitter = ArcAuditEmitter(Arc::clone(&state.shared.audit));
    authorize_database(&identity, database_id, &emitter).map_err(crate::Error::from)?;

    let sql = body.sql.as_str();

    // Request-selected database is authoritative for RLS vars; passing it as the
    // session database makes `scope.database_id()` resolve to `database_id`.
    let request = build_request_scope(
        &identity,
        verified_jwt.as_ref(),
        &headers,
        &state,
        database_id,
        peer.as_str(),
    );

    // Admission gate runs once, before either DDL dispatch or DML planning, so both
    // are covered. `Some(result)` carries the outcome as `X-RateLimit-*` headers below.
    let rate_limit_result = crate::control::server::session_auth::check_request_admission(
        &state.shared,
        &request,
        "sql",
    )?;
    let scope = request.into_resolved_scope();
    let rate_limit_headers =
        super::super::super::super::rate_limit_headers::rate_limit_headers(&rate_limit_result);

    // HTTP is stateless — no BEGIN/COMMIT session concept — so a session-less scope
    // satisfies the DDL dispatch signature and always takes the autocommit branch.
    let http_scope = crate::control::server::shared::session::DetachedTxnScope::new();
    let txn_ctx = http_scope.ctx();

    // Try DDL commands first. Reached only after the admission call above, so
    // `shared::ddl::user_dispatch` must not admit this request a second time.
    if let Some(result) = crate::control::server::shared::ddl::dispatch(
        &state.shared,
        &identity,
        sql.trim(),
        database_id,
        &txn_ctx,
    )
    .await
    {
        return match result {
            Ok(results) => {
                let json_rows = ddl_results_to_json(results);
                Ok((
                    rate_limit_headers,
                    axum::Json(HttpQueryResponse::ok(json_rows)),
                ))
            }
            Err(e) => Err(ddl_error_to_api(e)),
        };
    }

    // Extract per-query ON DENY override + plan SQL with RLS injection.
    let tenant_id = identity.tenant_id;

    // Quota enforcement — reject before any planning or dispatch.
    state
        .shared
        .check_tenant_quota(tenant_id)
        .map_err(|e| ApiError::RateLimited {
            message: e.to_string(),
            retry_after_secs: 1,
        })?;

    let (clean_sql, scope) =
        crate::control::server::session_auth::apply_per_query_on_deny(sql, scope);
    // Planning and lease admission run as one retried unit so a descriptor drain
    // starting between them is absorbed rather than surfaced.
    let admission = plan_authorize_and_admit(PlanAdmissionRequest {
        state: &state.shared,
        query_ctx: &state.query_ctx,
        scope: &scope,
        sql: &clean_sql,
        trace_id: crate::types::TraceId::ZERO,
    })
    .await
    .map_err(ApiError::from)?;
    let tasks = admission.tasks;
    let output_schema = admission.output_schema;
    let _lease_scope = admission.lease_scope;

    if tasks.is_empty() {
        return Ok((
            rate_limit_headers,
            axum::Json(HttpQueryResponse::ok(vec![])),
        ));
    }

    // Track active request for quota accounting.
    let _request = state.shared.tenant_request_guard(tenant_id);

    let result_rows = run_task_loop(
        tasks,
        TaskLoopParams {
            state: &state,
            identity: &identity,
            scope,
            output_schema,
            database_id,
            tenant_id,
            trace_id,
        },
    )
    .await?;

    Ok((
        rate_limit_headers,
        axum::Json(HttpQueryResponse::ok(result_rows)),
    ))
}
