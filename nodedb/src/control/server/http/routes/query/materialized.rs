// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::bridge::envelope::Status;
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;

use super::super::super::auth::{ApiError, AppState, build_request_scope, resolve_auth_parts};
use super::super::super::peer::PeerAddr;
use super::super::super::transport::ClientTransport;
use super::super::super::types::{HttpQueryRequest, HttpQueryResponse};
use super::super::result_shape::{
    HttpShaped, ddl_results_to_json, passthrough_json_row, shape_http_payload,
};
use super::{DatabaseQueryParam, resolve_database_id};

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
        super::super::super::rate_limit_headers::rate_limit_headers(&rate_limit_result);

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

    // Execute each task via the SPSC bridge.
    let mut result_rows = Vec::new();
    // Checked once, not per task: keeps the per-task extraction below a true
    // no-op when metering is disabled (the default).
    let metering_enabled = state.shared.metering_config.enabled;

    async {
        for task in tasks {
            // Extracted before `task.plan` is cloned/moved into any branch below.
            let plan_metering_info =
                metering_enabled.then(|| PlanMeteringInfo::extract(&task.plan));
            // A spent hard quota refuses the task before it runs; charging below is
            // success-path only and never refuses.
            if let Some(info) = &plan_metering_info {
                admit_quota_for_dispatch(&state.shared, &scope, info).map_err(gateway_error)?;
            }
            let rows_before = result_rows.len();
            // `INSERT ... SELECT` orchestrates on the Control Plane and issues its own
            // WAL-backed writes, so the outer per-task WAL append is skipped for it.
            // Never a clone-write shape.
            if let crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. },
            ) = &task.plan
            {
                let plan_kind = describe_plan(&task.plan);
                let plan_for_shape = task.plan.clone();
                let authorized_task =
                    authorize_materialized_task(&state.shared, &identity, &task)
                        .map_err(gateway_error)?;
                let resp = crate::control::insert_select::run_authorized_insert_select(
                    &state.shared,
                    authorized_task,
                )
                .await
                .map_err(gateway_error)?;
                append_response(
                    &mut result_rows,
                    resp,
                    ShapedAppend {
                        plan: &plan_for_shape,
                        plan_kind,
                        output_schema: &output_schema,
                        state: &state,
                        database_id,
                        tenant_id,
                        redaction: &QueryRedaction::for_plan(
                            tenant_id,
                            scope.auth(),
                            &plan_for_shape,
                        ),
                    },
                )?;
                meter_task_dispatch(&state.shared, &scope, &plan_metering_info, rows_before, &result_rows);
                continue;
            }

            // Autocommit `MERGE` orchestrates on the Control Plane and issues its own
            // writes, so the per-task WAL append below is skipped for it.
            if let crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::Merge {
                    target_collection: _,
                    source_collection: _,
                    source_alias: _,
                    target_join_col: _,
                    source_join_col: _,
                    clauses: _,
                    returning: _,
                    resolved_inserts: None,
                    source_rows: _,
                    rls_filters: _,
                    rls_write_check: _,
                    resolved_sum_targets: _,
                    declared_primary_key: _,
                },
            ) = &task.plan
            {
                let plan_kind = describe_plan(&task.plan);
                let plan_for_shape = task.plan.clone();
                let authorized_task =
                    authorize_materialized_task(&state.shared, &identity, &task)
                        .map_err(gateway_error)?;
                let resp = crate::control::merge_orchestrator::run_authorized_merge(
                    &state.shared,
                    authorized_task,
                )
                .await
                .map_err(gateway_error)?;
                append_response(
                    &mut result_rows,
                    resp,
                    ShapedAppend {
                        plan: &plan_for_shape,
                        plan_kind,
                        output_schema: &output_schema,
                        state: &state,
                        database_id,
                        tenant_id,
                        redaction: &QueryRedaction::for_plan(
                            tenant_id,
                            scope.auth(),
                            &plan_for_shape,
                        ),
                    },
                )?;
                meter_task_dispatch(&state.shared, &scope, &plan_metering_info, rows_before, &result_rows);
                continue;
            }

            // Autocommit `UPDATE ... FROM <source>` scans the source on its own core and
            // ships it into the plan; the orchestrator's own write skips the WAL append below.
            if let crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                    target_collection: _,
                    source_collection: _,
                    source_alias: _,
                    target_join_col: _,
                    source_join_col: _,
                    updates: _,
                    target_filters: _,
                    returning: _,
                    source_rows: None,
                    rls_filters: _,
                    rls_write_check: _,
                    resolved_sum_targets: _,
                    declared_primary_key: _,
                },
            ) = &task.plan
            {
                let plan_kind = describe_plan(&task.plan);
                let plan_for_shape = task.plan.clone();
                let authorized_task =
                    authorize_materialized_task(&state.shared, &identity, &task)
                        .map_err(gateway_error)?;
                let resp = crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                    &state.shared,
                    authorized_task,
                )
                .await
                .map_err(gateway_error)?;
                append_response(
                    &mut result_rows,
                    resp,
                    ShapedAppend {
                        plan: &plan_for_shape,
                        plan_kind,
                        output_schema: &output_schema,
                        state: &state,
                        database_id,
                        tenant_id,
                        redaction: &QueryRedaction::for_plan(
                            tenant_id,
                            scope.auth(),
                            &plan_for_shape,
                        ),
                    },
                )?;
                meter_task_dispatch(&state.shared, &scope, &plan_metering_info, rows_before, &result_rows);
                continue;
            }

            // A governed columnar predicate UPDATE/DELETE resolves to a concrete row set
            // before proposing, skipping the WAL append below; local (non-Raft) path skips this.
            if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&task.plan)
                && state.shared.async_raft_proposer().is_some()
            {
                let plan_kind = describe_plan(&task.plan);
                let plan_for_shape = task.plan.clone();
                let authorized_task =
                    authorize_materialized_task(&state.shared, &identity, &task)
                        .map_err(gateway_error)?;
                let resp = crate::control::write_resolve::run_authorized_write_resolve(
                    &state.shared,
                    authorized_task,
                    resolver,
                )
                .await
                .map_err(gateway_error)?;
                append_response(
                    &mut result_rows,
                    resp,
                    ShapedAppend {
                        plan: &plan_for_shape,
                        plan_kind,
                        output_schema: &output_schema,
                        state: &state,
                        database_id,
                        tenant_id,
                        redaction: &QueryRedaction::for_plan(
                            tenant_id,
                            scope.auth(),
                            &plan_for_shape,
                        ),
                    },
                )?;
                meter_task_dispatch(&state.shared, &scope, &plan_metering_info, rows_before, &result_rows);
                continue;
            }

            // Captured before dispatch moves `task.plan` — needed by shaping below.
            let plan_kind = describe_plan(&task.plan);
            let plan_for_shape = task.plan.clone();
            // Resolved once per task, reused for every payload it produced.
            let redaction = QueryRedaction::for_plan(tenant_id, scope.auth(), &plan_for_shape);

            // Clone CoW write-path interception, then authorization, run once
            // per task before dispatch — same protocol-neutral gate every
            // transport runs.
            let emitter = crate::control::security::audit::ArcAuditEmitter(Arc::clone(
                &state.shared.audit,
            ));
            let checked = match crate::control::server::shared::clone_write::intercept_and_authorize(
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
            .map_err(gateway_error)?
            {
                crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(resp) => {
                    append_response(
                        &mut result_rows,
                        resp,
                        ShapedAppend {
                            plan: &plan_for_shape,
                            plan_kind,
                            output_schema: &output_schema,
                            state: &state,
                            database_id,
                            tenant_id,
                            redaction: &redaction,
                        },
                    )?;
                    meter_task_dispatch(
                        &state.shared,
                        &scope,
                        &plan_metering_info,
                        rows_before,
                        &result_rows,
                    );
                    continue;
                }
                crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(
                    checked,
                ) => checked,
            };

            // Prefer gateway (cluster-aware, owns WAL durability), else fall back to
            // local SPSC dispatch, where WAL append precedes enqueue so LSN order matches.
            let payloads = match state.shared.gateway.get() {
                Some(gw) => {
                    let gw_ctx = QueryContext {
                        tenant_id: checked.tenant_id(),
                        trace_id,
                        database_id,
                        txn_id: None,
                    };
                    gw.execute(&gw_ctx, checked)
                        .await
                        .map_err(gateway_error)?
                }
                None => {
                    // Single-node boot: gateway not yet initialised — dispatch locally.
                    let response = crate::control::server::dispatch_utils::dispatch_authorized_autocommit_write(
                        &state.shared,
                        checked,
                        trace_id,
                    )
                        .await
                        .map_err(gateway_error)?;
                    if response.status != Status::Ok {
                        return Err(response_error(&response));
                    }
                    vec![response.payload.to_vec()]
                }
            };

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
                }) {
                    Ok(HttpShaped::Rows(rows)) => result_rows.extend(rows),
                    Ok(HttpShaped::Passthrough) => result_rows.push(passthrough_json_row(payload)),
                    Err(e) => return Err(ApiError::Internal(e.message().to_string())),
                }
            }
            meter_task_dispatch(&state.shared, &scope, &plan_metering_info, rows_before, &result_rows);
        }

        Ok((rate_limit_headers, axum::Json(HttpQueryResponse::ok(result_rows))))
    }
    .await
}

/// Authorize one task with no clone-write check — used only by the
/// Control-Plane orchestrator branches ahead of the general dispatch tail,
/// whose plan shapes (`InsertSelect`, `Merge`, `UpdateFromJoin`, a governed
/// predicate resolution) are never clone-write shapes.
fn authorize_materialized_task(
    shared: &crate::control::state::SharedState,
    identity: &crate::control::security::identity::AuthenticatedIdentity,
    task: &nodedb_physical::physical_task::PhysicalTask,
) -> crate::Result<crate::control::server::shared::authorization::AuthorizedTask> {
    let emitter = ArcAuditEmitter(Arc::clone(&shared.audit));
    crate::control::server::shared::authorization::authorize_task_set(
        identity,
        std::slice::from_ref(task),
        &shared.permissions,
        &shared.roles,
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

fn ddl_error_to_api(error: crate::control::server::shared::ddl::DdlError) -> ApiError {
    let status = if error.sqlstate == "42501" {
        axum::http::StatusCode::FORBIDDEN
    } else {
        axum::http::StatusCode::BAD_REQUEST
    };
    ApiError::Coded {
        status,
        message: error.message,
        code: error.code,
    }
}

fn gateway_error(error: crate::Error) -> ApiError {
    let (status, msg) = GatewayErrorMap::to_http(&error);
    ApiError::HttpStatus(status, msg)
}

fn response_error(response: &crate::bridge::envelope::Response) -> ApiError {
    let detail = response
        .error_code
        .as_ref()
        .map(|code| format!("{code:?}"))
        .unwrap_or_else(|| "unknown error".into());
    ApiError::Internal(detail)
}

/// Meter one task's dispatch after its rows are appended to `result_rows` —
/// the row count is the delta since `rows_before`.
fn meter_task_dispatch(
    state: &crate::control::state::SharedState,
    scope: &crate::control::security::request_scope::RequestAuthScope<'_>,
    info: &Option<PlanMeteringInfo>,
    rows_before: usize,
    result_rows: &[serde_json::Value],
) {
    if let Some(info) = info {
        let task_rows = (result_rows.len() - rows_before) as u64;
        meter_dispatch(state, scope, info, Some(task_rows));
    }
}

/// Everything one orchestrated task's response needs to be shaped and
/// appended. Grouped so the append helper stays within the argument budget as
/// it gained the per-statement redaction resolution.
struct ShapedAppend<'a> {
    plan: &'a crate::bridge::envelope::PhysicalPlan,
    plan_kind: crate::control::server::response_shape::types::PlanKind,
    output_schema: &'a crate::control::server::response_shape::schema::OutputSchema,
    state: &'a AppState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: crate::types::TenantId,
    redaction: &'a QueryRedaction,
}

fn append_response(
    result_rows: &mut Vec<serde_json::Value>,
    response: crate::bridge::envelope::Response,
    append: ShapedAppend<'_>,
) -> Result<(), ApiError> {
    if response.status != Status::Ok {
        return Err(response_error(&response));
    }
    let payload = response.payload.to_vec();
    if payload.is_empty() {
        return Ok(());
    }
    match shape_http_payload(MaterializedShapeRequest {
        payload: &payload,
        plan: append.plan,
        plan_kind: append.plan_kind,
        projection: Some(append.output_schema),
        state: &append.state.shared,
        database_id: append.database_id,
        tenant_id: append.tenant_id,
        redaction: Some(append.redaction.ctx(&append.state.shared.redaction)),
    }) {
        Ok(HttpShaped::Rows(rows)) => result_rows.extend(rows),
        Ok(HttpShaped::Passthrough) => result_rows.push(passthrough_json_row(&payload)),
        Err(e) => return Err(ApiError::Internal(e.message().to_string())),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_insufficient_privilege_maps_to_forbidden() {
        let error =
            crate::control::server::shared::ddl::DdlError::new("42501", "write permission denied");

        assert!(matches!(
            ddl_error_to_api(error),
            ApiError::Coded { status, message, code }
                if status == axum::http::StatusCode::FORBIDDEN
                    && message == "write permission denied"
                    && code == nodedb_types::error::ErrorCode::AUTHORIZATION_DENIED
        ));
    }

    /// Round-trips through the actual JSON response body `into_response()`
    /// produces — not just the pre-serialization `ApiError` — so this proves
    /// the code reaches the client, not merely that the server set it.
    #[tokio::test]
    async fn ddl_error_code_survives_into_response_json() {
        let error =
            crate::control::server::shared::ddl::DdlError::new("42501", "write permission denied");

        let response = ddl_error_to_api(error).into_response();
        assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid JSON body");
        assert_eq!(
            json["code"],
            nodedb_types::error::ErrorCode::AUTHORIZATION_DENIED.to_string()
        );
        assert_eq!(json["error"], "write permission denied");
    }
}
