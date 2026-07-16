// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::bridge::envelope::Status;
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::authorization::{authorize_database, authorize_task_set};

use super::super::super::auth::{ApiError, AppState, resolve_identity};
use super::super::super::types::{HttpQueryRequest, HttpQueryResponse};
use super::super::result_shape::{
    HttpShaped, ddl_results_to_json, passthrough_json_row, shape_http_payload,
};
use super::{DatabaseQueryParam, resolve_database_id};

/// POST /v1/query — execute a SQL/DDL statement.
///
/// Request body: `{ "sql": "..." }`
/// Response: `{ "status": "ok", "rows": [...] }` or `{ "error": "..." }`
///
/// Database context (optional):
/// - `X-NodeDB-Database: <name>` header (highest priority)
/// - `?database=<name>` query parameter (fallback)
pub async fn query(
    headers: HeaderMap,
    QueryParams(db_param): QueryParams<DatabaseQueryParam>,
    State(state): State<AppState>,
    axum::Json(body): axum::Json<HttpQueryRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, "http")?;
    let database_id = resolve_database_id(&headers, &db_param, &state)?;
    let trace_id = crate::control::trace_context::extract_from_headers(&headers);
    let emitter = ArcAuditEmitter(Arc::clone(&state.shared.audit));
    authorize_database(&identity, database_id, &emitter).map_err(crate::Error::from)?;

    let sql = body.sql.as_str();

    // HTTP is stateless — there is no BEGIN/COMMIT session concept over this
    // transport, so a session-less scope satisfies the DDL dispatch signature.
    // A fresh store reports "not in a transaction block" for any address, so
    // the staging gate inside `plan_and_dispatch` always takes the immediate
    // autocommit branch here, unchanged from before the gate existed.
    let http_scope = crate::control::server::shared::session::DetachedTxnScope::new();
    let txn_ctx = http_scope.ctx();

    // Try DDL commands first (same as pgwire handler).
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
                Ok(axum::Json(HttpQueryResponse::ok(json_rows)))
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

    let mut auth_ctx = crate::control::server::session_auth::build_auth_context(&identity);
    let clean_sql =
        crate::control::server::session_auth::extract_and_apply_on_deny(sql, &mut auth_ctx);
    let perm_cache = state.shared.permission_cache.read().await;
    let sec = crate::control::planner::context::PlanSecurityContext {
        identity: &identity,
        auth: &auth_ctx,
        rls_store: &state.shared.rls,
        permissions: &state.shared.permissions,
        roles: &state.shared.roles,
        permission_cache: Some(&*perm_cache),
    };
    let (mut tasks, output_schema) = state
        .query_ctx
        .plan_sql_with_rls(crate::control::planner::context::PlanSqlWithRlsParams {
            sql: &clean_sql,
            tenant_id,
            database_id,
            sec: &sec,
        })
        .await
        .map_err(|e| ApiError::BadRequest(format!("SQL planning failed: {e}")))?;

    crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        &state.shared,
        &mut tasks,
        tenant_id,
        database_id,
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(ApiError::from)?;

    authorize_task_set(
        &identity,
        &tasks,
        &state.shared.permissions,
        &state.shared.roles,
        &emitter,
    )
    .map_err(crate::Error::from)?;

    if tasks.is_empty() {
        return Ok(axum::Json(HttpQueryResponse::ok(vec![])));
    }

    // Track active request for quota accounting.
    state.shared.tenant_request_start(tenant_id);

    // Execute each task via the SPSC bridge.
    let mut result_rows = Vec::new();

    let result = async {
        for task in tasks {
            // `INSERT ... SELECT` is orchestrated on the Control Plane: the
            // source is scanned, each target row gets its OWN fresh, registered
            // surrogate, and the rows are written via an atomic `BatchInsert`.
            // The orchestrator issues its own WAL-backed writes, so the outer
            // per-task WAL append below is skipped for it.
            if let crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::InsertSelect {
                    target_collection,
                    source_collection,
                    source_filters,
                    source_limit,
                },
            ) = &task.plan
            {
                let plan_kind = describe_plan(&task.plan);
                let plan_for_shape = task.plan.clone();
                let resp = crate::control::insert_select::run_insert_select(
                    &state.shared,
                    task.tenant_id,
                    task.database_id,
                    target_collection,
                    source_collection,
                    source_filters,
                    *source_limit,
                )
                .await
                .map_err(gateway_error)?;
                append_response(
                    &mut result_rows,
                    resp,
                    &plan_for_shape,
                    plan_kind,
                    &output_schema,
                    &state,
                    database_id,
                    tenant_id,
                )?;
                continue;
            }

            // Autocommit `MERGE` is orchestrated on the Control Plane: each
            // NOT-MATCHED insert row gets its OWN fresh, registered surrogate
            // and all arms apply atomically. The orchestrator issues its own
            // writes, so the per-task WAL append below is skipped for it.
            if let crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::Merge {
                    target_collection,
                    source_collection,
                    source_alias,
                    target_join_col,
                    source_join_col,
                    clauses,
                    returning: _,
                    resolve_only: false,
                    resolved_inserts: None,
                    source_rows: _,
                },
            ) = &task.plan
            {
                let plan_kind = describe_plan(&task.plan);
                let plan_for_shape = task.plan.clone();
                let resp = crate::control::merge_orchestrator::run_merge(
                    &state.shared,
                    crate::control::merge_orchestrator::MergeArgs {
                        tenant_id: task.tenant_id,
                        database_id: task.database_id,
                        target_collection,
                        source_collection,
                        source_alias,
                        target_join_col,
                        source_join_col,
                        clauses,
                    },
                )
                .await
                .map_err(gateway_error)?;
                append_response(
                    &mut result_rows,
                    resp,
                    &plan_for_shape,
                    plan_kind,
                    &output_schema,
                    &state,
                    database_id,
                    tenant_id,
                )?;
                continue;
            }

            // Autocommit `UPDATE ... FROM <source>` is orchestrated on the
            // Control Plane: the source is scanned on its OWN core and shipped
            // into the plan so the target-core handler joins against it instead
            // of a local read. The orchestrator issues its own write, so the
            // per-task WAL append below is skipped for it.
            if let crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                    target_collection,
                    source_collection,
                    source_alias,
                    target_join_col,
                    source_join_col,
                    updates,
                    target_filters,
                    returning,
                    resolve_only: false,
                    source_rows: None,
                },
            ) = &task.plan
            {
                let plan_kind = describe_plan(&task.plan);
                let plan_for_shape = task.plan.clone();
                let resp = crate::control::update_from_join_orchestrator::run_update_from_join(
                    &state.shared,
                    crate::control::update_from_join_orchestrator::UpdateFromJoinArgs {
                        tenant_id: task.tenant_id,
                        database_id: task.database_id,
                        target_collection,
                        source_collection,
                        source_alias,
                        target_join_col,
                        source_join_col,
                        updates,
                        target_filters,
                        returning: returning.as_ref(),
                    },
                )
                .await
                .map_err(gateway_error)?;
                append_response(
                    &mut result_rows,
                    resp,
                    &plan_for_shape,
                    plan_kind,
                    &output_schema,
                    &state,
                    database_id,
                    tenant_id,
                )?;
                continue;
            }

            // Captured before dispatch moves `task.plan` — needed by the
            // protocol-neutral shaping core below.
            let plan_kind = describe_plan(&task.plan);
            let plan_for_shape = task.plan.clone();

            // Dispatch: prefer gateway when available (cluster-aware routing —
            // the gateway owns WAL durability on the target node), fall back to
            // direct local SPSC dispatch on single-node boot. On the local path
            // the WAL append is performed inside the dispatch core, under the
            // write-admission guard and just before the enqueue, so LSN order
            // matches apply order.
            let payloads = match state.shared.gateway.as_ref() {
                Some(gw) => {
                    let gw_ctx = QueryContext {
                        tenant_id: task.tenant_id,
                        trace_id,
                        database_id,
                        txn_id: None,
                    };
                    gw.execute(&gw_ctx, task.plan)
                        .await
                        .map_err(gateway_error)?
                }
                None => {
                    // Single-node boot: gateway not yet initialised — dispatch locally.
                    let response =
                        crate::control::server::dispatch_utils::dispatch_autocommit_write(
                            &state.shared,
                            crate::control::server::dispatch_utils::AutocommitWrite {
                                tenant_id: task.tenant_id,
                                database_id: task.database_id,
                                vshard_id: task.vshard_id,
                                plan: task.plan,
                                trace_id,
                                event_source: crate::event::EventSource::User,
                                txn_id: None,
                            },
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
                match shape_http_payload(
                    payload,
                    &plan_for_shape,
                    plan_kind,
                    Some(&output_schema),
                    &state.shared,
                    database_id,
                    tenant_id,
                ) {
                    Ok(HttpShaped::Rows(rows)) => result_rows.extend(rows),
                    Ok(HttpShaped::Passthrough) => result_rows.push(passthrough_json_row(payload)),
                    Err(e) => return Err(ApiError::Internal(e.message().to_string())),
                }
            }
        }

        Ok(axum::Json(HttpQueryResponse::ok(result_rows)))
    }
    .await;

    state.shared.tenant_request_end(tenant_id);
    result
}

fn ddl_error_to_api(error: crate::control::server::shared::ddl::DdlError) -> ApiError {
    if error.sqlstate == "42501" {
        ApiError::Forbidden(error.message)
    } else {
        ApiError::BadRequest(error.message)
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

#[allow(clippy::too_many_arguments)]
fn append_response(
    result_rows: &mut Vec<serde_json::Value>,
    response: crate::bridge::envelope::Response,
    plan: &crate::bridge::envelope::PhysicalPlan,
    plan_kind: crate::control::server::response_shape::types::PlanKind,
    output_schema: &crate::control::server::response_shape::schema::OutputSchema,
    state: &AppState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: crate::types::TenantId,
) -> Result<(), ApiError> {
    if response.status != Status::Ok {
        return Err(response_error(&response));
    }
    let payload = response.payload.to_vec();
    if payload.is_empty() {
        return Ok(());
    }
    match shape_http_payload(
        &payload,
        plan,
        plan_kind,
        Some(output_schema),
        &state.shared,
        database_id,
        tenant_id,
    ) {
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
        let error = crate::control::server::shared::ddl::DdlError {
            sqlstate: "42501".into(),
            message: "write permission denied".into(),
        };

        assert!(matches!(
            ddl_error_to_api(error),
            ApiError::Forbidden(message) if message == "write permission denied"
        ));
    }
}
