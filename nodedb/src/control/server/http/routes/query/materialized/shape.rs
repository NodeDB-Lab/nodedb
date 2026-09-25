// SPDX-License-Identifier: BUSL-1.1

//! The per-task dispatch loop: runs each admitted task, orchestrating the
//! Control-Plane-side plan shapes (`InsertSelect`, `Merge`,
//! `UpdateFromJoin`, a governed predicate resolution) directly and routing
//! everything else through the general clone-write / gateway / local-SPSC
//! dispatch path, then shapes each response into the JSON row set.

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::Status;
use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{PlanKind, describe_plan};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::types::{DatabaseId, TenantId, TraceId};

use super::super::super::super::auth::{ApiError, AppState};
use super::super::super::result_shape::{
    HttpShaped, passthrough_json_row, shape_error_to_api, shape_http_payload,
};
use super::encode::{gateway_error, response_error};

/// Everything the per-task loop needs, beyond the tasks themselves.
pub(super) struct TaskLoopParams<'a> {
    pub(super) state: &'a AppState,
    pub(super) identity: &'a AuthenticatedIdentity,
    pub(super) scope: RequestAuthScope<'a>,
    pub(super) output_schema: OutputSchema,
    pub(super) database_id: DatabaseId,
    pub(super) tenant_id: TenantId,
    pub(super) trace_id: TraceId,
}

/// Run every admitted task and return the statement's JSON rows.
pub(super) async fn run_task_loop(
    tasks: Vec<PhysicalTask>,
    params: TaskLoopParams<'_>,
) -> Result<Vec<serde_json::Value>, ApiError> {
    let TaskLoopParams {
        state,
        identity,
        scope,
        output_schema,
        database_id,
        tenant_id,
        trace_id,
    } = params;

    let mut result_rows = Vec::new();
    // Checked once, not per task: keeps the per-task extraction below a true
    // no-op when metering is disabled (the default).
    let metering_enabled = state.shared.metering_config.enabled;

    for task in tasks {
        // Extracted before `task.plan` is cloned/moved into any branch below.
        let plan_metering_info = metering_enabled.then(|| PlanMeteringInfo::extract(&task.plan));
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
            let authorized_task = authorize_materialized_task(&state.shared, identity, &task)
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
                    state,
                    database_id,
                    tenant_id,
                    redaction: &QueryRedaction::for_plan(tenant_id, scope.auth(), &plan_for_shape),
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
                resolved_insert_identities: _,
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
            let authorized_task = authorize_materialized_task(&state.shared, identity, &task)
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
                    state,
                    database_id,
                    tenant_id,
                    redaction: &QueryRedaction::for_plan(tenant_id, scope.auth(), &plan_for_shape),
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
            let authorized_task = authorize_materialized_task(&state.shared, identity, &task)
                .map_err(gateway_error)?;
            let resp =
                crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
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
                    state,
                    database_id,
                    tenant_id,
                    redaction: &QueryRedaction::for_plan(tenant_id, scope.auth(), &plan_for_shape),
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

        // A governed columnar predicate UPDATE/DELETE resolves to a concrete row set
        // before proposing, skipping the WAL append below; local (non-Raft) path skips this.
        if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&task.plan)
            && state.shared.async_raft_proposer().is_some()
        {
            let plan_kind = describe_plan(&task.plan);
            let plan_for_shape = task.plan.clone();
            let authorized_task = authorize_materialized_task(&state.shared, identity, &task)
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
                    state,
                    database_id,
                    tenant_id,
                    redaction: &QueryRedaction::for_plan(tenant_id, scope.auth(), &plan_for_shape),
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

        // Captured before dispatch moves `task.plan` — needed by shaping below.
        let plan_kind = describe_plan(&task.plan);
        let plan_for_shape = task.plan.clone();
        // Resolved once per task, reused for every payload it produced.
        let redaction = QueryRedaction::for_plan(tenant_id, scope.auth(), &plan_for_shape);

        // Clone CoW write-path interception, then authorization, run once
        // per task before dispatch — same protocol-neutral gate every
        // transport runs.
        let emitter = ArcAuditEmitter(Arc::clone(&state.shared.audit));
        let checked = match crate::control::server::shared::clone_write::intercept_and_authorize(
            crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                state: &state.shared,
                task,
                identity,
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
                        state,
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
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(checked) => {
                checked
            }
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
                gw.execute(&gw_ctx, checked).await.map_err(gateway_error)?
            }
            None => {
                // Single-node boot: gateway not yet initialised — dispatch locally.
                let response =
                    crate::control::server::dispatch_utils::dispatch_authorized_durable_write(
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

        // HTTP carries no session, so `currval` reports "not yet called
        // in this session" while `nextval` still advances the registry —
        // the same answer the plan-time adapter gives this transport.
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
                Ok(HttpShaped::Rows(rows)) => result_rows.extend(rows),
                Ok(HttpShaped::Passthrough) => result_rows.push(passthrough_json_row(payload)),
                Err(e) => return Err(shape_error_to_api(e)),
            }
        }
        meter_task_dispatch(
            &state.shared,
            &scope,
            &plan_metering_info,
            rows_before,
            &result_rows,
        );
    }

    Ok(result_rows)
}

/// Authorize one task with no clone-write check — used only by the
/// Control-Plane orchestrator branches ahead of the general dispatch tail,
/// whose plan shapes (`InsertSelect`, `Merge`, `UpdateFromJoin`, a governed
/// predicate resolution) are never clone-write shapes.
fn authorize_materialized_task(
    shared: &crate::control::state::SharedState,
    identity: &AuthenticatedIdentity,
    task: &PhysicalTask,
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

/// Meter one task's dispatch after its rows are appended to `result_rows` —
/// the row count is the delta since `rows_before`.
fn meter_task_dispatch(
    state: &crate::control::state::SharedState,
    scope: &RequestAuthScope<'_>,
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
    plan_kind: PlanKind,
    output_schema: &'a OutputSchema,
    state: &'a AppState,
    database_id: DatabaseId,
    tenant_id: TenantId,
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
    // HTTP carries no session: `nextval` advances the registry, `currval`
    // reports "not yet called in this session".
    let sequences = crate::control::sequence::SessionSequenceAccess::for_session(
        &append.state.shared,
        None,
        append.database_id,
        append.tenant_id,
    );
    match shape_http_payload(MaterializedShapeRequest {
        payload: &payload,
        plan: append.plan,
        plan_kind: append.plan_kind,
        projection: Some(append.output_schema),
        state: &append.state.shared,
        database_id: append.database_id,
        tenant_id: append.tenant_id,
        redaction: Some(append.redaction.ctx(&append.state.shared.redaction)),
        sequences: Some(&sequences),
    }) {
        Ok(HttpShaped::Rows(rows)) => result_rows.extend(rows),
        Ok(HttpShaped::Passthrough) => result_rows.push(passthrough_json_row(&payload)),
        Err(e) => return Err(shape_error_to_api(e)),
    }
    Ok(())
}
