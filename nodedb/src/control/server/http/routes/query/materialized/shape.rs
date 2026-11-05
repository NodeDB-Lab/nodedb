// SPDX-License-Identifier: BUSL-1.1

//! The per-task dispatch loop: runs each admitted task, then shapes each
//! response into the JSON row set.
//!
//! A Control-Plane orchestrated plan runs in `orchestrated`. Every other
//! task takes the clone-write gate, then the gateway route to its owner.

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::metering::PlanMeteringInfo;
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::types::{DatabaseId, TenantId, TraceId};

use super::super::super::super::auth::{ApiError, AppState};
use super::append::{ShapedAppend, append_payload, append_response, meter_task_dispatch};
use super::encode::gateway_error;
use super::orchestrated::run_orchestrated;

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

        // Captured before dispatch moves `task.plan`. Resolved once per task,
        // reused for every payload it produced.
        let plan_for_shape = task.plan.clone();
        let redaction =
            QueryRedaction::for_plan(tenant_id, task.database_id, scope.auth(), &plan_for_shape);
        let append = ShapedAppend {
            plan: &plan_for_shape,
            plan_kind: describe_plan(&plan_for_shape),
            output_schema: &output_schema,
            state,
            database_id,
            tenant_id,
            redaction: &redaction,
        };

        if let Some(orchestrated) = run_orchestrated(&state.shared, identity, &task).await? {
            append_payload(&mut result_rows, &orchestrated.payload, &append).await?;
            if orchestrated.metered {
                meter_task_dispatch(
                    &state.shared,
                    &scope,
                    &plan_metering_info,
                    rows_before,
                    &result_rows,
                );
            }
            continue;
        }

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
                append_response(&mut result_rows, resp, &append).await?;
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

        // The gateway routes the task to its owner and owns WAL durability there.
        let gateway = state.shared.installed_gateway().map_err(gateway_error)?;
        let gw_ctx = QueryContext {
            tenant_id: checked.tenant_id(),
            trace_id,
            database_id,
            txn_id: None,
            linearizable: crate::control::server::shared::session::read_scope::linearizable(),
        };
        let payloads = gateway
            .execute(&gw_ctx, checked)
            .await
            .map_err(gateway_error)?;
        for payload in &payloads {
            append_payload(&mut result_rows, payload, &append).await?;
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
