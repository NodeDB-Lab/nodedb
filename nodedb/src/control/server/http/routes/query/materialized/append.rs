// SPDX-License-Identifier: BUSL-1.1

//! Per-task helpers the dispatch loop and the orchestrated plans share:
//! metering, and shaping one task's answer into the statement's JSON rows.

use crate::bridge::envelope::Status;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::PlanKind;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::types::{DatabaseId, TenantId};

use super::super::super::super::auth::{ApiError, AppState};
use super::super::super::result_shape::{
    HttpShaped, passthrough_json_row, shape_error_to_api, shape_http_payload,
};
use super::encode::response_error;

/// Meter one task's dispatch after its rows are appended to `result_rows` —
/// the row count is the delta since `rows_before`.
pub(super) fn meter_task_dispatch(
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

/// Everything one task's answer needs to be shaped and appended. Resolved
/// once per task and reused for every payload the task produced.
pub(super) struct ShapedAppend<'a> {
    pub(super) plan: &'a crate::bridge::envelope::PhysicalPlan,
    pub(super) plan_kind: PlanKind,
    pub(super) output_schema: &'a OutputSchema,
    pub(super) state: &'a AppState,
    pub(super) database_id: DatabaseId,
    pub(super) tenant_id: TenantId,
    pub(super) redaction: &'a QueryRedaction,
}

/// Shape a response. A refusal becomes the HTTP error its code maps to.
pub(super) fn append_response(
    result_rows: &mut Vec<serde_json::Value>,
    response: crate::bridge::envelope::Response,
    append: &ShapedAppend<'_>,
) -> Result<(), ApiError> {
    if response.status != Status::Ok {
        return Err(response_error(&response));
    }
    append_payload(result_rows, &response.payload.to_vec(), append)
}

/// Shape one payload into rows, or pass a write's or tag's payload through
/// as one JSON row. An empty payload adds nothing.
pub(super) fn append_payload(
    result_rows: &mut Vec<serde_json::Value>,
    payload: &[u8],
    append: &ShapedAppend<'_>,
) -> Result<(), ApiError> {
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
        payload,
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
        Ok(HttpShaped::Passthrough) => result_rows.push(passthrough_json_row(payload)),
        Err(e) => return Err(shape_error_to_api(e)),
    }
    Ok(())
}
