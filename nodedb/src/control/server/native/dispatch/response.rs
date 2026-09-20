// SPDX-License-Identifier: BUSL-1.1

//! Native response conversion for direct physical operations.

use nodedb_types::protocol::{NativeResponse, ResponseStatus};

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::{
    PlanKind, describe_plan, payload_to_dml_outcome,
};

use super::{
    DispatchCtx, apply_dml_outcome, error_response_to_native, error_to_native,
    shape_error_to_native, to_native_columns_rows,
};

pub(crate) fn data_plane_response_to_native(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    plan: &PhysicalPlan,
    response: &Response,
) -> NativeResponse {
    if response.status == Status::Error {
        return error_response_to_native(seq, response);
    }
    let plan_kind = describe_plan(plan);
    if response.payload.is_empty() {
        let mut native = NativeResponse::ok(seq);
        native.watermark_lsn = response.watermark_lsn.as_u64();
        // A count-bearing write with no payload is a handler bug: the count
        // reader refuses it rather than answering without a count.
        if let PlanKind::DmlResult(_) | PlanKind::DmlResultByOp = plan_kind {
            return match payload_to_dml_outcome(&[], plan_kind) {
                Ok(Some(outcome)) => {
                    apply_dml_outcome(&mut native, outcome);
                    native
                }
                Ok(None) => native,
                Err(e) => error_to_native(seq, &e),
            };
        }
        return native;
    }
    let redaction = QueryRedaction::for_plan(ctx.tenant_id(), ctx.auth_context(), plan);
    match shape_response_materialized(MaterializedShapeRequest {
        payload: &response.payload,
        plan,
        plan_kind,
        projection: None,
        state: ctx.state,
        database_id: ctx.database_id(),
        tenant_id: ctx.tenant_id(),
        redaction: Some(redaction.ctx(&ctx.state.redaction)),
        // No projection, so no Control-Plane computed column to resolve.
        sequences: None,
    }) {
        Ok(ShapeOutcome::Rows(shaped)) => {
            let (columns, rows) = to_native_columns_rows(&shaped);
            NativeResponse {
                seq,
                status: ResponseStatus::Ok,
                columns: Some(columns),
                rows: Some(rows),
                rows_affected: None,
                command: None,
                watermark_lsn: response.watermark_lsn.as_u64(),
                error: None,
                auth: None,
                warnings: shaped.notice.into_iter().collect(),
            }
        }
        // A count-bearing write reports the count and verb the payload
        // carries; an opaque execution reports neither.
        Ok(ShapeOutcome::Passthrough) => {
            let mut native = NativeResponse::ok(seq);
            native.watermark_lsn = response.watermark_lsn.as_u64();
            match payload_to_dml_outcome(&response.payload, plan_kind) {
                Ok(Some(outcome)) => apply_dml_outcome(&mut native, outcome),
                Ok(None) => {}
                Err(e) => return error_to_native(seq, &e),
            }
            native
        }
        Err(error) => shape_error_to_native(seq, &error),
    }
}
