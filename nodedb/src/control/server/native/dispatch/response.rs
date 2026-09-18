// SPDX-License-Identifier: BUSL-1.1

//! Native response conversion for direct physical operations.

use nodedb_types::protocol::{NativeResponse, ResponseStatus};

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::describe_plan;

use super::{DispatchCtx, error_response_to_native, shape_error_to_native, to_native_columns_rows};

pub(crate) fn data_plane_response_to_native(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    plan: &PhysicalPlan,
    response: &Response,
) -> NativeResponse {
    if response.status == Status::Error {
        return error_response_to_native(seq, response);
    }
    if response.payload.is_empty() {
        let mut native = NativeResponse::ok(seq);
        native.watermark_lsn = response.watermark_lsn.as_u64();
        return native;
    }
    let redaction = QueryRedaction::for_plan(ctx.tenant_id(), ctx.auth_context(), plan);
    match shape_response_materialized(MaterializedShapeRequest {
        payload: &response.payload,
        plan,
        plan_kind: describe_plan(plan),
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
                watermark_lsn: response.watermark_lsn.as_u64(),
                error: None,
                auth: None,
                warnings: shaped.notice.into_iter().collect(),
            }
        }
        Ok(ShapeOutcome::Passthrough) => {
            let mut native = NativeResponse::ok(seq);
            native.watermark_lsn = response.watermark_lsn.as_u64();
            native
        }
        Err(error) => shape_error_to_native(seq, &error),
    }
}
