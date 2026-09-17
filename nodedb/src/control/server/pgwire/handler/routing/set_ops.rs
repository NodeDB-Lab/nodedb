// SPDX-License-Identifier: BUSL-1.1

//! Set operation payload merging for pgwire: UNION DISTINCT, INTERSECT,
//! EXCEPT over collected per-task payloads, shaped into a pgwire response.

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::PgWireResult;

use nodedb_physical::physical_task::PostSetOp;

use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::redaction::RedactionCtx;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::set_op_merge::{
    SetMergeMode, dedup_union_payloads, merge_set_op_payloads,
};

use super::super::super::types::sqlstate_error;
use super::super::plan::{PlanKind, multirow_payload_to_response};
use super::super::shape_encode;

/// Apply set operation merging to collected sub-query payloads, then shape and
/// project the merged result into an already-encoded pgwire response.
pub(super) fn apply_set_ops(
    dedup_payloads: &[Vec<u8>],
    dedup_set_op: PostSetOp,
    projection: Option<&OutputSchema>,
    result_formats: &[FieldFormat],
    redaction: Option<RedactionCtx<'_>>,
) -> PgWireResult<(Response, Option<String>)> {
    let merged = match dedup_set_op {
        PostSetOp::Intersect | PostSetOp::IntersectAll => {
            merge_set_op_payloads(dedup_payloads, SetMergeMode::Intersect)
        }
        PostSetOp::Except | PostSetOp::ExceptAll => {
            merge_set_op_payloads(dedup_payloads, SetMergeMode::Except)
        }
        _ => dedup_union_payloads(dedup_payloads),
    };
    Ok(
        match compose::shape_payload_no_plan(&merged, PlanKind::MultiRow, projection, redaction)
            .map_err(|e| sqlstate_error("XX000", e.message()))?
        {
            ShapeOutcome::Rows(shaped) => {
                shape_encode::shaped_query_response(shaped, result_formats)
            }
            ShapeOutcome::Passthrough => {
                let shaped = multirow_payload_to_response(&merged);
                (shaped.response, shaped.notice)
            }
        },
    )
}
