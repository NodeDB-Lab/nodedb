// SPDX-License-Identifier: BUSL-1.1

//! Shaping of one dispatched task's Data-Plane response into pgwire output.

use std::sync::Arc;

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::sequence::{SessionSequenceAccess, SessionSequenceValues};
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::session::SessionId;
use crate::types::{DatabaseId, TenantId};

use super::super::super::super::types::shape_error_to_pg;
use super::super::super::core::NodeDbPgHandler;
use super::super::super::plan::{PlanKind, payload_to_response};
use super::super::super::shape_encode;

/// Everything needed to shape one task's response.
pub(super) struct ShapeTaskParams<'a> {
    pub(super) response: &'a crate::bridge::envelope::Response,
    pub(super) plan: &'a PhysicalPlan,
    pub(super) plan_kind: PlanKind,
    pub(super) projection: Option<&'a OutputSchema>,
    pub(super) result_formats: &'a [FieldFormat],
    pub(super) session_id: SessionId,
    pub(super) tenant_id: TenantId,
    pub(super) database_id: DatabaseId,
    pub(super) auth_ctx: &'a crate::control::security::auth_context::AuthContext,
    /// This connection's `currval` map, for the projection's Control-Plane
    /// computed columns.
    pub(super) session_sequences: Option<Arc<SessionSequenceValues>>,
}

impl NodeDbPgHandler {
    /// Shape one task's response: rows are encoded and pushed onto
    /// `responses`, except `RETURNING` rows, which fold into
    /// `returning_rows` so the statement answers with one result set.
    ///
    /// Returns the task's own row count for metering, `None` for a
    /// passthrough response with no row payload to count.
    pub(super) fn shape_task_response(
        &self,
        params: ShapeTaskParams<'_>,
        responses: &mut Vec<Response>,
        returning_rows: &mut Option<ShapedRows>,
    ) -> PgWireResult<Option<u64>> {
        let ShapeTaskParams {
            response,
            plan,
            plan_kind,
            projection,
            result_formats,
            session_id,
            tenant_id,
            database_id,
            auth_ctx,
            session_sequences,
        } = params;
        let redaction = QueryRedaction::for_plan(tenant_id, auth_ctx, plan);
        let sequences = SessionSequenceAccess::for_session(
            &self.state,
            session_sequences,
            database_id,
            tenant_id,
        );
        match compose::shape_response_materialized(MaterializedShapeRequest {
            payload: &response.payload,
            plan,
            plan_kind,
            projection,
            state: &self.state,
            database_id,
            tenant_id,
            redaction: Some(redaction.ctx(&self.state.redaction)),
            sequences: Some(&sequences),
        })
        .map_err(|e| shape_error_to_pg(&e))?
        {
            ShapeOutcome::Rows(shaped) => {
                let task_rows = shaped.rows.len() as u64;
                if matches!(plan_kind, PlanKind::ReturningRows) {
                    // Folded, not emitted: the whole statement answers with
                    // one result set after the loop.
                    match returning_rows {
                        Some(accumulated) => accumulated.append(shaped),
                        None => *returning_rows = Some(shaped),
                    }
                } else {
                    let (encoded, notice) =
                        shape_encode::shaped_query_response(shaped, result_formats);
                    if let Some(n) = notice {
                        self.sessions.push_notice(session_id, n);
                    }
                    responses.push(encoded);
                }
                Ok(Some(task_rows))
            }
            ShapeOutcome::Passthrough => {
                let shaped = payload_to_response(&response.payload, plan_kind)?;
                if let Some(notice) = shaped.notice {
                    self.sessions.push_notice(session_id, notice);
                }
                responses.push(shaped.response);
                Ok(None)
            }
        }
    }
}
