// SPDX-License-Identifier: BUSL-1.1

//! Folds forwarded task payloads into the statement's one answer: rows pushed
//! as they arrive, `RETURNING` rows as one result set, and every write folded
//! through [`StatementTag`] into one command tag — the same tail
//! `dispatch_loop/finish.rs` emits for a locally dispatched statement.

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::PgWireResult;

use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{
    ShapedRows, StatementTag, payload_to_dml_outcome,
};
use crate::control::server::shared::session::SessionId;

use super::super::super::command_tag::push_folded_tag;
use super::super::super::types::{dml_fold_error_to_pg, error_to_pg, shape_error_to_pg};
use super::super::core::NodeDbPgHandler;
use super::super::plan::PlanKind;
use super::super::shape_encode;

/// How forwarded rows shape back to the client.
pub(super) struct GatewayShaping<'a> {
    pub(super) projection: Option<&'a OutputSchema>,
    pub(super) result_formats: &'a [FieldFormat],
    /// Resolved once over the whole forwarded task set.
    pub(super) redaction: &'a QueryRedaction,
    /// Receives a shaped row set's NOTICE.
    pub(super) session_id: SessionId,
}

/// The statement's answer, accumulated across every forwarded task.
pub(super) struct GatewayFold {
    responses: Vec<Response>,
    /// The statement's `RETURNING` rows, folded across every task.
    returning_rows: Option<ShapedRows>,
    /// The statement's command tag, folded across every write task.
    statement_tag: StatementTag,
}

impl GatewayFold {
    pub(super) fn with_capacity(tasks: usize) -> Self {
        Self {
            responses: Vec::with_capacity(tasks),
            returning_rows: None,
            statement_tag: StatementTag::default(),
        }
    }
}

impl NodeDbPgHandler {
    /// The statement's responses: its rows, then its one command tag.
    pub(super) fn finish_gateway_fold(
        &self,
        fold: GatewayFold,
        shaping: &GatewayShaping<'_>,
    ) -> Vec<Response> {
        let GatewayFold {
            mut responses,
            returning_rows,
            statement_tag,
        } = fold;
        if let Some(shaped) = returning_rows {
            let (response, notice) =
                shape_encode::shaped_query_response(shaped, shaping.result_formats);
            if let Some(n) = notice {
                self.sessions.push_notice(shaping.session_id, n);
            }
            responses.push(response);
        }
        // A statement never carries both RETURNING rows and a count-bearing
        // tag: `RETURNING` classifies as `ReturningRows`, which folds rows
        // and no count.
        push_folded_tag(&mut responses, statement_tag.finish());
        responses
    }

    /// Fold one forwarded payload by its task's plan kind: rows push onto
    /// the fold, `RETURNING` rows accumulate into one result set, a
    /// passthrough payload folds its count and verb into the statement's
    /// tag, and an opaque execution folds as such. A task whose count does
    /// not answer the statement (`counts_toward_tag` false: a derived
    /// implicit-edge write beside the user's own) folds as opaque.
    ///
    /// Returns the rows the payload decoded to, `None` for a passthrough
    /// payload with no row count.
    pub(super) fn fold_gateway_payload(
        &self,
        fold: &mut GatewayFold,
        payload: &[u8],
        plan_kind: PlanKind,
        counts_toward_tag: bool,
        shaping: &GatewayShaping<'_>,
    ) -> PgWireResult<Option<u64>> {
        // Gateway forwarding carries no sequence access: a projection with
        // Control-Plane computed columns is refused by the shaper rather
        // than NULL-filled.
        match compose::shape_payload_no_plan(
            payload,
            plan_kind,
            shaping.projection,
            Some(shaping.redaction.ctx(&self.state.redaction)),
            None,
        )
        .map_err(|e| shape_error_to_pg(&e))?
        {
            ShapeOutcome::Rows(shaped) => {
                let rows = shaped.rows.len() as u64;
                if matches!(plan_kind, PlanKind::ReturningRows) {
                    match &mut fold.returning_rows {
                        Some(accumulated) => accumulated.append(shaped),
                        None => fold.returning_rows = Some(shaped),
                    }
                } else {
                    let (response, notice) =
                        shape_encode::shaped_query_response(shaped, shaping.result_formats);
                    if let Some(n) = notice {
                        self.sessions.push_notice(shaping.session_id, n);
                    }
                    fold.responses.push(response);
                }
                Ok(Some(rows))
            }
            ShapeOutcome::Passthrough => {
                if !counts_toward_tag {
                    fold.statement_tag.fold_opaque();
                    return Ok(None);
                }
                // A count-bearing plan with no payload is refused by the
                // count reader, never answered without a count.
                match payload_to_dml_outcome(payload, plan_kind).map_err(|e| error_to_pg(&e))? {
                    Some(outcome) => fold
                        .statement_tag
                        .fold(outcome)
                        .map_err(|e| dml_fold_error_to_pg(&e))?,
                    None => fold.statement_tag.fold_opaque(),
                }
                Ok(None)
            }
        }
    }
}
