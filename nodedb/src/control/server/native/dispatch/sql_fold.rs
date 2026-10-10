// SPDX-License-Identifier: BUSL-1.1

//! Folding each task's answer into one native SQL statement's response: its
//! columns and rows, its warnings, and its one command tag.

use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::sequence::SessionSequenceAccess;
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{
    DmlOutcome, FoldedTag, PlanKind, StatementTag, describe_plan, staged_dml_outcome,
};
use crate::control::server::shared::session::staging_gate::{StagedTagKind, StagedWriteOutcome};
use crate::types::DatabaseId;
use nodedb_physical::physical_task::PhysicalTask;

use super::{
    DispatchCtx, apply_dml_outcome, dml_fold_error_to_native, shape_error_to_native,
    to_native_columns_rows,
};

/// What shaping a task's answer reads.
pub(super) struct FoldShape<'a> {
    pub seq: u64,
    pub output_schema: Option<&'a OutputSchema>,
    pub database_id: DatabaseId,
    /// The concrete session access, never `&dyn SequenceAccess`: the trait
    /// object is not `Sync`, and the shape lives across the dispatch awaits
    /// of a `Send` connection future.
    pub sequences: &'a SessionSequenceAccess<'a>,
}

/// One statement's response, accumulated across its tasks.
pub(super) struct StatementFold {
    pub columns: Option<Vec<String>>,
    pub rows: Vec<Vec<Value>>,
    pub warnings: Vec<String>,
    pub tag: StatementTag,
    pub last_lsn: u64,
}

impl StatementFold {
    /// An empty fold for the statement that planned `tasks`. The tag is built
    /// over every task, so a derived write beside the user's own adds no
    /// verb and no count.
    pub(super) fn for_tasks(tasks: &[PhysicalTask]) -> Self {
        Self {
            columns: None,
            rows: Vec::new(),
            warnings: Vec::new(),
            tag: StatementTag::for_plans(tasks.iter().map(|t| &t.plan)),
            last_lsn: 0,
        }
    }

    /// Fold the count-bearing outcome of the task that ran `plan` into the
    /// statement's tag.
    pub(super) fn fold_dml(
        &mut self,
        seq: u64,
        plan: &PhysicalPlan,
        outcome: DmlOutcome,
    ) -> Result<(), Box<NativeResponse>> {
        self.tag
            .fold(self.tag.role_of(plan), outcome)
            .map_err(|e| Box::new(dml_fold_error_to_native(seq, &e)))
    }

    /// Shape `payload`, the answer `plan` gave as `plan_kind`, and fold its
    /// rows. Returns the row count, `None` when the payload carries no rows.
    pub(super) fn fold_rows(
        &mut self,
        ctx: &DispatchCtx<'_>,
        shape: &FoldShape<'_>,
        plan: &PhysicalPlan,
        plan_kind: PlanKind,
        payload: &[u8],
    ) -> Result<Option<u64>, Box<NativeResponse>> {
        let redaction = QueryRedaction::for_plan(ctx.tenant_id(), ctx.auth_context(), plan);
        match shape_response_materialized(MaterializedShapeRequest {
            payload,
            plan,
            plan_kind,
            projection: shape.output_schema,
            state: ctx.state,
            database_id: shape.database_id,
            tenant_id: ctx.tenant_id(),
            redaction: Some(redaction.ctx(&ctx.state.redaction)),
            sequences: Some(shape.sequences),
        }) {
            Ok(ShapeOutcome::Rows(mut shaped)) => {
                if let Some(notice) = shaped.notice.take() {
                    self.warnings.push(notice);
                }
                let (cols, rows) = to_native_columns_rows(&shaped);
                if !cols.is_empty() && self.columns.is_none() {
                    self.columns = Some(cols);
                }
                let count = rows.len() as u64;
                self.rows.extend(rows);
                Ok(Some(count))
            }
            Ok(ShapeOutcome::Passthrough) => Ok(None),
            Err(e) => Err(Box::new(shape_error_to_native(shape.seq, &e))),
        }
    }

    /// Fold a write its transaction staged. A write that answers `RETURNING`
    /// folds its rows in place of a count, as an autocommit one does. A
    /// computed-value payload (`RawPayload`) is shaped instead of counted.
    pub(super) fn fold_staged(
        &mut self,
        ctx: &DispatchCtx<'_>,
        shape: &FoldShape<'_>,
        plan: &PhysicalPlan,
        outcome: StagedWriteOutcome,
    ) -> Result<(), Box<NativeResponse>> {
        if !outcome.returning_rows.is_empty() {
            for rows in &outcome.returning_rows {
                self.fold_rows(ctx, shape, plan, PlanKind::ReturningRows, rows)?;
            }
            return Ok(());
        }
        if !matches!(outcome.kind, StagedTagKind::RawPayload) {
            return self.fold_dml(
                shape.seq,
                plan,
                staged_dml_outcome(outcome.kind, outcome.affected),
            );
        }
        if outcome.payload.is_empty() {
            self.tag.fold_opaque();
            return Ok(());
        }
        // The value rides in the payload, never as a count.
        if self
            .fold_rows(ctx, shape, plan, describe_plan(plan), &outcome.payload)?
            .is_none()
        {
            self.tag.fold_opaque();
        }
        Ok(())
    }

    /// The statement's one response.
    pub(super) fn finish(self, seq: u64) -> NativeResponse {
        let mut r = NativeResponse::ok(seq);
        r.watermark_lsn = self.last_lsn;
        r.warnings = self.warnings;
        if !self.rows.is_empty() {
            r.columns = self.columns;
            r.rows = Some(self.rows);
        }
        match self.tag.finish() {
            Some(FoldedTag::Dml(outcome)) => apply_dml_outcome(&mut r, outcome),
            Some(FoldedTag::Opaque) | None => {}
        }
        r
    }
}
