// SPDX-License-Identifier: BUSL-1.1

//! One statement's answer, folded across its tasks: its row sets, its
//! warnings and its one command tag.
//!
//! The fold is protocol-neutral. pgwire renders the same tag fold, native
//! renders a [`StatementAnswer`] as one response frame, and HTTP renders it as
//! JSON rows.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::{
    DmlOutcome, FoldedTag, PlanKind, ShapedRows, StatementTag, TaskTagRole, describe_plan,
    payload_to_dml_outcome, staged_dml_outcome,
};
use crate::control::server::shared::session::staging_gate::{StagedTagKind, StagedWriteOutcome};
use nodedb_physical::physical_task::PhysicalTask;

use super::error::StatementError;
use super::task_loop::StatementExec;

/// A statement's finished answer.
pub(crate) struct StatementAnswer {
    /// Each task's shaped rows, in dispatch order. A row set's notice is
    /// already in `warnings`.
    pub(crate) rows: Vec<ShapedRows>,
    pub(crate) warnings: Vec<String>,
    /// The statement's one tag. `None` when no task folded into it.
    pub(crate) tag: Option<FoldedTag>,
    /// The watermark of the last task the statement dispatched.
    pub(crate) last_lsn: u64,
}

/// One statement's answer, accumulated across its tasks.
pub(crate) struct StatementFold {
    rows: Vec<ShapedRows>,
    warnings: Vec<String>,
    pub(crate) tag: StatementTag,
    pub(crate) last_lsn: u64,
}

impl StatementFold {
    /// An empty fold for the statement that planned `tasks`. The tag is built
    /// over every task, so a derived write beside the user's own adds no
    /// verb and no count.
    pub(crate) fn for_tasks(tasks: &[PhysicalTask]) -> Self {
        Self {
            rows: Vec::new(),
            warnings: Vec::new(),
            tag: StatementTag::for_plans(tasks.iter().map(|t| &t.plan)),
            last_lsn: 0,
        }
    }

    /// Fold the count-bearing outcome of the task that ran `plan` into the
    /// statement's tag.
    pub(crate) fn fold_dml(
        &mut self,
        plan: &PhysicalPlan,
        outcome: DmlOutcome,
    ) -> Result<(), StatementError> {
        self.tag
            .fold(self.tag.role_of(plan), outcome)
            .map_err(StatementError::DmlFold)
    }

    /// Fold one shaped row set. Returns its row count.
    pub(crate) fn fold_shaped(&mut self, mut shaped: ShapedRows) -> u64 {
        if let Some(notice) = shaped.notice.take() {
            self.warnings.push(notice);
        }
        let count = shaped.rows.len() as u64;
        self.rows.push(shaped);
        count
    }

    /// Shape `payload`, the answer `plan` gave as `plan_kind`, and fold its
    /// rows. Returns the row count, `None` when the payload carries no rows.
    pub(crate) fn fold_rows(
        &mut self,
        exec: &StatementExec<'_>,
        plan: &PhysicalPlan,
        plan_kind: PlanKind,
        payload: &[u8],
    ) -> Result<Option<u64>, StatementError> {
        let redaction = QueryRedaction::for_plan(exec.tenant_id(), exec.scope.auth(), plan);
        match shape_response_materialized(MaterializedShapeRequest {
            payload,
            plan,
            plan_kind,
            projection: exec.output_schema,
            state: exec.state,
            database_id: exec.database_id,
            tenant_id: exec.tenant_id(),
            redaction: Some(redaction.ctx(&exec.state.redaction)),
            sequences: Some(exec.sequences),
        }) {
            Ok(ShapeOutcome::Rows(shaped)) => Ok(Some(self.fold_shaped(shaped))),
            Ok(ShapeOutcome::Passthrough) => Ok(None),
            Err(error) => Err(StatementError::Shape(error)),
        }
    }

    /// Fold a write its transaction staged. A write that answers `RETURNING`
    /// folds its rows in place of a count, as an autocommit one does. A
    /// computed-value payload (`RawPayload`) is shaped instead of counted.
    pub(crate) fn fold_staged(
        &mut self,
        exec: &StatementExec<'_>,
        plan: &PhysicalPlan,
        outcome: StagedWriteOutcome,
    ) -> Result<(), StatementError> {
        if !outcome.returning_rows.is_empty() {
            for rows in &outcome.returning_rows {
                self.fold_rows(exec, plan, PlanKind::ReturningRows, rows)?;
            }
            return Ok(());
        }
        if !matches!(outcome.kind, StagedTagKind::RawPayload) {
            return self.fold_dml(plan, staged_dml_outcome(outcome.kind, outcome.affected));
        }
        if outcome.payload.is_empty() {
            self.tag.fold_opaque();
            return Ok(());
        }
        // The value rides in the payload, never as a count.
        if self
            .fold_rows(exec, plan, describe_plan(plan), &outcome.payload)?
            .is_none()
        {
            self.tag.fold_opaque();
        }
        Ok(())
    }

    /// Fold a dispatched task's answer `payload`: its rows, or its count.
    /// Returns the task's own row count for metering, `None` when it carries
    /// none.
    pub(crate) fn fold_answer(
        &mut self,
        exec: &StatementExec<'_>,
        plan: &PhysicalPlan,
        payload: &[u8],
    ) -> Result<Option<u64>, StatementError> {
        let plan_kind = describe_plan(plan);
        let count_bearing = matches!(plan_kind, PlanKind::DmlResult(_) | PlanKind::DmlResultByOp);
        if payload.is_empty() && !count_bearing {
            // Not a count-bearing plan (graph / vector / index write): no
            // count and no verb to report. A count-bearing plan with no
            // payload falls through so the count reader refuses it.
            self.tag.fold_opaque();
            return Ok(None);
        }
        if let Some(rows) = self.fold_rows(exec, plan, plan_kind, payload)? {
            return Ok(Some(rows));
        }
        // A count-bearing write reports the rows it touched and its verb; an
        // opaque execution reports neither. A count of one per dispatched
        // task reports a row for a delete that removed nothing and for an
        // `ON CONFLICT DO NOTHING` insert that skipped.
        if self.tag.role_of(plan) == TaskTagRole::Opaque {
            self.tag.fold_opaque();
            return Ok(None);
        }
        match payload_to_dml_outcome(payload, plan_kind) {
            Ok(Some(outcome)) => {
                let affected = outcome.affected;
                self.fold_dml(plan, outcome)?;
                Ok(Some(affected))
            }
            Ok(None) => {
                self.tag.fold_opaque();
                Ok(None)
            }
            Err(error) => Err(StatementError::Error(error)),
        }
    }

    /// The statement's answer.
    pub(crate) fn finish(self) -> StatementAnswer {
        StatementAnswer {
            rows: self.rows,
            warnings: self.warnings,
            tag: self.tag.finish(),
            last_lsn: self.last_lsn,
        }
    }
}
