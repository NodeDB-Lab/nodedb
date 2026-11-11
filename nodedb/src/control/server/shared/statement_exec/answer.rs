// SPDX-License-Identifier: BUSL-1.1

//! A request's answer, folded per statement across its tasks: each
//! statement's row sets, its warnings and its one command tag.
//!
//! The fold is protocol-neutral. A transport sums a [`RequestAnswer`] into
//! one [`StatementAnswer`] at its edge. pgwire renders the same tag fold,
//! native renders the sum as one response frame, and HTTP renders it as JSON
//! rows.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{
    DmlOutcome, FoldedTag, PlanKind, ShapedRows, StatementTag, TaskTagRole, describe_plan,
    payload_to_dml_outcome, staged_dml_outcome,
};
use crate::control::server::shared::session::staging_gate::{StagedTagKind, StagedWriteOutcome};
use nodedb_physical::physical_task::PhysicalTask;

use super::error::StatementError;
use super::statement_map::StatementMap;
use super::task_loop::StatementExec;

/// The answer a transport renders: every statement's rows and warnings, in
/// statement order, and one tag over every statement.
pub(crate) struct StatementAnswer {
    /// Each task's shaped rows, in statement order, then dispatch order. A
    /// row set's notice is already in `warnings`.
    pub(crate) rows: Vec<ShapedRows>,
    pub(crate) warnings: Vec<String>,
    /// The request's one tag. `None` when no task folded into it.
    pub(crate) tag: Option<FoldedTag>,
    /// The watermark of the last task the request dispatched.
    pub(crate) last_lsn: u64,
}

/// One statement's part of a request's answer.
pub(crate) struct StatementPart {
    /// The statement's shaped rows, in dispatch order.
    pub(crate) rows: Vec<ShapedRows>,
    pub(crate) warnings: Vec<String>,
    /// The statement's one tag. `None` when no task folded into it.
    pub(crate) tag: Option<FoldedTag>,
}

/// A request's finished answer: one part per statement, in request order.
pub(crate) struct RequestAnswer {
    /// One part per statement. A statement that planned no task answers an
    /// empty part.
    pub(crate) statements: Vec<StatementPart>,
    /// The watermark of the last task the request dispatched.
    pub(crate) last_lsn: u64,
}

impl RequestAnswer {
    /// The one answer a transport renders.
    ///
    /// Rows and warnings follow statement order. The tags fold as one
    /// request tag: the same verb sums, different verbs answer `MIXED` with
    /// the summed count.
    pub(crate) fn summed(self) -> Result<StatementAnswer, StatementError> {
        let mut rows = Vec::new();
        let mut warnings = Vec::new();
        let mut tag = StatementTag::for_request(std::iter::empty());
        for part in self.statements {
            rows.extend(part.rows);
            warnings.extend(part.warnings);
            match part.tag {
                Some(FoldedTag::Dml(outcome)) => tag
                    .fold(TaskTagRole::Counts, outcome)
                    .map_err(StatementError::DmlFold)?,
                Some(FoldedTag::Opaque) => tag.fold_opaque(),
                None => {}
            }
        }
        Ok(StatementAnswer {
            rows,
            warnings,
            tag: tag.finish(),
            last_lsn: self.last_lsn,
        })
    }
}

/// One statement's answer, accumulated across its tasks.
pub(crate) struct StatementFold {
    /// The statement's index in its request. Its rows shape against the
    /// output schema at this index.
    index: usize,
    rows: Vec<ShapedRows>,
    warnings: Vec<String>,
    pub(crate) tag: StatementTag,
}

impl StatementFold {
    /// An empty fold for statement `index`, which planned `plans`. The tag
    /// covers the statement's own plans only, so a derived write beside the
    /// user's own adds no verb and no count.
    pub(crate) fn for_plans<'a>(
        index: usize,
        plans: impl IntoIterator<Item = &'a PhysicalPlan>,
    ) -> Self {
        Self {
            index,
            rows: Vec::new(),
            warnings: Vec::new(),
            tag: StatementTag::for_request(plans),
        }
    }

    /// The output columns the statement announced. `None` when the caller
    /// passed no schema for it.
    pub(crate) fn projection<'e>(&self, exec: &'e StatementExec<'_>) -> Option<&'e OutputSchema> {
        exec.schemas.get(self.index)
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
    pub(crate) async fn fold_rows(
        &mut self,
        exec: &StatementExec<'_>,
        plan: &PhysicalPlan,
        plan_kind: PlanKind,
        payload: &[u8],
    ) -> Result<Option<u64>, StatementError> {
        let redaction =
            QueryRedaction::for_plan(exec.tenant_id(), exec.database_id, exec.scope.auth(), plan);
        match shape_response_materialized(MaterializedShapeRequest {
            payload,
            plan,
            plan_kind,
            projection: self.projection(exec),
            state: exec.state,
            database_id: exec.database_id,
            tenant_id: exec.tenant_id(),
            redaction: Some(redaction.ctx(&exec.state.redaction)),
            sequences: Some(exec.sequences),
        })
        .await
        {
            Ok(ShapeOutcome::Rows(shaped)) => Ok(Some(self.fold_shaped(shaped))),
            Ok(ShapeOutcome::Passthrough) => Ok(None),
            Err(error) => Err(StatementError::Shape(error)),
        }
    }

    /// Fold a write its transaction staged. A write that answers `RETURNING`
    /// folds its rows in place of a count, as an autocommit one does. A
    /// computed-value payload (`RawPayload`) is shaped instead of counted.
    pub(crate) async fn fold_staged(
        &mut self,
        exec: &StatementExec<'_>,
        plan: &PhysicalPlan,
        outcome: StagedWriteOutcome,
    ) -> Result<(), StatementError> {
        if !outcome.returning_rows.is_empty() {
            for rows in &outcome.returning_rows {
                self.fold_rows(exec, plan, PlanKind::ReturningRows, rows)
                    .await?;
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
            .fold_rows(exec, plan, describe_plan(plan), &outcome.payload)
            .await?
            .is_none()
        {
            self.tag.fold_opaque();
        }
        Ok(())
    }

    /// Fold a dispatched task's answer `payload`: its rows, or its count.
    /// Returns the task's own row count for metering, `None` when it carries
    /// none.
    pub(crate) async fn fold_answer(
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
        if let Some(rows) = self.fold_rows(exec, plan, plan_kind, payload).await? {
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

    /// The statement's part of the request's answer.
    pub(crate) fn finish(self) -> StatementPart {
        StatementPart {
            rows: self.rows,
            warnings: self.warnings,
            tag: self.tag.finish(),
        }
    }
}

/// A request's answer, accumulated per statement across its tasks.
pub(crate) struct RequestFold {
    /// One fold per statement, in statement order.
    folds: Vec<StatementFold>,
    /// The statement of each task, in task order.
    task_statements: Vec<usize>,
    /// The index of each statement's last task, in statement order. `None`
    /// for a statement that planned no task.
    last_tasks: Vec<Option<usize>>,
    /// The watermark of the last task the request dispatched.
    pub(crate) last_lsn: u64,
}

impl RequestFold {
    /// An empty fold for a request of `statement_count` statements that
    /// planned `tasks`. `map` names each task's statement. A task with no
    /// entry in `map` is an internal error.
    pub(crate) fn for_tasks(
        tasks: &[PhysicalTask],
        map: &StatementMap,
        statement_count: usize,
    ) -> Result<Self, StatementError> {
        Self::for_plans(tasks.iter().map(|task| &task.plan), map, statement_count)
    }

    /// [`Self::for_tasks`] over the tasks' plans, in task order. Each
    /// statement's tag covers its own plans only.
    pub(crate) fn for_plans<'a>(
        plans: impl IntoIterator<Item = &'a PhysicalPlan>,
        map: &StatementMap,
        statement_count: usize,
    ) -> Result<Self, StatementError> {
        let groups = map
            .group(plans, statement_count)
            .map_err(StatementError::Error)?;
        let folds = groups
            .by_statement
            .into_iter()
            .enumerate()
            .map(|(index, plans)| StatementFold::for_plans(index, plans))
            .collect::<Vec<_>>();
        let mut last_tasks = vec![None; folds.len()];
        for (task, &statement) in groups.task_statements.iter().enumerate() {
            if let Some(last) = last_tasks.get_mut(statement) {
                *last = Some(task);
            }
        }
        Ok(Self {
            folds,
            task_statements: groups.task_statements,
            last_tasks,
            last_lsn: 0,
        })
    }

    /// The number of statements the request holds.
    pub(crate) fn statement_count(&self) -> usize {
        self.folds.len()
    }

    /// The statement task `task` belongs to, or `None` for a task past the
    /// end.
    pub(crate) fn statement_of_task(&self, task: usize) -> Option<usize> {
        self.task_statements.get(task).copied()
    }

    /// The statement whose last task is `task`, or `None` when the
    /// statement of `task` has later tasks.
    ///
    /// A derived task sits at the end of the task set. So a statement with a
    /// derived task ends after every statement planned after it.
    pub(crate) fn statement_ending_at(&self, task: usize) -> Option<usize> {
        let statement = self.statement_of_task(task)?;
        match self.last_tasks.get(statement) {
            Some(Some(last)) if *last == task => Some(statement),
            _ => None,
        }
    }

    /// The fold of the statement task `task` belongs to.
    pub(crate) fn fold_of(&mut self, task: usize) -> Result<&mut StatementFold, StatementError> {
        let fold = match self.task_statements.get(task) {
            Some(&statement) => self.folds.get_mut(statement),
            None => None,
        };
        fold.ok_or_else(|| {
            StatementError::Error(crate::Error::Internal {
                detail: format!("task {task} has no statement fold"),
            })
        })
    }

    /// Mark `error` as raised by the statement task `task` belongs to. The
    /// error of a request of one statement stays as it is.
    pub(crate) fn in_statement(&self, task: usize, error: StatementError) -> StatementError {
        match self.task_statements.get(task) {
            Some(&statement) if self.folds.len() > 1 => StatementError::InStatement {
                statement,
                statements: self.folds.len(),
                error: Box::new(error),
            },
            _ => error,
        }
    }

    /// The request's answer.
    pub(crate) fn finish(self) -> RequestAnswer {
        RequestAnswer {
            statements: self.folds.into_iter().map(StatementFold::finish).collect(),
            last_lsn: self.last_lsn,
        }
    }
}

#[cfg(test)]
#[path = "answer_tests.rs"]
mod tests;
