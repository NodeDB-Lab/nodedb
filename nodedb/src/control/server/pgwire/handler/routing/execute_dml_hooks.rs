// SPDX-License-Identifier: BUSL-1.1

//! Pre-dispatch hooks for the `dispatch_task_loop` autocommit write path:
//! truncate `restart_identity` extraction and clone CoW write-path
//! interception. A statement in a transaction routes each task through the
//! shared `txn_route` instead, which fires its triggers and takes its clone
//! copy-on-write steps inside the transaction.

use pgwire::api::portal::Format;
use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{
    DmlOutcome, StatementTag, TaskTagRole, payload_to_dml_outcome,
};
use crate::control::server::shared::session::SessionId;
use crate::control::trigger::dml_hook::DmlWriteInfo;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::types::{
    dml_fold_error_to_pg, error_to_pg, error_to_sqlstate, shape_error_to_pg,
};
use super::super::core::NodeDbPgHandler;
use super::super::plan::PlanKind;

/// What a write task handled short of normal dispatch contributes to the
/// statement's one command tag.
pub(super) enum HandledWrite {
    /// A count-bearing outcome: folds into the statement tag.
    Dml(DmlOutcome),
    /// No count and no verb (a buffered write, an INSTEAD OF body that
    /// replaced an opaque plan): the statement renders `OK` unless a DML
    /// outcome is folded too.
    Opaque,
}

impl HandledWrite {
    /// Fold this contribution, made by a task whose role is `role`, into the
    /// statement's tag.
    pub(super) fn fold_into(
        self,
        statement_tag: &mut StatementTag,
        role: TaskTagRole,
    ) -> PgWireResult<()> {
        match self {
            HandledWrite::Dml(outcome) => statement_tag
                .fold(role, outcome)
                .map_err(|e| dml_fold_error_to_pg(&e)),
            HandledWrite::Opaque => {
                statement_tag.fold_opaque();
                Ok(())
            }
        }
    }
}

/// What a pre-dispatch hook answered the task with.
pub(super) enum PreDispatchHandled {
    /// A clone write's `RETURNING` rows, encoded. Caller pushes the response.
    Rows(Response),
    /// A write's contribution to the statement tag. Caller folds it.
    Write(HandledWrite),
}

/// Outcome of running the pre-dispatch hooks for a single task.
pub(super) enum PreDispatchOutcome {
    /// The task was fully handled by clone write interception. Caller emits
    /// the answer and continues the loop.
    Handled(PreDispatchHandled),
    /// No interception occurred; caller proceeds to normal dispatch with the
    /// (possibly retargeted) task and its write classification.
    /// Boxed: `PhysicalTask` makes this variant far larger than `Handled`,
    /// which will otherwise bloat every `PreDispatchOutcome` on the stack.
    Proceed(Box<PreDispatchProceed>),
}

/// Payload for [`PreDispatchOutcome::Proceed`], boxed to keep the enum small.
pub(super) struct PreDispatchProceed {
    pub(super) task: PhysicalTask,
    pub(super) dml_info: Option<DmlWriteInfo>,
    pub(super) truncate_restart_collection: Option<String>,
}

/// Per-statement inputs the pre-dispatch hooks need alongside the task.
///
/// Grouped rather than passed positionally: the hooks need the requester, the
/// session, the plan's response classification and the statement's announced
/// output columns, and a positional list that long is easy to transpose.
#[derive(Clone, Copy)]
pub(super) struct PreDispatchContext<'a> {
    pub(super) identity: &'a AuthenticatedIdentity,
    pub(super) auth: &'a AuthContext,
    pub(super) tenant_id: TenantId,
    pub(super) session_id: SessionId,
    pub(super) plan_kind: PlanKind,
    /// The statement's resolved output columns, when any were announced to the
    /// client. A hook that answers the statement itself (clone write-path
    /// interception) shapes its rows against these, exactly as the normal
    /// dispatch path does — the client holds one RowDescription either way.
    pub(super) projection: Option<&'a OutputSchema>,
    /// The client's result-format request, which a hook that answers the
    /// statement itself honours as the normal dispatch path does.
    pub(super) result_formats: &'a Format,
}

impl NodeDbPgHandler {
    /// Run clone write-path interception for a single write task, before it
    /// reaches normal dispatch.
    pub(super) async fn run_pre_dispatch_hooks(
        &self,
        context: PreDispatchContext<'_>,
        mut task: PhysicalTask,
    ) -> PgWireResult<PreDispatchOutcome> {
        let PreDispatchContext {
            identity,
            tenant_id,
            ..
        } = context;
        // A write whose collection fires a BEFORE, INSTEAD OF or SYNC AFTER
        // body never reaches this path: it runs in its statement's
        // transaction (`txn_route`). The classification feeds the
        // post-dispatch auto-analyze.
        let dml_info = crate::control::trigger::dml_hook::classify_dml_write(&task.plan);

        // Extract truncate restart_identity info before task is moved.
        // Engine-neutral: `truncate_target` names every truncate-shaped op.
        let truncate_restart_collection = match task.plan.truncate_target() {
            Some((collection, true)) => Some(collection.to_string()),
            Some((_, false)) | None => None,
        };

        // --- Clone write-path interception ---
        // Protocol-neutral hook (`shared::clone_write`); native, RESP, and
        // HTTP each run it once at their own dispatch entry point instead.
        {
            use crate::control::server::shared::clone_write::{
                CloneWriteOutcome, maybe_intercept_clone_write,
            };
            match maybe_intercept_clone_write(&self.state, &mut task, identity, tenant_id)
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })? {
                CloneWriteOutcome::Handled(resp) => {
                    return Ok(PreDispatchOutcome::Handled(
                        self.clone_write_answer(context, &task.plan, &resp)?,
                    ));
                }
                CloneWriteOutcome::Passthrough => {}
            }
        }

        Ok(PreDispatchOutcome::Proceed(Box::new(PreDispatchProceed {
            task,
            dml_info,
            truncate_restart_collection,
        })))
    }
}

impl NodeDbPgHandler {
    /// The answer a clone copy-on-write that handled a write gives the
    /// statement: its `RETURNING` rows, or its count.
    pub(super) fn clone_write_answer(
        &self,
        context: PreDispatchContext<'_>,
        plan: &nodedb_physical::physical_plan::PhysicalPlan,
        resp: &crate::bridge::envelope::Response,
    ) -> PgWireResult<PreDispatchHandled> {
        use crate::control::server::response_shape::compose::{
            ShapeOutcome, shape_payload_no_plan,
        };
        use crate::control::server::response_shape::redaction::QueryRedaction;
        let PreDispatchContext {
            auth,
            tenant_id,
            session_id,
            plan_kind,
            projection,
            result_formats,
            ..
        } = context;
        // A clone write can carry RETURNING rows, which deliver stored column
        // values as a SELECT does.
        let redaction = QueryRedaction::for_plan(tenant_id, auth, plan);
        // A clone write's RETURNING list names stored columns only, never a
        // Control-Plane computed column.
        match shape_payload_no_plan(
            resp.payload.as_ref(),
            plan_kind,
            projection,
            Some(redaction.ctx(&self.state.redaction)),
            None,
        )
        .map_err(|e| shape_error_to_pg(&e))?
        {
            ShapeOutcome::Rows(shaped) => {
                // Clone write-path RETURNING rows (PointUpdate/PointDelete),
                // in the client's requested result formats.
                let (response, notice) =
                    crate::control::server::pgwire::handler::shape_encode::shaped_query_response(
                        shaped,
                        result_formats,
                    );
                if let Some(n) = notice {
                    self.sessions.push_notice(session_id, n);
                }
                Ok(PreDispatchHandled::Rows(response))
            }
            ShapeOutcome::Passthrough => {
                let handled = match payload_to_dml_outcome(resp.payload.as_ref(), plan_kind)
                    .map_err(|e| error_to_pg(&e))?
                {
                    Some(outcome) => HandledWrite::Dml(outcome),
                    None => HandledWrite::Opaque,
                };
                Ok(PreDispatchHandled::Write(handled))
            }
        }
    }
}
