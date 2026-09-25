// SPDX-License-Identifier: BUSL-1.1

//! Pre-dispatch hook interception for the `dispatch_task_loop` write path:
//! BEFORE/INSTEAD OF trigger firing (with OLD-row fetch and probe-driven
//! event reclassification), truncate `restart_identity` extraction, and
//! clone CoW write-path interception. Split out of `execute.rs` to keep
//! that file under the file-size limit, with no behavior change from
//! running inline in the per-task dispatch loop.

use std::collections::HashMap;
use std::sync::Arc;

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{
    DmlOutcome, StatementTag, payload_to_dml_outcome, staged_dml_outcome,
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
    /// No count and no verb (a buffered write, a trigger that consumed an
    /// opaque plan): the statement renders `OK` unless a DML outcome is
    /// folded too.
    Opaque,
}

impl HandledWrite {
    /// Fold this contribution into the statement's tag.
    pub(super) fn fold_into(self, statement_tag: &mut StatementTag) -> PgWireResult<()> {
        match self {
            HandledWrite::Dml(outcome) => statement_tag
                .fold(outcome)
                .map_err(|e| dml_fold_error_to_pg(&e)),
            HandledWrite::Opaque => {
                statement_tag.fold_opaque();
                Ok(())
            }
        }
    }
}

/// Outcome of routing a single task through the in-transaction staging gate.
pub(super) enum TxnRouteOutcome {
    /// Not staged/buffered: caller proceeds to normal dispatch with the
    /// (possibly `txn_id`-stamped) task.
    Proceed(Box<PhysicalTask>),
    /// Fully handled (buffered, or a staged write with its real count).
    /// Caller folds this into the statement tag and continues the loop.
    Handled(HandledWrite),
}

impl NodeDbPgHandler {
    /// Route a single task through the protocol-neutral in-transaction
    /// staging gate (`shared::session::staging_gate`), translating its
    /// outcome into this file's `PgWireResult`. A constraint violation on a
    /// staged write surfaces here as the pgwire error, matching the
    /// pre-refactor `stage_in_tx_point_write` behavior exactly.
    pub(super) async fn route_task_in_txn(
        &self,
        session_id: SessionId,
        identity: &AuthenticatedIdentity,
        task: PhysicalTask,
        plan_lease_scope: Arc<crate::control::lease::QueryLeaseScope>,
    ) -> PgWireResult<TxnRouteOutcome> {
        use crate::control::server::shared::session::expander_stage::{
            ExpanderOutcome, route_in_tx_expander,
        };
        use crate::control::server::shared::session::staging_gate::{
            InTxnRoute, StagingGateError, route_in_tx_write,
        };

        let user_id: Option<std::sync::Arc<str>> =
            Some(std::sync::Arc::from(identity.username.as_str()));

        // In-transaction `MERGE` and `UPDATE ... FROM` are resolved + staged at
        // STATEMENT time by the expander (read-your-own-writes for later
        // statements in the same txn); every other task falls through to the
        // neutral staging gate. The expander dispatches each derived point op via
        // the SAME closure, so it must be `Fn` — hence `user_id.clone()` per call.
        let buffer_start = self.sessions.buffered_task_count(session_id);
        let routed = match route_in_tx_expander(
            &self.state,
            &self.sessions,
            session_id,
            task,
            |stage_task| self.dispatch_authorized_task(stage_task, user_id.clone(), identity),
        )
        .await
        {
            Ok(ExpanderOutcome::Handled(route)) => Ok(route),
            Ok(ExpanderOutcome::Passthrough(task)) => {
                route_in_tx_write(
                    &self.state,
                    &self.sessions,
                    session_id,
                    *task,
                    |stage_task| {
                        self.dispatch_authorized_task(stage_task, user_id.clone(), identity)
                    },
                )
                .await
            }
            Err(e) => Err(e),
        };

        if self.sessions.buffered_task_count(session_id) > buffer_start
            && !self.sessions.attach_tx_lease_scope_since(
                session_id,
                buffer_start,
                plan_lease_scope,
            )
        {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "internal error: failed to retain descriptor leases for buffered transaction tasks"
                    .to_owned(),
            ))));
        }

        match routed {
            // The pgwire dispatch proposes a write through Raft or appends its
            // redo record in the funnel.
            Ok(InTxnRoute::Read(routed_task) | InTxnRoute::Autocommit(routed_task)) => {
                Ok(TxnRouteOutcome::Proceed(routed_task))
            }
            Ok(InTxnRoute::Buffered) => Ok(TxnRouteOutcome::Handled(HandledWrite::Opaque)),
            Ok(InTxnRoute::Staged(outcome)) => Ok(TxnRouteOutcome::Handled(HandledWrite::Dml(
                staged_dml_outcome(outcome.kind, outcome.affected),
            ))),
            Err(StagingGateError::Dispatch(e)) => {
                let (severity, code, message) = error_to_sqlstate(&e);
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                ))))
            }
            Err(StagingGateError::Rejected { code }) => {
                let (severity, sqlstate, message) = match code {
                    Some(code) => {
                        crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(&code)
                    }
                    None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
                };
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    sqlstate.to_owned(),
                    message,
                ))))
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
    /// The task was fully handled (trigger short-circuit, or clone write
    /// interception). Caller emits the answer and continues the loop.
    Handled(PreDispatchHandled),
    /// No interception occurred (or a mutation was applied in place);
    /// caller proceeds to normal dispatch with the (possibly mutated) task
    /// and the trigger bookkeeping needed for the AFTER-trigger phase.
    /// Boxed: `PhysicalTask` makes this variant far larger than `Handled`,
    /// which would otherwise bloat every `PreDispatchOutcome` on the stack.
    Proceed(Box<PreDispatchProceed>),
}

/// Payload for [`PreDispatchOutcome::Proceed`], boxed to keep the enum small.
pub(super) struct PreDispatchProceed {
    pub(super) task: PhysicalTask,
    pub(super) dml_info: Option<DmlWriteInfo>,
    pub(super) old_row: Option<HashMap<String, nodedb_types::Value>>,
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
}

impl NodeDbPgHandler {
    /// Run trigger interception and clone write-path interception for a
    /// single write task, before it reaches normal dispatch.
    pub(super) async fn run_pre_dispatch_hooks(
        &self,
        context: PreDispatchContext<'_>,
        mut task: PhysicalTask,
    ) -> PgWireResult<PreDispatchOutcome> {
        let PreDispatchContext {
            identity,
            auth,
            tenant_id,
            session_id,
            plan_kind,
            projection,
        } = context;
        // --- Trigger interception for DML writes ---
        let mut dml_info = crate::control::trigger::dml_hook::classify_dml_write(&task.plan);

        // The OLD read must retain the exact database identity of the task,
        // rather than re-resolving mutable session state.
        let database_id = task.database_id;

        // Fetch OLD row and fire BEFORE/INSTEAD OF triggers if applicable.
        let old_row = if let Some(ref info) = dml_info
            && info.document_id.is_some()
            && (matches!(
                info.event,
                crate::control::trigger::DmlEvent::Update
                    | crate::control::trigger::DmlEvent::Delete
            ) || info.needs_existence_probe)
        {
            let doc_id = info.document_id.as_deref().unwrap_or("");
            // `info.collection` is already qualified (from `DocumentOp`) —
            // rebuild rather than re-qualify.
            let row = crate::control::trigger::dml_hook::fetch_old_row(
                &self.state,
                identity,
                database_id,
                auth,
                &nodedb_types::QualifiedCollection::from_stored(info.collection.clone()),
                doc_id,
            )
            .await
            .map_err(|error| {
                let (severity, code, message) = error_to_sqlstate(&error);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;
            if !row.is_empty() { Some(row) } else { None }
        } else {
            None
        };

        // Probe-driven reclassification.
        if let Some(ref mut info) = dml_info
            && info.needs_existence_probe
        {
            info.event = if old_row.is_some() {
                crate::control::trigger::DmlEvent::Update
            } else {
                crate::control::trigger::DmlEvent::Insert
            };
        }

        if let Some(ref info) = dml_info {
            use crate::control::trigger::dml_hook_fire::PreDispatchResult;
            match crate::control::trigger::dml_hook_fire::fire_pre_dispatch_triggers(
                crate::control::trigger::dml_hook_fire::DispatchTriggerParams {
                    state: &self.state,
                    identity,
                    database_id,
                    tenant_id,
                    info,
                    old_row: &old_row,
                    cascade_depth: 0,
                },
            )
            .await
            .map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })? {
                PreDispatchResult::Handled => {
                    // The trigger consumed the row: the statement ran and
                    // affected nothing. A count-bearing plan keeps its verb
                    // with a zero count so the fold stays on one verb; an
                    // opaque plan contributes no count.
                    let handled = match plan_kind {
                        PlanKind::DmlResult(verb) => {
                            HandledWrite::Dml(DmlOutcome { verb, affected: 0 })
                        }
                        // The verb is resolved at apply time and no apply
                        // happened. The statement is an `INSERT ... ON
                        // CONFLICT DO UPDATE`, so it reports as `INSERT`.
                        PlanKind::DmlResultByOp => HandledWrite::Dml(DmlOutcome {
                            verb: "INSERT",
                            affected: 0,
                        }),
                        PlanKind::Execution
                        | PlanKind::ArraySlice
                        | PlanKind::ReturningRows
                        | PlanKind::SingleDocument
                        | PlanKind::MultiRow => HandledWrite::Opaque,
                    };
                    return Ok(PreDispatchOutcome::Handled(PreDispatchHandled::Write(
                        handled,
                    )));
                }
                PreDispatchResult::Proceed {
                    mutated_fields: Some(fields),
                } => {
                    crate::control::trigger::dml_hook::patch_task_with_mutated_fields(
                        &mut task, &fields,
                    );
                }
                PreDispatchResult::Proceed {
                    mutated_fields: None,
                } => {}
            }
        }

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
                    use crate::control::server::response_shape::compose::{
                        ShapeOutcome, shape_payload_no_plan,
                    };
                    use crate::control::server::response_shape::redaction::QueryRedaction;
                    // A clone write can carry RETURNING rows, which deliver
                    // stored column values just as a SELECT does.
                    let redaction = QueryRedaction::for_plan(tenant_id, auth, &task.plan);
                    // A clone write's RETURNING list names stored columns
                    // only, never a Control-Plane computed column.
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
                            // Clone write-path DML result (PointUpdate/PointDelete):
                            // no client-requested result formats, so text.
                            let (response, notice) =
                                crate::control::server::pgwire::handler::shape_encode::shaped_query_response(
                                    shaped,
                                    &[],
                                );
                            if let Some(n) = notice {
                                self.sessions.push_notice(session_id, n);
                            }
                            return Ok(PreDispatchOutcome::Handled(PreDispatchHandled::Rows(
                                response,
                            )));
                        }
                        ShapeOutcome::Passthrough => {
                            let handled =
                                match payload_to_dml_outcome(resp.payload.as_ref(), plan_kind)
                                    .map_err(|e| error_to_pg(&e))?
                                {
                                    Some(outcome) => HandledWrite::Dml(outcome),
                                    None => HandledWrite::Opaque,
                                };
                            return Ok(PreDispatchOutcome::Handled(PreDispatchHandled::Write(
                                handled,
                            )));
                        }
                    }
                }
                CloneWriteOutcome::Passthrough => {}
            }
        }

        Ok(PreDispatchOutcome::Proceed(Box::new(PreDispatchProceed {
            task,
            dml_info,
            old_row,
            truncate_restart_collection,
        })))
    }
}
