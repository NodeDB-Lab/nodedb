// SPDX-License-Identifier: BUSL-1.1

//! Pre-dispatch hook interception for the `dispatch_task_loop` write path:
//! BEFORE/INSTEAD OF trigger firing (with OLD-row fetch and probe-driven
//! event reclassification), truncate `restart_identity` extraction, and
//! clone CoW write-path interception. Split out of `execute.rs` to keep
//! that file under the file-size limit; behavior is unchanged — this is
//! the same code that used to run inline in the per-task dispatch loop.

use std::collections::HashMap;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::trigger::dml_hook::DmlWriteInfo;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;
use super::super::plan::PlanKind;

/// Outcome of routing a single task through the in-transaction staging gate.
pub(super) enum TxnRouteOutcome {
    /// Not staged/buffered: caller proceeds to normal dispatch with the
    /// (possibly `txn_id`-stamped) task.
    Proceed(Box<PhysicalTask>),
    /// Fully handled (buffered "OK", or a staged write's real command tag).
    /// Caller pushes this response and continues the loop.
    Handled(Response),
}

impl NodeDbPgHandler {
    /// Route a single task through the protocol-neutral in-transaction
    /// staging gate (`shared::session::staging_gate`), translating its
    /// outcome into this file's `PgWireResult`. A constraint violation on a
    /// staged write surfaces here as the pgwire error, matching the
    /// pre-refactor `stage_in_tx_point_write` behavior exactly.
    pub(super) async fn route_task_in_txn(
        &self,
        addr: &std::net::SocketAddr,
        identity: &AuthenticatedIdentity,
        task: PhysicalTask,
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
        let routed =
            match route_in_tx_expander(&self.state, &self.sessions, addr, task, |stage_task| {
                self.dispatch_task(stage_task, user_id.clone(), Some(identity))
            })
            .await
            {
                Ok(ExpanderOutcome::Handled(route)) => Ok(route),
                Ok(ExpanderOutcome::Passthrough(task)) => {
                    route_in_tx_write(&self.sessions, addr, *task, |stage_task| {
                        self.dispatch_task(stage_task, user_id.clone(), Some(identity))
                    })
                    .await
                }
                Err(e) => Err(e),
            };

        match routed {
            Ok(InTxnRoute::Read(routed_task)) => Ok(TxnRouteOutcome::Proceed(routed_task)),
            Ok(InTxnRoute::Buffered) => Ok(TxnRouteOutcome::Handled(Response::Execution(
                Tag::new("OK"),
            ))),
            Ok(InTxnRoute::Staged(outcome)) => {
                let tag = super::super::plan::tag_from_staged(outcome.kind, outcome.affected);
                Ok(TxnRouteOutcome::Handled(Response::Execution(tag)))
            }
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

/// Outcome of running the pre-dispatch hooks for a single task.
pub(super) enum PreDispatchOutcome {
    /// The task was fully handled (trigger short-circuit, or clone write
    /// interception). Caller pushes this response and continues the loop.
    Handled(Response),
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

impl NodeDbPgHandler {
    /// Run trigger interception and clone write-path interception for a
    /// single write task, before it reaches normal dispatch.
    pub(super) async fn run_pre_dispatch_hooks(
        &self,
        identity: &AuthenticatedIdentity,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        plan_kind: PlanKind,
        mut task: PhysicalTask,
    ) -> PgWireResult<PreDispatchOutcome> {
        // --- Trigger interception for DML writes ---
        let mut dml_info = crate::control::trigger::dml_hook::classify_dml_write(&task.plan);

        let database_id = self
            .sessions
            .get_current_database(addr)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);

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
            let row = crate::control::trigger::dml_hook::fetch_old_row(
                &self.state,
                database_id,
                tenant_id,
                &info.collection,
                doc_id,
            )
            .await;
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
                    return Ok(PreDispatchOutcome::Handled(Response::Execution(Tag::new(
                        "OK",
                    ))));
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
        let truncate_restart_collection =
            if let nodedb_physical::physical_plan::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::Truncate {
                    collection,
                    restart_identity: true,
                },
            ) = &task.plan
            {
                Some(collection.clone())
            } else {
                None
            };

        // --- Clone write-path interception ---
        // For PointUpdate / PointDelete on Shadowed/Materializing clones,
        // apply copy-up or tombstone before (or instead of) normal dispatch.
        // Non-cloned collections and Materialized clones short-circuit here.
        {
            use super::clone_write_dispatch::CloneWriteOutcome;
            match self
                .maybe_intercept_clone_write(&task, identity, tenant_id)
                .await?
            {
                CloneWriteOutcome::Handled(resp) => {
                    use crate::control::server::response_shape::compose::{
                        ShapeOutcome, shape_payload_no_plan,
                    };
                    match shape_payload_no_plan(resp.payload.as_ref(), plan_kind, None) {
                        ShapeOutcome::Rows(shaped) => {
                            // Clone write-path DML result (PointUpdate/PointDelete):
                            // no client-requested result formats, so text.
                            let (response, notice) =
                                crate::control::server::pgwire::handler::shape_encode::shaped_query_response(
                                    shaped,
                                    &[],
                                );
                            if let Some(n) = notice {
                                self.sessions.push_notice(addr, n);
                            }
                            return Ok(PreDispatchOutcome::Handled(response));
                        }
                        ShapeOutcome::Passthrough => {
                            let shaped =
                                crate::control::server::pgwire::handler::plan::payload_to_response(
                                    resp.payload.as_ref(),
                                    plan_kind,
                                )?;
                            if let Some(notice) = shaped.notice {
                                self.sessions.push_notice(addr, notice);
                            }
                            return Ok(PreDispatchOutcome::Handled(shaped.response));
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
