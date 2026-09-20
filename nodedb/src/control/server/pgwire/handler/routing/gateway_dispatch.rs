// SPDX-License-Identifier: BUSL-1.1

//! Gateway-based dispatch: routes tasks through `Gateway::execute`, which
//! ships each pre-planned `PhysicalPlan` to the leader that owns it over QUIC
//! via `ExecuteRequest`, rather than raw SQL text.
//!
//! Where a task set runs is decided in `placement`; this file carries it out.
//! Shaping the forwarded payloads into the statement's one answer lives in
//! `gateway_fold`.

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::gateway::GatewayErrorMap;
use crate::control::planner::calvin::write_class::{
    plan_counts_toward_statement_tag, plans_have_user_write,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::session::SessionId;
use crate::types::{TenantId, TraceId};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;
use super::super::plan::describe_plan;
use super::gateway_fold::{GatewayFold, GatewayShaping};

/// Meter one gateway-forwarded task, once its response has already shaped
/// successfully — mirrors `calvin_dispatch::meter_calvin_task`, the sibling
/// remote-dispatch door that bypasses `dispatch_task_loop` the same way.
///
/// `rows` is the row count the shaping step decoded, `None` for a
/// passthrough payload (no decoded row count) — `meter_dispatch` charges one
/// unit for `None`, correct for a write or a zero-row read.
fn meter_gateway_task(
    state: &crate::control::state::SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::id::DatabaseId,
    plan: &crate::bridge::envelope::PhysicalPlan,
    rows: Option<u64>,
) {
    if !state.metering_config.enabled {
        return;
    }
    let info = PlanMeteringInfo::extract(plan);
    let scope = RequestAuthScope::builder(identity, state.auth_stores())
        .with_session_database(Some(database_id))
        .build();
    meter_dispatch(state, &scope, &info, rows);
}

/// Everything a gateway dispatch needs besides the tasks themselves.
///
/// These travel together because they all describe the same request: the
/// principal it runs as, the tenant and database it is scoped to, and how its
/// rows are shaped back to the client.
pub(super) struct GatewayDispatchParams<'a> {
    pub(super) identity: &'a AuthenticatedIdentity,
    pub(super) tenant_id: TenantId,
    pub(super) database_id: nodedb_types::id::DatabaseId,
    /// Receives a shaped row set's NOTICE.
    pub(super) session_id: SessionId,
    pub(super) projection: Option<&'a OutputSchema>,
    pub(super) result_formats: &'a [FieldFormat],
    /// The requester's resolved context; its roles drive column-level
    /// redaction of the forwarded rows.
    pub(super) auth: &'a crate::control::security::auth_context::AuthContext,
}

impl NodeDbPgHandler {
    /// Execute all tasks via the gateway. Each task's plan is dispatched
    /// through `gateway.execute()` which ships the pre-planned physical
    /// plan to the target node via `ExecuteRequest`.
    ///
    /// Clone-check runs here, per task, immediately before forwarding — the
    /// remote leader's own receive side never re-runs it (see
    /// `exec_receiver::executor`), so the sending node is the only place the
    /// copy-up can happen for a task headed off-node.
    ///
    /// The statement answers exactly as a local dispatch does: rows as they
    /// arrive, `RETURNING` rows as one result set, and one command tag
    /// folded over every write task.
    pub(super) async fn dispatch_tasks_via_gateway(
        &self,
        tasks: Vec<PhysicalTask>,
        params: GatewayDispatchParams<'_>,
    ) -> PgWireResult<Vec<Response>> {
        let GatewayDispatchParams {
            identity,
            tenant_id,
            database_id,
            session_id,
            projection,
            result_formats,
            auth,
        } = params;
        // Resolved once for the whole forwarded task set, before the loop.
        let redaction = QueryRedaction::for_plans(tenant_id, auth, tasks.iter().map(|t| &t.plan));
        let shaping = GatewayShaping {
            projection,
            result_formats,
            redaction: &redaction,
            session_id,
        };
        let gateway = self.state.gateway.get().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "55000".to_owned(),
                "gateway not available".to_owned(),
            )))
        })?;

        // Forwarding is autocommit only: an in-block statement never reaches
        // here (`maybe_dispatch_tasks_via_gateway`), so no transaction id.
        let gw_ctx = crate::control::gateway::core::QueryContext {
            tenant_id,
            trace_id: TraceId::generate(),
            database_id,
            txn_id: None,
        };

        // A derived implicit-edge write beside the user's own never answers
        // the statement, exactly as Calvin's deposit rule has it.
        let has_user_write = plans_have_user_write(tasks.iter().map(|t| &t.plan));
        let mut fold = GatewayFold::with_capacity(tasks.len());
        for task in tasks {
            let plan_kind = describe_plan(&task.plan);
            let counts_toward_tag = plan_counts_toward_statement_tag(&task.plan, has_user_write);
            let plan_for_metering = task.plan.clone();
            let emitter = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
                &self.state.audit,
            ));
            let checked =
                match crate::control::server::shared::clone_write::intercept_and_authorize(
                    crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                        state: &self.state,
                        task,
                        identity,
                        tenant_id,
                        permissions: &self.state.permissions,
                        roles: &self.state.roles,
                        emitter: &emitter,
                    },
                )
                .await
                .map_err(|e| {
                    let (severity, code, message) =
                        super::super::super::types::error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })? {
                    crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(
                        resp,
                    ) => {
                        // The clone-write hook fully handled this task locally —
                        // never forwarded — so its one payload folds exactly
                        // like a forwarded one.
                        let rows = self.fold_gateway_payload(
                            &mut fold,
                            resp.payload.as_ref(),
                            plan_kind,
                            counts_toward_tag,
                            &shaping,
                        )?;
                        meter_gateway_task(
                            &self.state,
                            identity,
                            database_id,
                            &plan_for_metering,
                            rows,
                        );
                        continue;
                    }
                    crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(
                        checked,
                    ) => checked,
                };
            let payloads = gateway.execute(&gw_ctx, checked).await.map_err(|e| {
                let (code, msg) = GatewayErrorMap::to_pgwire(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    code.to_owned(),
                    msg,
                )))
            })?;

            // One task can yield several payloads (e.g. a multi-page scan).
            // Metered once per task, on the total row count across every
            // payload — never per payload, which bills a single task
            // multiple times. A task with no payload at all folds one empty
            // payload: an opaque execution folds as such, a count-bearing
            // write is refused by the count reader.
            let mut task_rows: Option<u64> = None;
            if payloads.is_empty() {
                task_rows = self.fold_gateway_payload(
                    &mut fold,
                    &[],
                    plan_kind,
                    counts_toward_tag,
                    &shaping,
                )?;
            }
            for payload in &payloads {
                if let Some(rows) = self.fold_gateway_payload(
                    &mut fold,
                    payload,
                    plan_kind,
                    counts_toward_tag,
                    &shaping,
                )? {
                    task_rows = Some(task_rows.unwrap_or(0) + rows);
                }
            }
            meter_gateway_task(
                &self.state,
                identity,
                database_id,
                &plan_for_metering,
                task_rows,
            );
        }

        Ok(self.finish_gateway_fold(fold, &shaping))
    }
}
