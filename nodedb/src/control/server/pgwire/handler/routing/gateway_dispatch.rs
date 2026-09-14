// SPDX-License-Identifier: BUSL-1.1

//! Gateway-based dispatch: routes tasks through `Gateway::execute` instead of
//! the old SQL-string `ForwardRequest` forwarding path.
//!
//! Where a task set runs is decided in `placement`; this file carries it out.
//!
//! `dispatch_tasks_via_gateway` replaces `forward_sql`: each task is dispatched
//! via `gateway.execute(ctx, plan)` which ships pre-planned `PhysicalPlan` bytes
//! over QUIC via `ExecuteRequest`, rather than raw SQL text.

use pgwire::api::results::{FieldFormat, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::gateway::GatewayErrorMap;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::types::{TenantId, TraceId};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::types::{numeric_code_to_sqlstate, sqlstate_error};
use super::super::core::NodeDbPgHandler;
use super::super::plan::{PlanKind, multirow_payload_to_response};
use super::super::shape_encode;

/// Meter one gateway-forwarded task, once its response has already shaped
/// successfully — mirrors `calvin_dispatch::meter_calvin_task`, the sibling
/// remote-dispatch door that bypasses `dispatch_task_loop` the same way.
///
/// `rows` is `Some(shaped.rows.len())` when the response was decoded into
/// rows by the shaping step just above the call site, `None` for a
/// `Passthrough` shape (no decoded row count) or an empty-payload `OK` tag
/// (no row payload at all) — `meter_dispatch` charges one unit for `None`,
/// correct for a write or a zero-row read.
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

/// Shape one payload the same way a normal gateway response payload is
/// shaped, and push the result onto `responses`. Shared by the forwarded
/// per-payload loop and the clone-write `Handled` short-circuit, which
/// carries exactly one payload.
fn push_shaped_response(
    responses: &mut Vec<Response>,
    payload: &[u8],
    projection: Option<&OutputSchema>,
    result_formats: &[FieldFormat],
    redaction: &QueryRedaction,
    state: &crate::control::state::SharedState,
) -> PgWireResult<()> {
    if payload.is_empty() {
        responses.push(Response::Execution(Tag::new("OK")));
        return Ok(());
    }
    match compose::shape_payload_no_plan(
        payload,
        PlanKind::MultiRow,
        projection,
        Some(redaction.ctx(&state.redaction)),
    )
    .map_err(|e| sqlstate_error(numeric_code_to_sqlstate(e.code()), e.message()))?
    {
        ShapeOutcome::Rows(shaped) => {
            let (response, notice) = shape_encode::shaped_query_response(shaped, result_formats);
            debug_assert!(
                notice.is_none(),
                "MultiRow gateway response must not carry a NOTICE"
            );
            responses.push(response);
        }
        ShapeOutcome::Passthrough => {
            responses.push(multirow_payload_to_response(payload).response);
        }
    }
    Ok(())
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
    pub(super) async fn dispatch_tasks_via_gateway(
        &self,
        tasks: Vec<PhysicalTask>,
        params: GatewayDispatchParams<'_>,
    ) -> PgWireResult<Vec<Response>> {
        let GatewayDispatchParams {
            identity,
            tenant_id,
            database_id,
            projection,
            result_formats,
            auth,
        } = params;
        // Resolved once for the whole forwarded task set, before the loop.
        let redaction = QueryRedaction::for_plans(tenant_id, auth, tasks.iter().map(|t| &t.plan));
        let gateway = self.state.gateway.get().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "55000".to_owned(),
                "gateway not available".to_owned(),
            )))
        })?;

        let gw_ctx = crate::control::gateway::core::QueryContext {
            tenant_id,
            trace_id: TraceId::generate(),
            database_id,
            txn_id: None,
        };

        let mut responses: Vec<Response> = Vec::with_capacity(tasks.len());
        for task in tasks {
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
                        // never forwarded — so shape its response the same way a
                        // single-payload gateway response would be.
                        let payload = resp.payload.to_vec();
                        push_shaped_response(
                            &mut responses,
                            &payload,
                            projection,
                            result_formats,
                            &redaction,
                            &self.state,
                        )?;
                        meter_gateway_task(
                            &self.state,
                            identity,
                            database_id,
                            &plan_for_metering,
                            None,
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

            if payloads.is_empty() {
                responses.push(Response::Execution(Tag::new("OK")));
                meter_gateway_task(&self.state, identity, database_id, &plan_for_metering, None);
            } else {
                // One task can yield several payloads (e.g. a multi-page
                // scan). Metered once per task below, on the total row count
                // across every payload — never per payload, or a single task
                // would be billed multiple times.
                let mut task_rows: Option<u64> = None;
                for payload in &payloads {
                    match compose::shape_payload_no_plan(
                        payload,
                        PlanKind::MultiRow,
                        projection,
                        Some(redaction.ctx(&self.state.redaction)),
                    )
                    .map_err(|e| sqlstate_error(numeric_code_to_sqlstate(e.code()), e.message()))?
                    {
                        ShapeOutcome::Rows(shaped) => {
                            task_rows = Some(task_rows.unwrap_or(0) + shaped.rows.len() as u64);
                            let (response, notice) =
                                shape_encode::shaped_query_response(shaped, result_formats);
                            // The gateway has no `addr` to route a NOTICE to; the
                            // MultiRow shape never carries one, so assert loudly
                            // rather than silently swallowing.
                            debug_assert!(
                                notice.is_none(),
                                "MultiRow gateway response must not carry a NOTICE"
                            );
                            responses.push(response);
                        }
                        ShapeOutcome::Passthrough => {
                            responses.push(multirow_payload_to_response(payload).response);
                        }
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
        }

        if responses.is_empty() {
            responses.push(Response::Execution(Tag::new("OK")));
        }

        Ok(responses)
    }
}
