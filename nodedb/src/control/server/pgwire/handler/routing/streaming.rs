// SPDX-License-Identifier: BUSL-1.1

//! Single-node pgwire streaming fast path for unordered SELECTs.
//!
//! An autocommit, multi-row SELECT compiled to a root-level
//! `Exchange{Gather{as_aggregate:false}}` over a plain unordered scan can stream
//! its rows straight to the client: the coordinator fans the scan to all local
//! cores and builds a lazy `QueryResponse` that pulls row batches as they
//! arrive, instead of materializing and merging the whole result first.

use std::sync::Arc;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::shared::session::SessionId;

use super::super::super::types::error_to_sqlstate;
use super::super::super::types::shape_error_to_pg;
use super::super::core::NodeDbPgHandler;
use super::super::plan::{PlanKind, multirow_payload_to_response};
use super::super::stream_response;
use super::result_shaping::ResultShaping;

pub(super) struct StreamSelectContext<'a> {
    pub(super) identity: &'a AuthenticatedIdentity,
    /// The requester's resolved context — its roles drive column-level
    /// redaction, which is Control-Plane-only and so must be captured here
    /// rather than travel with the plan.
    pub(super) auth: &'a crate::control::security::auth_context::AuthContext,
    pub(super) plan_kind: PlanKind,
    pub(super) session_id: SessionId,
    pub(super) shaping: ResultShaping<'a>,
    pub(super) lease_scope: Arc<crate::control::lease::QueryLeaseScope>,
}

impl NodeDbPgHandler {
    /// Build a streaming `Response` for an eligible autocommit SELECT, or
    /// `Ok(None)` when the task is not streamable (caller falls back to the
    /// normal dispatch path).
    ///
    /// Eligibility (all required):
    ///   - no post-set-op (UNION/INTERSECT/EXCEPT need the full sets),
    ///   - no Control-Plane computed column in the projection
    ///     (`cp_computed` needs per-row session sequence access, which the
    ///     per-batch shaper does not carry),
    ///   - `PlanKind::MultiRow` (the streamed shape is one TEXT column),
    ///   - autocommit (not inside a BEGIN..COMMIT block — in-block reads
    ///     participate in snapshot-isolation read tracking on the normal path),
    ///   - the plan is `Exchange{Gather{as_aggregate:false}}` over a scan for
    ///     which [`PhysicalPlan::is_streamable_unordered_scan`] holds.
    ///
    /// The returned stream is `Send + 'static`: it owns a cloned `Arc<SharedState>`
    /// and the scan plan, borrowing neither `self` nor `task`.
    pub(super) async fn maybe_stream_select(
        &self,
        task: &PhysicalTask,
        context: StreamSelectContext<'_>,
    ) -> PgWireResult<Option<Response>> {
        let StreamSelectContext {
            identity,
            auth,
            plan_kind,
            session_id,
            shaping,
            lease_scope,
        } = context;
        if task.post_set_op != PostSetOp::None
            || shaping
                .projection
                .is_some_and(|s| !s.cp_computed.is_empty())
            || !matches!(plan_kind, PlanKind::MultiRow)
            || self.sessions.transaction_state(session_id)
                == crate::control::server::shared::session::TransactionState::InBlock
        {
            return Ok(None);
        }

        let PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather {
                as_aggregate: false,
            },
        })) = &task.plan
        else {
            return Ok(None);
        };

        if !child.is_streamable_unordered_scan() {
            return Ok(None);
        }

        // Permission was already checked by the caller before this point.
        let limit = child.streamable_scan_limit();
        // Clone the child and owned state so the row stream is `Send + 'static`
        // and does not borrow `self` or `task`.
        let child_plan = (**child).clone();
        let mut child_task = task.clone();
        child_task.plan = child_plan.clone();
        let tenant_id = child_task.tenant_id;
        let state = std::sync::Arc::clone(&self.state);
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
        let checked_child =
            match crate::control::server::shared::clone_write::intercept_and_authorize(
                crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                    state: &state,
                    task: child_task,
                    identity,
                    tenant_id,
                    permissions: &state.permissions,
                    roles: &state.roles,
                    emitter: &emitter,
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
                // The clone-write hook fully handled this write locally — return
                // its shaped response directly rather than opening a stream. A
                // streamable child is a plain unordered scan in every eligible
                // case here (the eligibility gate above only admits `Gather` over
                // `is_streamable_unordered_scan`), so this is unreachable in
                // practice; it is still handled because `Response` can represent
                // it, unlike the native/HTTP streaming siblings.
                crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(resp) => {
                    let payload = resp.payload.as_ref();
                    let response = if payload.is_empty() {
                        Response::Execution(Tag::new("OK"))
                    } else {
                        let redaction = QueryRedaction::for_plan(task.tenant_id, auth, &child_plan);
                        // The gate above admits no Control-Plane computed
                        // column, so no session sequence access is needed.
                        match compose::shape_payload_no_plan(
                            payload,
                            plan_kind,
                            shaping.projection,
                            Some(redaction.ctx(&state.redaction)),
                            None,
                        )
                        .map_err(|e| shape_error_to_pg(&e))?
                        {
                            ShapeOutcome::Rows(shaped) => {
                                let (response, _notice) = crate::control::server::pgwire::handler::shape_encode::shaped_query_response(
                                shaped,
                                shaping.formats,
                            );
                                response
                            }
                            ShapeOutcome::Passthrough => {
                                multirow_payload_to_response(payload).response
                            }
                        }
                    };
                    return Ok(Some(response));
                }
                crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(
                    checked,
                ) => checked,
            };

        // Single-node fans to local cores directly; cluster routes the scan to
        // its owning vShard (local or remote over the L4 QUIC streaming
        // transport) via the gateway and merges per-route streams.
        let stream = if let Some(gw) = state.gateway.get() {
            let ctx = crate::control::gateway::core::QueryContext {
                tenant_id: task.tenant_id,
                trace_id: crate::types::TraceId::ZERO,
                database_id: task.database_id,
                txn_id: None,
            };
            gw.execute_stream(&ctx, checked_child).await
        } else {
            crate::control::server::exchange::gather::gather_all_cores_stream_authorized(
                &state,
                checked_child.into_authorized(),
                crate::types::TraceId::ZERO,
            )
        }
        .map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        // Shape the streamed rows to match the SELECT projection, mirroring
        // the (now-removed) post-hoc reproject seam:
        //   - named columns  -> lazy per-batch shaping + projection
        //   - `SELECT *`      -> materialize, then id-first column union
        //   - anything else   -> raw single-column envelope passthrough
        //
        // Column-level redaction is resolved ONCE here, from the scan plan the
        // stream actually reads, and handed to every batch's shaping call
        // below. Deriving it per batch would let an early batch ship before
        // the policy was consulted.
        let redaction = Some(QueryRedaction::for_plan(task.tenant_id, auth, &child_plan));

        // The streaming fast path bypasses the dispatch loop's own metering
        // call, so without this every pgwire autocommit SELECT that streams
        // would go unbilled — and a token quota over reads would never
        // accumulate anything to enforce against. The guard charges on drop,
        // when the stream finishes or the client disconnects, so what is
        // billed is rows actually written. `None` when metering is disabled.
        let meter_guard = {
            let scope = crate::control::security::request_scope::RequestAuthScope::builder(
                identity,
                state.auth_stores(),
            )
            .with_session_database(Some(task.database_id))
            .build();
            let info =
                crate::control::server::shared::metering::PlanMeteringInfo::extract(&task.plan);
            crate::control::server::shared::metering::DetachedMeterGuard::new(&state, &scope, &info)
        };

        let response = match shaping.projection {
            Some(s) if !s.is_star && !s.columns.is_empty() => {
                stream_response::streaming_shaped_response(
                    stream,
                    limit,
                    s.clone(),
                    shaping.formats,
                    stream_response::StreamResponseContext {
                        lease_scope,
                        redaction,
                        state: std::sync::Arc::clone(&state),
                        meter_guard,
                    },
                )
            }
            Some(s) if s.is_star => {
                stream_response::streaming_star_response(
                    stream,
                    limit,
                    redaction,
                    &state,
                    meter_guard,
                )
                .await
            }
            _ => stream_response::streaming_multirow_response(
                stream,
                limit,
                stream_response::StreamResponseContext {
                    lease_scope,
                    redaction,
                    state: std::sync::Arc::clone(&state),
                    meter_guard,
                },
            ),
        };

        Ok(Some(response))
    }
}
