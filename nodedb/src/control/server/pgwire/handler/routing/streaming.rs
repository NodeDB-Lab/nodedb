// SPDX-License-Identifier: BUSL-1.1

//! Single-node pgwire streaming fast path for unordered SELECTs.
//!
//! An autocommit, multi-row SELECT compiled to a root-level
//! `Exchange{Gather{as_aggregate:false}}` over a plain unordered scan can stream
//! its rows straight to the client: the coordinator fans the scan to all local
//! cores and builds a lazy `QueryResponse` that pulls row batches as they
//! arrive, instead of materializing and merging the whole result first.

use std::sync::Arc;

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::bridge::envelope::describe_row_context;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::streamable::{StreamableScan, open_streamable_scan};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::shared::session::SessionId;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;
use super::super::plan::PlanKind;
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
    ///   - [`open_streamable_scan`] clears the plan: an
    ///     `Exchange{Gather{as_aggregate:false}}` over a streamable unordered
    ///     scan that reads through no unmaterialized clone.
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

        // The row stream owns a cloned `Arc<SharedState>` and the scan plan,
        // so it is `Send + 'static` and borrows neither `self` nor `task`.
        let state = std::sync::Arc::clone(&self.state);
        let Some(StreamableScan {
            checked: checked_child,
            child_plan,
            limit,
        }) = open_streamable_scan(&state, identity, task)
            .await
            .map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?
        else {
            return Ok(None);
        };

        // The gateway routes the scan to its owning vShard (local or remote
        // over the L4 QUIC streaming transport) and merges per-route streams.
        let ctx = crate::control::gateway::core::QueryContext {
            tenant_id: task.tenant_id,
            trace_id: crate::types::TraceId::ZERO,
            database_id: task.database_id,
            txn_id: None,
            linearizable: self.sessions.read_consistency(session_id).requires_leader(),
        };
        let stream = match state.installed_gateway() {
            Ok(gateway) => gateway.execute_stream(&ctx, checked_child).await,
            Err(error) => Err(error),
        }
        .map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        // Shape the streamed rows to match the SELECT projection:
        //   - named columns  -> lazy per-batch shaping + projection
        //   - `SELECT *`      -> materialize, then id-first column union
        //   - anything else   -> raw single-column envelope passthrough
        //
        // Column-level redaction is resolved ONCE here, from the scan plan the
        // stream actually reads, and handed to every batch's shaping call
        // below. Deriving it per batch will let an early batch ship before
        // the policy was consulted.
        let redaction = Some(QueryRedaction::for_plan(
            task.tenant_id,
            task.database_id,
            auth,
            &child_plan,
        ));

        // The streaming fast path bypasses the dispatch loop's own metering
        // call, so without this every pgwire autocommit SELECT that streams
        // will go unbilled — and a token quota over reads will never
        // accumulate anything to enforce against. The guard charges on drop,
        // when the stream finishes or the client disconnects, so what is
        // billed is rows actually written. `None` when metering is disabled.
        let meter_guard = {
            let scope = crate::control::security::request_scope::RequestAuthScope::builder(
                identity,
                state.auth_stores(),
            )
            .with_session_database(Some(task.database_id))
            .build()
            .map_err(|e| crate::control::server::pgwire::types::error_map::error_to_pg(&e))?;
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
                        row_context: describe_row_context(&child_plan),
                    },
                )
            }
            Some(s) if s.is_star => {
                stream_response::streaming_star_response(
                    stream,
                    limit,
                    s,
                    describe_row_context(&child_plan),
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
                    row_context: describe_row_context(&child_plan),
                },
            ),
        };

        Ok(Some(response))
    }
}
