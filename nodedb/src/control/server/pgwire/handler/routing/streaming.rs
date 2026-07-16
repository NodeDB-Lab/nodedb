// SPDX-License-Identifier: BUSL-1.1

//! Single-node pgwire streaming fast path for unordered SELECTs.
//!
//! An autocommit, multi-row SELECT compiled to a root-level
//! `Exchange{Gather{as_aggregate:false}}` over a plain unordered scan can stream
//! its rows straight to the client: the coordinator fans the scan to all local
//! cores and builds a lazy `QueryResponse` that pulls row batches as they
//! arrive, instead of materializing and merging the whole result first.

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::control::server::response_shape::schema::OutputSchema;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;
use super::super::plan::PlanKind;
use super::super::stream_response;

impl NodeDbPgHandler {
    /// Build a streaming `Response` for an eligible autocommit SELECT, or
    /// `Ok(None)` when the task is not streamable (caller falls back to the
    /// normal dispatch path).
    ///
    /// Eligibility (all required):
    ///   - no post-set-op (UNION/INTERSECT/EXCEPT need the full sets),
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
        plan_kind: PlanKind,
        post_set_op: PostSetOp,
        addr: &std::net::SocketAddr,
        projection: Option<&OutputSchema>,
        result_formats: &[FieldFormat],
    ) -> PgWireResult<Option<Response>> {
        if post_set_op != PostSetOp::None
            || !matches!(plan_kind, PlanKind::MultiRow)
            || self.sessions.transaction_state(addr)
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
        let state = std::sync::Arc::clone(&self.state);

        // Single-node fans to local cores directly; cluster routes the scan to
        // its owning vShard (local or remote over the L4 QUIC streaming
        // transport) via the gateway and merges per-route streams.
        let stream = if let Some(gw) = state.gateway.as_ref() {
            let ctx = crate::control::gateway::core::QueryContext {
                tenant_id: task.tenant_id,
                trace_id: crate::types::TraceId::ZERO,
                database_id: task.database_id,
                txn_id: None,
            };
            gw.execute_stream(&ctx, child_plan).await
        } else {
            crate::control::server::exchange::gather::gather_all_cores_stream(
                &state,
                task.tenant_id,
                task.database_id,
                child_plan,
                crate::types::TraceId::ZERO,
                task.txn_id,
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
        let response = match projection {
            Some(s) if !s.is_star && !s.columns.is_empty() => {
                stream_response::streaming_shaped_response(stream, limit, s.clone(), result_formats)
            }
            Some(s) if s.is_star => stream_response::streaming_star_response(stream, limit).await,
            _ => stream_response::streaming_multirow_response(stream, limit),
        };

        Ok(Some(response))
    }
}
