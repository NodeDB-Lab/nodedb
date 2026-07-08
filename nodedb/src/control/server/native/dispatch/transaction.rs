// SPDX-License-Identifier: BUSL-1.1

//! Transaction control adapters for the native protocol: BEGIN, COMMIT,
//! ROLLBACK — thin shims over the protocol-neutral orchestrator in
//! `control/server/shared/session/`.
//!
//! Driving the neutral core means native GAINS everything pgwire already did:
//! Calvin multi-shard COMMIT, read-your-own-write SI exclusion, deferred offset
//! / GAP_FREE / DDL / notify flush on COMMIT, and DDL-buffer + GAP_FREE + cursor
//! + notify cleanup on ROLLBACK.

use std::future::Future;
use std::pin::Pin;

use nodedb_types::TraceId;
use nodedb_types::protocol::NativeResponse;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::control::gateway::core::QueryContext as GatewayQueryContext;
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::{
    AbortReason, CommitOutcome, TxnDataPlane, commit, lifecycle,
};
use crate::control::state::SharedState;
use crate::types::{Lsn, RequestId};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::dispatch_utils;
use super::DispatchCtx;

/// Native Data-Plane dispatch seam for the neutral transaction orchestrator.
///
/// Routes a task through the cluster gateway when one is configured, otherwise
/// through the direct SPSC dispatch path — the exact branch native COMMIT used
/// before extraction. The gateway path synthesizes an `Ok` [`Response`] on
/// success (carrying the first vShard payload so overlay-marker meta-ops still
/// decode), and surfaces gateway errors as a Rust `Err`.
pub(crate) struct NativeTxnDp<'a> {
    pub(crate) state: &'a SharedState,
}

impl TxnDataPlane for NativeTxnDp<'_> {
    fn dispatch_no_wal<'a>(
        &'a self,
        task: PhysicalTask,
        wal_lsn: Option<Lsn>,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Response>> + Send + 'a>> {
        let state = self.state;
        Box::pin(async move {
            match state.gateway.as_ref() {
                Some(gw) => {
                    let gw_ctx = GatewayQueryContext {
                        tenant_id: task.tenant_id,
                        trace_id: TraceId::generate(),
                        database_id: task.database_id,
                        // Carries the transaction's id so staging-overlay
                        // meta-ops (StageWrite / DropTxnOverlay) reach remote
                        // shards correctly keyed.
                        txn_id: task.txn_id,
                    };
                    let payloads = gw.execute(&gw_ctx, task.plan).await?;
                    Ok(Response {
                        request_id: RequestId::new(0),
                        status: Status::Ok,
                        attempt: 0,
                        partial: false,
                        payload: Payload::from_vec(payloads.into_iter().next().unwrap_or_default()),
                        watermark_lsn: Lsn::new(0),
                        error_code: None,
                    })
                }
                None => {
                    dispatch_utils::dispatch_write_to_data_plane(
                        state,
                        dispatch_utils::WriteDispatch {
                            tenant_id: task.tenant_id,
                            database_id: task.database_id,
                            vshard_id: task.vshard_id,
                            plan: task.plan,
                            trace_id: TraceId::ZERO,
                            event_source: crate::event::EventSource::User,
                            txn_id: None,
                            wal_lsn,
                        },
                    )
                    .await
                }
            }
        })
    }
}

pub(crate) fn handle_begin(ctx: &DispatchCtx<'_>, seq: u64) -> NativeResponse {
    match lifecycle::run_begin(ctx.sessions, ctx.peer_addr, ctx.state) {
        Ok(()) => NativeResponse::status_row(seq, "BEGIN"),
        Err(e) => {
            let message = match &e {
                crate::Error::BadRequest { detail } => detail.clone(),
                other => other.to_string(),
            };
            NativeResponse::error(seq, "25P02", message)
        }
    }
}

pub(crate) async fn handle_commit(ctx: &DispatchCtx<'_>, seq: u64) -> NativeResponse {
    let dp = NativeTxnDp { state: ctx.state };
    match commit::run_commit(ctx.sessions, ctx.peer_addr, ctx.identity, ctx.state, &dp).await {
        CommitOutcome::Committed => NativeResponse::status_row(seq, "COMMIT"),
        CommitOutcome::Aborted { reason } => commit_abort_to_native(seq, &reason),
    }
}

pub(crate) async fn handle_rollback(ctx: &DispatchCtx<'_>, seq: u64) -> NativeResponse {
    let dp = NativeTxnDp { state: ctx.state };
    lifecycle::run_rollback(ctx.sessions, ctx.peer_addr, ctx.identity, ctx.state, &dp).await;
    NativeResponse::status_row(seq, "ROLLBACK")
}

/// Map a neutral commit abort reason to the native error frame native emitted
/// before extraction (batch/dispatch failures collapse to `40001`, batch
/// rejections carry the Data-Plane SQLSTATE).
fn commit_abort_to_native(seq: u64, reason: &AbortReason) -> NativeResponse {
    let (code, message): (&'static str, String) = match reason {
        AbortReason::Serialization => (
            "40001",
            "could not serialize access due to concurrent update".to_owned(),
        ),
        AbortReason::NoTransaction => (
            "25000",
            "current transaction is aborted, commands ignored until end of transaction block"
                .to_owned(),
        ),
        AbortReason::BatchRejected { code } => {
            let code = code.clone().unwrap_or(ErrorCode::RejectedPrevalidation {
                reason: "transaction commit failed".to_owned(),
            });
            let (_severity, sqlstate, message) = error_code_to_sqlstate(&code);
            (sqlstate, format!("transaction commit failed: {message}"))
        }
        AbortReason::CalvinCancelled => (
            "57014",
            "Calvin coordinator cancelled (deadline exceeded)".to_owned(),
        ),
        AbortReason::CalvinTimeout => {
            ("57014", "timed out waiting for Calvin sequencer".to_owned())
        }
        AbortReason::Dispatch(e) => ("40001", format!("transaction commit failed: {e}")),
        AbortReason::DdlPropose(e) => ("XX000", format!("{e}")),
    };
    NativeResponse::error(seq, code, message)
}
