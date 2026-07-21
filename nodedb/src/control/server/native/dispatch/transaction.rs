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

use crate::bridge::envelope::{ErrorCode, Response};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::{
    AbortReason, CommitOutcome, TxnDataPlane, commit, lifecycle,
};
use crate::control::state::SharedState;
use crate::types::Lsn;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::dispatch_utils;
use super::DispatchCtx;

/// Native Data-Plane dispatch seam for the neutral transaction orchestrator.
///
/// Always dispatches through the direct SPSC write path using the task's
/// pre-classified `vshard_id`, mirroring pgwire's `dispatch_task_no_wal`.
/// The gateway must NOT be used here: commit-time tasks carry `MetaOp` plans
/// (`ResolveTxn`, `TransactionBatch`) with no named collection, so the
/// gateway's router cannot derive a route for them and falls back to
/// vShard 0 — durably applying the commit batch on the wrong core (#193).
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
                    // Batch COMMIT record, not per-task WAL append — see
                    // `dispatch_task_no_wal`'s equivalent limitation.
                    resolved_now_ms: None,
                },
            )
            .await
        })
    }
}

pub(crate) fn handle_begin(ctx: &DispatchCtx<'_>, seq: u64) -> NativeResponse {
    // OpCode::Begin can arrive before any SQL statement has created the
    // session. `SessionStore::begin` no-ops when the peer has no entry, so
    // ensure the session exists first (SQL "BEGIN" already does this in
    // `sql.rs` before calling here).
    ctx.sessions.ensure_session(*ctx.peer_addr);
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
