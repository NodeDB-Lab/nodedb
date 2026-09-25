// SPDX-License-Identifier: BUSL-1.1

//! Single-task dispatch for direct Data Plane operations, routed through the
//! protocol-neutral in-transaction staging gate. Split out of `direct_ops.rs`
//! to keep that file under the size limit; behavior is unchanged.

use nodedb_types::protocol::NativeResponse;

use crate::bridge::envelope::{Payload, Response, Status};
use crate::control::server::response_shape::types::staged_dml_outcome;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::session::staging_gate::{
    InTxnRoute, StagedTagKind, StagingGateError, route_in_tx_write,
};
use crate::types::{Lsn, RequestId};

use super::raw_dispatch::dispatch_authorized_single_task;
use super::response::data_plane_response_to_native;
use super::{
    DispatchCtx, apply_dml_outcome, error_code_to_native, error_to_native,
    error_to_native_with_sqlstate,
};

/// Dispatch one plan via the gateway (when wired) or the local SPSC path,
/// converting the Data-Plane response into a `NativeResponse`.
///
/// This is the exact single-plan dispatch the direct-op handler used before
/// implicit-edge extraction; it is factored out so the no-edge fast path and
/// the single-shard edge loop share one code path.
///
/// Routes through the same protocol-neutral in-transaction staging gate
/// (`route_in_tx_write`) the SQL-planned dispatch loops (`sql_loop.rs`,
/// pgwire's `execute_dml_hooks.rs`) already use. Outside a transaction block
/// the task comes back unchanged (`InTxnRoute::Read`, or `Autocommit` for a
/// write), and the gateway gives a write its durable route. Inside a
/// transaction block, a stageable write (e.g. `KvBatchPut`)
/// is applied to the per-transaction overlay at statement time instead of
/// hitting durable storage directly. Otherwise a native direct-op write
/// inside `BEGIN...COMMIT` would commit immediately and survive `ROLLBACK`,
/// an atomicity gap. A non-stageable write is buffered for COMMIT-time
/// replay, matching the SQL path's deferral for the same plan shapes.
pub(super) async fn dispatch_single_task(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    authorized: crate::control::server::shared::authorization::AuthorizedTask,
) -> NativeResponse {
    // Authorization must precede the staging decision. Non-stageable writes
    // are buffered without invoking the stage-dispatch closure, so authorizing
    // only inside that closure would let an ungranted task reach trusted
    // COMMIT replay. Consume the expanded-set capability here so every branch
    // below originates from the final authorization decision.
    let task = authorized.into_staging_task();

    // Cloned before `route_in_tx_write` consumes `task`, so a staged write
    // whose outcome carries a computed-value payload (KV `Incr` / `Cas` /
    // `GetSet`, see `StagedTagKind::RawPayload`) can be shaped into the
    // response the same way the non-staged branch below shapes it.
    let plan_for_staged_response = task.plan.clone();

    // Only when metering is enabled — the default is disabled, so this is a
    // no-op on the hot path for every deployment that hasn't turned it on.
    // Covers the `Read` / `Autocommit` dispatch below (reads, and writes that
    // apply now). `Staged` meters itself inside
    // `staging_gate::stage_write` — the single choke-point every `Staged`
    // route (this file, `sql_loop.rs`, the expander's per-op staging, and
    // pgwire's `execute_dml_hooks.rs`) dispatches through, so it is metered
    // there once rather than duplicated in every caller's closure. `Buffered`
    // performs no dispatch at all here — it is metered at COMMIT replay
    // instead (`session::commit::run_commit`).
    let plan_metering_info = ctx
        .state
        .metering_config
        .enabled
        .then(|| PlanMeteringInfo::extract(&plan_for_staged_response));

    // A spent hard quota refuses the task before it runs. The charging call
    // below is on the success path by design and so can never refuse
    // anything itself.
    if let Some(info) = &plan_metering_info
        && let Err(e) = admit_quota_for_dispatch(ctx.state, &ctx.scope, info)
    {
        return error_to_native_with_sqlstate(seq, "53400", &e);
    }

    let task = match route_in_tx_write(
        ctx.state,
        ctx.sessions,
        ctx.peer_addr.into(),
        task,
        |stage_task| {
            dispatch_authorized_single_task(
                ctx,
                stage_task.tenant_id,
                stage_task.vshard_id,
                stage_task.plan,
                stage_task.txn_id,
            )
        },
    )
    .await
    {
        // A write here reaches the gateway, which proposes it through Raft or
        // appends its redo record in the funnel.
        Ok(InTxnRoute::Read(routed_task) | InTxnRoute::Autocommit(routed_task)) => *routed_task,
        // A buffered write applies at COMMIT: no count and no verb yet.
        Ok(InTxnRoute::Buffered) => return NativeResponse::ok(seq),
        Ok(InTxnRoute::Staged(outcome)) => {
            // The staging gate decided the verb and counted the rows it
            // applied to the overlay; only a computed-value payload is
            // forwarded as-is.
            if !matches!(outcome.kind, StagedTagKind::RawPayload) {
                let mut r = NativeResponse::ok(seq);
                apply_dml_outcome(&mut r, staged_dml_outcome(outcome.kind, outcome.affected));
                return r;
            }
            let synthetic = Response {
                request_id: RequestId::new(0),
                status: Status::Ok,
                attempt: 0,
                partial: false,
                payload: Payload::from_vec(outcome.payload),
                watermark_lsn: Lsn::new(0),
                error_code: None,
                read_set_valid: None,
                read_version_lsn: crate::types::Lsn::ZERO,
                write_set: Vec::new(),
            };
            return data_plane_response_to_native(ctx, seq, &plan_for_staged_response, &synthetic);
        }
        Err(StagingGateError::Dispatch(e)) => return error_to_native(seq, &e),
        Err(StagingGateError::Rejected { code }) => {
            return error_code_to_native(seq, code.as_ref());
        }
    };

    // `ClusterArray` plans are handled entirely on the Control Plane by the
    // `ArrayCoordinator` — they must never reach the SPSC bridge or the
    // trigger/DML machinery. A write reshapes into per-shard `ArrayOp` tasks
    // inside `route_in_tx_write` above and never reaches here as
    // `ClusterArray`; only reads (`Slice`/`Agg`) and autocommit `Put`/`Delete`
    // arrive as this plan by the time the route resolves to `Read`.
    // Intercepted here, before `dispatch_authorized_single_task` would
    // otherwise route it toward the Data Plane. No SQL output schema exists
    // on this direct-op path, so no projection narrows the shaped rows.
    // Metering is not applied here, matching pgwire's `ClusterArray`
    // short-circuit, which also does not meter this path.
    if matches!(
        task.plan,
        crate::bridge::envelope::PhysicalPlan::ClusterArray(_)
    ) {
        let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
            Ok(a) => a,
            Err(e) => return error_to_native(seq, &e),
        };
        return match super::cluster_array::dispatch_cluster_array_task(ctx, authorized, None).await
        {
            Ok(super::cluster_array::ClusterArrayOutcome::Rows {
                columns,
                rows,
                notice,
            }) => {
                let mut r = NativeResponse::ok(seq);
                if !columns.is_empty() {
                    r.columns = Some(columns);
                }
                r.rows = Some(rows);
                r.warnings = notice.into_iter().collect();
                r
            }
            Ok(super::cluster_array::ClusterArrayOutcome::Affected(outcome)) => {
                let mut r = NativeResponse::ok(seq);
                apply_dml_outcome(&mut r, outcome);
                r
            }
            Err(e) => error_to_native(seq, &e),
        };
    }

    let plan_for_response = task.plan.clone();
    let task_vshard = task.vshard_id;
    match dispatch_authorized_single_task(
        ctx,
        task.tenant_id,
        task.vshard_id,
        task.plan,
        task.txn_id,
    )
    .await
    {
        Ok(resp) => {
            // Track direct-op reads, including NotFound phantom observations,
            // identically to native SQL and pgwire conflict detection.
            let records_read = resp.status == Status::Ok
                || resp.error_code.as_deref()
                    == Some(&crate::bridge::envelope::ErrorCode::NotFound);
            if records_read
                && ctx.sessions.transaction_state(ctx.peer_addr)
                    == crate::control::server::shared::session::TransactionState::InBlock
            {
                crate::control::server::shared::session::record_reads_for_response(
                    ctx.state,
                    ctx.sessions,
                    ctx.peer_addr.into(),
                    ctx.tenant_id(),
                    crate::control::server::shared::session::ResponseReads {
                        plan: &plan_for_response,
                        watermarks: &[(task_vshard, resp.watermark_lsn)],
                        read_version_lsn: resp.read_version_lsn,
                        found: resp.status == Status::Ok,
                        distributed_reads: &[],
                        read_lsn_vshard: task_vshard,
                    },
                )
                .await;
            }
            let native = data_plane_response_to_native(ctx, seq, &plan_for_response, &resp);
            // Metered here, once this task's real dispatch has already
            // succeeded — covers the `Read` route (autocommit writes/reads,
            // in-transaction reads); see `plan_metering_info`'s doc comment
            // above for the `Staged`/`Buffered` routes.
            if native.status != nodedb_types::protocol::ResponseStatus::Error
                && let Some(info) = &plan_metering_info
            {
                let rows = native
                    .rows
                    .as_ref()
                    .map(|rows| rows.len() as u64)
                    .or(native.rows_affected);
                meter_dispatch(ctx.state, &ctx.scope, info, rows);
            }
            native
        }
        Err(e) => error_to_native(seq, &e),
    }
}
