// SPDX-License-Identifier: BUSL-1.1

//! Per-task dispatch loop for the DataFusion-planned SQL path. Split out of
//! `sql.rs` to keep that file under the file-size limit, with no behavior
//! change from running inline in `execute_planned`.
//!
//! The single-task dispatch primitive it calls lives in `sql_dispatch_task.rs`.

use std::sync::Arc;

use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use crate::bridge::envelope::Status;
use crate::control::sequence::SessionSequenceAccess;
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{
    DmlOutcome, FoldedTag, PlanKind, StatementTag, describe_plan, payload_to_dml_outcome,
    staged_dml_outcome,
};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::session::expander_stage::{
    ExpanderOutcome, route_in_tx_expander,
};
use crate::control::server::shared::session::staging_gate::{
    InTxnRoute, StagedTagKind, StagingGateError, route_in_tx_write,
};
use crate::types::DatabaseId;
use nodedb_physical::physical_task::PhysicalTask;

use super::sql_dispatch_task::dispatch_task;
use super::streaming::SqlOutcome;
use super::{
    DispatchCtx, apply_dml_outcome, dml_fold_error_to_native, error_code_to_native,
    error_response_to_native, error_to_native, error_to_native_with_sqlstate,
    shape_error_to_native, to_native_columns_rows,
};
use crate::control::server::native::sqlstate_code::sqlstate_error;

/// Wrap a materialized response as a non-streaming [`SqlOutcome`].
#[inline]
fn resp(r: NativeResponse) -> SqlOutcome {
    SqlOutcome::Response(Box::new(r))
}

/// Fold one task's count-bearing outcome into the statement's tag, rendering
/// a verb mismatch as the native error frame.
fn fold_dml(
    tag: &mut StatementTag,
    seq: u64,
    outcome: DmlOutcome,
) -> Result<(), Box<NativeResponse>> {
    tag.fold(outcome)
        .map_err(|e| Box::new(dml_fold_error_to_native(seq, &e)))
}

/// Run the per-task dispatch loop for a planned, non-streamed task set,
/// materializing all rows/columns/affected-count into a single
/// [`SqlOutcome::Response`].
///
/// Called from `execute_planned` after the streaming fast path has been
/// ruled out (or declined). Buffers writes when in an explicit transaction
/// block, exactly like the pgwire dispatch loop.
///
/// The statement's `rows_affected` and `command` come from one
/// [`StatementTag`] folded over every task, the same fold pgwire renders as
/// its command tag: a count-bearing task contributes its reported count and
/// verb, an opaque task (buffered write, index or graph maintenance,
/// computed-value payload) contributes nothing. Neither field is ever
/// synthesised from the task count.
pub(super) async fn run_dispatch_loop(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    tasks: Vec<PhysicalTask>,
    output_schema: Option<&OutputSchema>,
    database_id: DatabaseId,
    plan_lease_scope: Arc<crate::control::lease::QueryLeaseScope>,
) -> SqlOutcome {
    let mut all_columns: Option<Vec<String>> = None;
    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut last_lsn = 0u64;
    let mut statement_tag = StatementTag::default();
    // Checked once rather than per task — metering is disabled by default, so
    // this keeps the per-task extraction below (which clones the collection
    // name) a true no-op on the hot path for every deployment that hasn't
    // turned it on.
    let metering_enabled = ctx.state.metering_config.enabled;
    // Session-scoped sequence access for the statement's Control-Plane
    // computed columns: the registry plus this connection's `currval` map.
    let session_sequences = ctx.sessions.sequence_values(ctx.peer_addr);
    let sequences = SessionSequenceAccess::for_session(
        ctx.state,
        session_sequences,
        database_id,
        ctx.tenant_id(),
    );

    for task in tasks {
        if task.tenant_id != ctx.tenant_id() {
            return resp(sqlstate_error(seq, "42501", "tenant isolation violation"));
        }

        // Cloned before `route_in_tx_write` consumes `task`, so a staged
        // write whose outcome carries a computed payload (KV `Incr` /
        // `IncrFloat` / `Cas` / `GetSet` -- see `StagedTagKind::RawPayload`)
        // can be shaped into the response exactly like the non-staged branch
        // below shapes `task_resp.payload`.
        let plan_for_staged_response = task.plan.clone();
        // Extracted from the same clone above, before `task` is moved into
        // the routing call below — metering needs the collection/engine
        // shape after this task's dispatch succeeds. Only covers the direct
        // dispatch below (`InTxnRoute::Read`, i.e. autocommit writes/reads
        // and in-transaction reads); `Buffered`/`Staged` tasks `continue`
        // before reaching the metering call and are not billed here — a
        // `Buffered` task performs no dispatch yet (replayed at COMMIT), and
        // a `Staged` task's dispatch happens inside `route_in_tx_write`'s
        // closure, whose response is consumed before returning `Staged` and
        // is not observable at this loop level.
        let plan_metering_info =
            metering_enabled.then(|| PlanMeteringInfo::extract(&plan_for_staged_response));

        // A spent hard quota refuses the task before it runs; the charging
        // call at the end of this loop is on the success path and so can
        // never refuse anything itself.
        if let Some(info) = &plan_metering_info
            && let Err(e) = admit_quota_for_dispatch(ctx.state, &ctx.scope, info)
        {
            return resp(error_to_native_with_sqlstate(seq, "53400", &e));
        }

        // In transaction: route through the protocol-neutral staging gate.
        // Reads (including in-transaction reads) come back as `Read` with
        // `txn_id` stamped for read-your-own-writes; non-stageable writes are
        // buffered for COMMIT-time replay; stageable writes are applied to
        // the per-transaction overlay immediately for a real affected count
        // and statement-time constraint errors. Outside a transaction block,
        // `route_in_tx_write` always returns `Read(task)` unchanged, so the
        // autocommit path is untouched.
        // In-transaction `MERGE` and `UPDATE ... FROM` are resolved + staged at
        // STATEMENT time by the expander (read-your-own-writes for later
        // statements in the same txn); every other task falls through to the
        // neutral staging gate.
        let buffer_start = ctx.sessions.buffered_task_count(ctx.peer_addr);
        let routed = match route_in_tx_expander(
            ctx.state,
            ctx.sessions,
            ctx.peer_addr.into(),
            task,
            |stage_task| async move {
                dispatch_task(ctx, stage_task)
                    .await
                    .map(|(resp, _, _)| resp)
            },
        )
        .await
        {
            Ok(ExpanderOutcome::Handled(route)) => Ok(route),
            Ok(ExpanderOutcome::Passthrough(task)) => {
                route_in_tx_write(
                    ctx.state,
                    ctx.sessions,
                    ctx.peer_addr.into(),
                    *task,
                    |stage_task| async move {
                        dispatch_task(ctx, stage_task)
                            .await
                            .map(|(resp, _, _)| resp)
                    },
                )
                .await
            }
            Err(e) => Err(e),
        };
        if ctx.sessions.buffered_task_count(ctx.peer_addr) > buffer_start
            && !ctx.sessions.attach_tx_lease_scope_since(
                ctx.peer_addr,
                buffer_start,
                Arc::clone(&plan_lease_scope),
            )
        {
            return resp(sqlstate_error(
                seq,
                "XX000",
                "internal error: failed to retain descriptor leases for buffered transaction tasks",
            ));
        }
        // A buffered or staged write reports only a count: it has no stored
        // row to project at statement time, and COMMIT answers with one tag for
        // the whole transaction, so a statement that asked for rows would
        // otherwise succeed with none. Refused here for every verb, matching
        // the pgwire loop.
        let returns_rows = matches!(
            describe_plan(&plan_for_staged_response),
            PlanKind::ReturningRows
        );
        let task = match routed {
            Ok(InTxnRoute::Read(routed_task)) => *routed_task,
            Ok(InTxnRoute::Buffered) => {
                if returns_rows {
                    return resp(error_to_native(
                        seq,
                        &crate::control::server::shared::returning::
                            in_transaction_returning_unsupported(),
                    ));
                }
                // Applied at COMMIT: no count and no verb yet.
                statement_tag.fold_opaque();
                continue;
            }
            Ok(InTxnRoute::Staged(outcome)) => {
                if returns_rows {
                    return resp(error_to_native(
                        seq,
                        &crate::control::server::shared::returning::
                            in_transaction_returning_unsupported(),
                    ));
                }
                // The staging gate decided the verb and counted the rows it
                // applied to the overlay; only a computed-value payload
                // (`RawPayload`) is shaped instead of folded.
                if !matches!(outcome.kind, StagedTagKind::RawPayload) {
                    if let Err(e) = fold_dml(
                        &mut statement_tag,
                        seq,
                        staged_dml_outcome(outcome.kind, outcome.affected),
                    ) {
                        return SqlOutcome::Response(e);
                    }
                } else if outcome.payload.is_empty() {
                    statement_tag.fold_opaque();
                } else {
                    let plan_kind = describe_plan(&plan_for_staged_response);
                    let redaction = QueryRedaction::for_plan(
                        ctx.tenant_id(),
                        ctx.auth_context(),
                        &plan_for_staged_response,
                    );
                    match shape_response_materialized(MaterializedShapeRequest {
                        payload: &outcome.payload,
                        plan: &plan_for_staged_response,
                        plan_kind,
                        projection: output_schema,
                        state: ctx.state,
                        database_id,
                        tenant_id: ctx.tenant_id(),
                        redaction: Some(redaction.ctx(&ctx.state.redaction)),
                        sequences: Some(&sequences),
                    }) {
                        Ok(ShapeOutcome::Rows(mut shaped)) => {
                            if let Some(notice) = shaped.notice.take() {
                                warnings.push(notice);
                            }
                            let (cols, rows) = to_native_columns_rows(&shaped);
                            if !cols.is_empty() && all_columns.is_none() {
                                all_columns = Some(cols);
                            }
                            all_rows.extend(rows);
                        }
                        // The value rides in the payload, never as a count.
                        Ok(ShapeOutcome::Passthrough) => statement_tag.fold_opaque(),
                        Err(e) => return resp(shape_error_to_native(seq, &e)),
                    }
                }
                continue;
            }
            Err(StagingGateError::Dispatch(e)) => return resp(error_to_native(seq, &e)),
            Err(StagingGateError::Rejected { code }) => {
                return resp(error_code_to_native(seq, code.as_ref()));
            }
        };

        // `ClusterArray` plans are handled entirely on the Control Plane by
        // the `ArrayCoordinator` — they must never reach the SPSC bridge or
        // the trigger/DML machinery. A write reshapes into per-shard
        // `ArrayOp` tasks inside the staging gate above and never reaches
        // here as `ClusterArray`; only reads (`Slice`/`Agg`) and autocommit
        // `Put`/`Delete` arrive as this plan by the time `routed` resolves to
        // `Read`. Intercepted here, before `dispatch_task` would otherwise
        // route it through the gateway toward the Data Plane. Metering is
        // not applied here, matching pgwire's `ClusterArray` short-circuit,
        // which also does not meter this path.
        if matches!(
            task.plan,
            crate::bridge::envelope::PhysicalPlan::ClusterArray(_)
        ) {
            let authorized = match super::sql_gateway::authorize_native_task(ctx, &task) {
                Ok(a) => a,
                Err(e) => return resp(error_to_native(seq, &e)),
            };
            match super::cluster_array::dispatch_cluster_array_task(ctx, authorized, output_schema)
                .await
            {
                Ok(super::cluster_array::ClusterArrayOutcome::Rows {
                    columns,
                    rows,
                    notice,
                }) => {
                    if let Some(n) = notice {
                        warnings.push(n);
                    }
                    if !columns.is_empty() && all_columns.is_none() {
                        all_columns = Some(columns);
                    }
                    all_rows.extend(rows);
                }
                Ok(super::cluster_array::ClusterArrayOutcome::Affected(outcome)) => {
                    if let Err(e) = fold_dml(&mut statement_tag, seq, outcome) {
                        return SqlOutcome::Response(e);
                    }
                }
                Err(e) => return resp(error_to_native(seq, &e)),
            }
            continue;
        }

        let plan_for_response = task.plan.clone();
        let task_vshard = task.vshard_id;
        let task_database_id = task.database_id;
        let (task_resp, shard_watermarks, dist_reads) = match dispatch_task(ctx, task).await {
            Ok(r) => r,
            Err(e) => return resp(error_to_native(seq, &e)),
        };

        // Track reads for snapshot-isolation / cross-shard conflict detection at
        // the protocol-neutral layer — the native (canonical) transport records
        // identically to pgwire. Recorded BEFORE the error short-circuit so an
        // absent-key point read (a `NotFound` from the Data Plane) is captured
        // too; a "not found" is a validatable phantom observation. A multi-core
        // fan read records one entry per participating shard from the gather's
        // per-shard watermarks; a single read falls back to its one watermark.
        let records_read = task_resp.status == Status::Ok
            || task_resp.error_code.as_deref()
                == Some(&crate::bridge::envelope::ErrorCode::NotFound);
        if records_read
            && ctx.sessions.transaction_state(ctx.peer_addr)
                == crate::control::server::shared::session::TransactionState::InBlock
        {
            let watermarks = if shard_watermarks.is_empty() {
                vec![(task_vshard, task_resp.watermark_lsn)]
            } else {
                shard_watermarks
            };
            crate::control::server::shared::session::record_reads_for_response(
                ctx.state,
                ctx.sessions,
                ctx.peer_addr.into(),
                ctx.tenant_id(),
                crate::control::server::shared::session::ResponseReads {
                    plan: &plan_for_response,
                    watermarks: &watermarks,
                    read_version_lsn: task_resp.read_version_lsn,
                    found: task_resp.status == Status::Ok,
                    distributed_reads: &dist_reads,
                    read_lsn_vshard: task_vshard,
                },
            )
            .await;
        }

        if task_resp.status == Status::Error {
            return resp(error_response_to_native(seq, &task_resp));
        }

        // --- TRUNCATE RESTART IDENTITY ---
        // Autocommit only: a buffered truncate restarts its sequences at
        // COMMIT. Same rule as the pgwire dispatch loop.
        if let Some((collection, true)) = plan_for_response.truncate_target() {
            ctx.state
                .sequence_registry
                .restart_sequences_for_collection(
                    task_database_id.as_u64(),
                    ctx.tenant_id().as_u64(),
                    collection.as_str(),
                );
        }

        last_lsn = task_resp.watermark_lsn.as_u64();

        // This task's own row count, for metering below — distinct from
        // `statement_tag`/`all_rows`, which accumulate across every task in
        // the loop.
        let mut task_rows: Option<u64> = None;
        let plan_kind = describe_plan(&plan_for_response);
        let count_bearing = matches!(plan_kind, PlanKind::DmlResult(_) | PlanKind::DmlResultByOp);
        if task_resp.payload.is_empty() && !count_bearing {
            // Not a count-bearing plan (graph / vector / index write): no
            // count and no verb to report. A count-bearing plan with no
            // payload falls through so the count reader refuses it.
            statement_tag.fold_opaque();
        } else {
            let redaction =
                QueryRedaction::for_plan(ctx.tenant_id(), ctx.auth_context(), &plan_for_response);
            match shape_response_materialized(MaterializedShapeRequest {
                payload: &task_resp.payload,
                plan: &plan_for_response,
                plan_kind,
                projection: output_schema,
                state: ctx.state,
                database_id,
                tenant_id: ctx.tenant_id(),
                redaction: Some(redaction.ctx(&ctx.state.redaction)),
                sequences: Some(&sequences),
            }) {
                Ok(ShapeOutcome::Rows(mut shaped)) => {
                    if let Some(notice) = shaped.notice.take() {
                        warnings.push(notice);
                    }
                    let (cols, rows) = to_native_columns_rows(&shaped);
                    if !cols.is_empty() && all_columns.is_none() {
                        all_columns = Some(cols);
                    }
                    task_rows = Some(rows.len() as u64);
                    all_rows.extend(rows);
                }
                // A count-bearing write reports the rows it touched and its
                // verb; an opaque execution reports neither. Counting one per
                // dispatched task instead would report a row for a delete
                // that removed nothing and for an `ON CONFLICT DO NOTHING`
                // insert that skipped.
                Ok(ShapeOutcome::Passthrough) => {
                    match payload_to_dml_outcome(&task_resp.payload, plan_kind) {
                        Ok(Some(outcome)) => {
                            task_rows = Some(outcome.affected);
                            if let Err(e) = fold_dml(&mut statement_tag, seq, outcome) {
                                return SqlOutcome::Response(e);
                            }
                        }
                        Ok(None) => statement_tag.fold_opaque(),
                        Err(e) => return resp(error_to_native(seq, &e)),
                    }
                }
                Err(e) => return resp(shape_error_to_native(seq, &e)),
            }
        }

        // Metered here, once per successfully dispatched task (see the scope
        // note on `plan_metering_info` above for the tasks this does not
        // cover) — `task_resp.status == Status::Error` returned above, so
        // every path reaching here is the success path.
        if let Some(info) = &plan_metering_info {
            meter_dispatch(ctx.state, &ctx.scope, info, task_rows);
        }
    }

    let mut r = NativeResponse::ok(seq);
    r.watermark_lsn = last_lsn;
    r.warnings = warnings;
    if !all_rows.is_empty() {
        r.columns = all_columns;
        r.rows = Some(all_rows);
    }
    match statement_tag.finish() {
        Some(FoldedTag::Dml(outcome)) => apply_dml_outcome(&mut r, outcome),
        Some(FoldedTag::Opaque) | None => {}
    }
    resp(r)
}
