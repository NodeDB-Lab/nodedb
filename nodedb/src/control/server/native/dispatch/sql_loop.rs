// SPDX-License-Identifier: BUSL-1.1

//! Per-task dispatch loop for the DataFusion-planned SQL path.
//!
//! A statement in a transaction (the client's block, or the implicit
//! transaction a statement whose writes fire a BEFORE, INSTEAD OF or SYNC
//! AFTER body runs in) routes each task through the shared `txn_route`, the
//! route pgwire takes too: the write's trigger bodies join the transaction,
//! a shadowed clone takes its copy-on-write steps, and the write stages. The
//! single-task dispatch primitive lives in `sql_dispatch_task.rs`, and the
//! folding of each task's answer in `sql_fold.rs`.

use std::ops::ControlFlow;
use std::sync::Arc;

use nodedb_types::protocol::NativeResponse;

use crate::bridge::envelope::Status;
use crate::control::sequence::SessionSequenceAccess;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{
    PlanKind, TaskTagRole, describe_plan, payload_to_dml_outcome, replaced_write_outcome,
};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::server::shared::session::staging_gate::StagingGateError;
use crate::control::server::shared::txn_route::{
    StatementEvents, TxnTaskContext, TxnTaskOutcome, route_txn_task,
};
use crate::types::DatabaseId;
use nodedb_physical::physical_task::PhysicalTask;

use super::sql_dispatch_task::dispatch_task;
use super::sql_fold::{FoldShape, StatementFold};
use super::streaming::SqlOutcome;
use super::{
    DispatchCtx, error_code_to_native, error_response_to_native, error_to_native,
    error_to_native_with_sqlstate,
};
use crate::control::server::native::sqlstate_code::sqlstate_error;

/// Wrap a materialized response as a non-streaming [`SqlOutcome`].
#[inline]
fn resp(r: NativeResponse) -> SqlOutcome {
    SqlOutcome::Response(Box::new(r))
}

/// A planned statement the loop dispatches.
pub(super) struct PlannedStatement<'a> {
    pub tasks: Vec<PhysicalTask>,
    pub output_schema: Option<&'a OutputSchema>,
    pub database_id: DatabaseId,
    pub plan_lease_scope: Arc<crate::control::lease::QueryLeaseScope>,
    /// The row images the statement's cross-shard balances were settled
    /// from. A statement in a transaction adds them to its read set.
    pub sum_target_reads: Vec<ReadSetEntry>,
}

/// Run the per-task dispatch loop for a planned, non-streamed task set,
/// materializing all rows/columns/affected-count into a single
/// [`SqlOutcome::Response`]. `txn` is the statement's transaction, `None`
/// for an autocommit statement.
///
/// The statement's `rows_affected` and `command` come from one
/// [`crate::control::server::response_shape::types::StatementTag`] folded
/// over every task, the same fold pgwire renders as its command tag: a
/// count-bearing task contributes its reported count and verb, an opaque task
/// (buffered write, index or graph maintenance, computed-value payload)
/// contributes nothing. Neither field is ever synthesised from the task
/// count.
pub(super) async fn run_dispatch_loop(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    statement: PlannedStatement<'_>,
    txn: Option<&DmlTxnCtx<'_>>,
) -> SqlOutcome {
    let PlannedStatement {
        tasks,
        output_schema,
        database_id,
        plan_lease_scope,
        sum_target_reads,
    } = statement;
    if let Some(txn) = txn
        && !sum_target_reads.is_empty()
    {
        txn.sessions
            .record_read_entries(txn.session_id, sum_target_reads);
    }
    // A derived write beside the user's own (an implicit edge, a cross-shard
    // balance move) never answers the statement, exactly as Calvin's deposit
    // rule has it: the fold is built over every task to decide it.
    let mut fold = StatementFold::for_tasks(&tasks);
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
    let shape = FoldShape {
        seq,
        output_schema,
        database_id,
        sequences: &sequences,
    };
    let route_ctx = txn.map(|txn| TxnTaskContext {
        state: ctx.state,
        identity: ctx.identity,
        auth: ctx.auth_context(),
        txn,
        lease_scope: &plan_lease_scope,
        fire_triggers: true,
    });
    let mut statement_events = StatementEvents::default();

    for task in tasks {
        if task.tenant_id != ctx.tenant_id() {
            return resp(sqlstate_error(seq, "42501", "tenant isolation violation"));
        }

        // Cloned before routing consumes `task`: a staged write's rows and
        // computed-value payload shape against it.
        let plan = task.plan.clone();
        // Metering needs the collection/engine shape after this task's
        // dispatch succeeds. It covers the direct dispatch below (reads, and
        // writes that apply now). A staged write meters itself inside the
        // staging gate, and a buffered write meters at COMMIT.
        let plan_metering_info = metering_enabled.then(|| PlanMeteringInfo::extract(&plan));

        // A spent hard quota refuses the task before it runs; the charging
        // call at the end of this loop is on the success path and so can
        // never refuse anything itself.
        if let Some(info) = &plan_metering_info
            && let Err(e) = admit_quota_for_dispatch(ctx.state, &ctx.scope, info)
        {
            return resp(error_to_native_with_sqlstate(seq, "53400", &e));
        }

        // The task to dispatch below: the input task, or the one routing
        // hands back. Every other routing outcome answers the task here.
        let task = if let Some(route) = route_ctx.as_ref() {
            let routed = route_txn_task(
                route,
                task,
                &mut statement_events,
                |stage_task| async move {
                    dispatch_task(ctx, stage_task)
                        .await
                        .map(|(resp, _, _)| resp)
                },
            )
            .await;
            let answered = match routed {
                Ok(TxnTaskOutcome::Dispatch(routed_task)) => ControlFlow::Continue(*routed_task),
                Ok(TxnTaskOutcome::Buffered) => {
                    // Applied at COMMIT: no count and no verb yet.
                    fold.tag.fold_opaque();
                    ControlFlow::Break(Ok(()))
                }
                Ok(TxnTaskOutcome::Staged(outcome)) => {
                    ControlFlow::Break(fold.fold_staged(ctx, &shape, &plan, outcome))
                }
                Ok(TxnTaskOutcome::CloneHandled(clone_resp)) => ControlFlow::Break(
                    fold_answer(ctx, &shape, &mut fold, &plan, clone_resp.payload.as_ref())
                        .map(drop),
                ),
                Ok(TxnTaskOutcome::InsteadOf) => {
                    ControlFlow::Break(match replaced_write_outcome(describe_plan(&plan)) {
                        Some(outcome) => fold.fold_dml(seq, &plan, outcome),
                        None => {
                            fold.tag.fold_opaque();
                            Ok(())
                        }
                    })
                }
                Err(StagingGateError::Dispatch(e)) => return resp(error_to_native(seq, &e)),
                Err(StagingGateError::Rejected { code }) => {
                    return resp(error_code_to_native(seq, code.as_ref()));
                }
            };
            match answered {
                ControlFlow::Continue(routed_task) => routed_task,
                ControlFlow::Break(Ok(())) => continue,
                ControlFlow::Break(Err(e)) => return SqlOutcome::Response(e),
            }
        } else {
            task
        };

        // `ClusterArray` plans are handled entirely on the Control Plane by
        // the `ArrayCoordinator` — they must never reach the SPSC bridge or
        // the trigger/DML machinery. A write in a transaction reshapes into
        // per-shard `ArrayOp` tasks inside the staging gate above and never
        // reaches here as `ClusterArray`; only reads (`Slice`/`Agg`) and
        // autocommit `Put`/`Delete` arrive as this plan. Intercepted here,
        // before `dispatch_task` will otherwise route it through the gateway
        // toward the Data Plane. Metering is not applied here, matching
        // pgwire's `ClusterArray` short-circuit, which also does not meter
        // this path.
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
                        fold.warnings.push(n);
                    }
                    if !columns.is_empty() && fold.columns.is_none() {
                        fold.columns = Some(columns);
                    }
                    fold.rows.extend(rows);
                }
                Ok(super::cluster_array::ClusterArrayOutcome::Affected(outcome)) => {
                    if let Err(e) = fold.fold_dml(seq, &plan, outcome) {
                        return SqlOutcome::Response(e);
                    }
                }
                Err(e) => return resp(error_to_native(seq, &e)),
            }
            continue;
        }

        let task_vshard = task.vshard_id;
        let task_database_id = task.database_id;
        let (task_resp, shard_watermarks, dist_reads) = match dispatch_task(ctx, task).await {
            Ok(r) => r,
            Err(e) => return resp(error_to_native(seq, &e)),
        };

        // Track reads for snapshot-isolation / cross-shard conflict detection
        // at the protocol-neutral layer, into the statement's transaction.
        // Recorded BEFORE the error short-circuit so an absent-key point read
        // (a `NotFound` from the Data Plane) is captured too; a "not found" is
        // a validatable phantom observation. A multi-core fan read records one
        // entry per participating shard from the gather's per-shard
        // watermarks; a single read falls back to its one watermark.
        let records_read = task_resp.status == Status::Ok
            || task_resp.error_code.as_deref()
                == Some(&crate::bridge::envelope::ErrorCode::NotFound);
        if records_read && let Some(txn) = txn {
            let watermarks = if shard_watermarks.is_empty() {
                vec![(task_vshard, task_resp.watermark_lsn)]
            } else {
                shard_watermarks
            };
            crate::control::server::shared::session::record_reads_for_response(
                ctx.state,
                txn.sessions,
                txn.session_id,
                ctx.tenant_id(),
                crate::control::server::shared::session::ResponseReads {
                    plan: &plan,
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
        if let Some((collection, true)) = plan.truncate_target() {
            ctx.state
                .sequence_registry
                .restart_sequences_for_collection(
                    task_database_id.as_u64(),
                    ctx.tenant_id().as_u64(),
                    collection.as_str(),
                );
        }

        fold.last_lsn = task_resp.watermark_lsn.as_u64();

        let task_rows = match fold_answer(ctx, &shape, &mut fold, &plan, task_resp.payload.as_ref())
        {
            Ok(rows) => rows,
            Err(e) => return SqlOutcome::Response(e),
        };

        // Metered here, once per successfully dispatched task (see the scope
        // note on `plan_metering_info` above for the tasks this does not
        // cover) — `task_resp.status == Status::Error` returned above, so
        // every path reaching here is the success path.
        if let Some(info) = &plan_metering_info {
            meter_dispatch(ctx.state, &ctx.scope, info, task_rows);
        }
    }

    if let Some(route) = route_ctx.as_ref()
        && let Err(e) = statement_events.fire(route).await
    {
        return resp(error_to_native(seq, &e));
    }

    resp(fold.finish(seq))
}

/// Run a statement in an implicit transaction: it commits when every task
/// succeeds, and rolls back when one fails.
pub(super) async fn run_implicit_statement(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    statement: PlannedStatement<'_>,
) -> SqlOutcome {
    let client = DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    let outcome = crate::control::trigger::statement_txn::with_statement_txn_lifted(
        ctx.state,
        ctx.identity,
        &client,
        true,
        |error| resp(error_to_native(seq, &error)),
        async |txn: &DmlTxnCtx<'_>| {
            let outcome = run_dispatch_loop(ctx, seq, statement, Some(txn)).await;
            match &outcome {
                SqlOutcome::Response(r)
                    if r.status == nodedb_types::protocol::ResponseStatus::Error =>
                {
                    Err(outcome)
                }
                _ => Ok(outcome),
            }
        },
    )
    .await;
    match outcome {
        Ok(outcome) | Err(outcome) => outcome,
    }
}

/// Fold a dispatched task's answer `payload`: its rows, or its count. Returns
/// the task's own row count for metering, `None` when it carries none.
fn fold_answer(
    ctx: &DispatchCtx<'_>,
    shape: &FoldShape<'_>,
    fold: &mut StatementFold,
    plan: &crate::bridge::envelope::PhysicalPlan,
    payload: &[u8],
) -> Result<Option<u64>, Box<NativeResponse>> {
    let plan_kind = describe_plan(plan);
    let count_bearing = matches!(plan_kind, PlanKind::DmlResult(_) | PlanKind::DmlResultByOp);
    if payload.is_empty() && !count_bearing {
        // Not a count-bearing plan (graph / vector / index write): no count
        // and no verb to report. A count-bearing plan with no payload falls
        // through so the count reader refuses it.
        fold.tag.fold_opaque();
        return Ok(None);
    }
    if let Some(rows) = fold.fold_rows(ctx, shape, plan, plan_kind, payload)? {
        return Ok(Some(rows));
    }
    // A count-bearing write reports the rows it touched and its verb; an
    // opaque execution reports neither. Counting one per dispatched task
    // instead will report a row for a delete that removed nothing and for an
    // `ON CONFLICT DO NOTHING` insert that skipped.
    if fold.tag.role_of(plan) == TaskTagRole::Opaque {
        fold.tag.fold_opaque();
        return Ok(None);
    }
    match payload_to_dml_outcome(payload, plan_kind) {
        Ok(Some(outcome)) => {
            let affected = outcome.affected;
            fold.fold_dml(shape.seq, plan, outcome)?;
            Ok(Some(affected))
        }
        Ok(None) => {
            fold.tag.fold_opaque();
            Ok(None)
        }
        Err(e) => Err(Box::new(error_to_native(shape.seq, &e))),
    }
}
