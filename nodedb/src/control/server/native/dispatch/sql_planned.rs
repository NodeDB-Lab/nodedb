// SPDX-License-Identifier: BUSL-1.1

//! Planned SQL execution: DataFusion planning, admission, and dispatch of
//! the planned tasks to the Data Plane, Calvin, or a lazy stream.

use nodedb_types::TraceId;
use nodedb_types::protocol::NativeResponse;

use std::sync::Arc;

use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, classify_dispatch,
    dispatch_authorized_tasks_to_calvin,
};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::native::sqlstate_code::sqlstate_error;
use crate::control::server::shared::check_constraint::{
    enforce_statement_checks, enforce_statement_enum_labels,
};
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::server::shared::txn_route::{
    statement_needs_implicit_txn, unbufferable_joined_statement,
};
use crate::control::server::shared::write_admission::{all_writes_bufferable, plan_is_write};

use super::sql::resp;
use super::sql_loop::{PlannedStatement, run_dispatch_loop, run_implicit_statement};
use super::streaming::{SqlOutcome, try_open_sql_stream};
use super::{DispatchCtx, ddl_result_to_native, error_to_native};

/// Plan SQL via DataFusion and dispatch tasks to the Data Plane.
///
/// When `allow_stream` is set and the planned statement is an eligible
/// autocommit, single-task, unordered multi-row SELECT, returns
/// [`SqlOutcome::Stream`] for lazy frame emission. Every other case — writes,
/// in-block buffering, multi-task, set-ops, errors — collapses to a single
/// [`SqlOutcome::Response`].
pub(super) async fn execute_planned(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    sql: &str,
    database_id: crate::types::DatabaseId,
    allow_stream: bool,
) -> SqlOutcome {
    // `ctx.scope` is the single request-scoped auth contract, built once per
    // request in `session::request::handle_request` — it already carries
    // `database_id` (agreeing with the `database_id` passed into this
    // function) and a scope-grant-enriched `AuthContext`. A per-query
    // `ON DENY` override (e.g. `SELECT ... ON DENY ERROR 'CODE' MESSAGE
    // '...'`) rebuilds the scope rather than mutating it in place
    // (`RequestAuthScope` has no `&mut` path to `on_deny_override` by
    // design), so a clone is taken here: `ctx.scope` stays the canonical,
    // unmodified scope for any other consumer of `ctx` during this request,
    // while `scope` below is the (possibly overridden) one this statement
    // dispatches and admits under.
    let (clean_sql, scope) =
        crate::control::server::session_auth::apply_per_query_on_deny(sql, ctx.scope.clone());

    // Forward every per-session planning GUC (vector-dim quota, force-shuffle
    // join/agg overrides + partition counts, broadcast / shuffle-aggregate cost
    // thresholds) into the shared query context before planning — the same
    // protocol-neutral resolution pgwire performs, so the canonical native
    // transport honors these overrides identically. Native plans without a plan
    // cache, so the returned bypass flags are not needed here.
    crate::control::server::shared::planning_overrides::apply_planning_session_overrides(
        ctx.query_ctx,
        ctx.sessions,
        ctx.state,
        ctx.peer_addr,
        ctx.tenant_id(),
    );

    // General CHECK constraints and enum labels on an INSERT or UPDATE are
    // enforced before planning, by the same code pgwire runs.
    if let Err(error) = enforce_statement_checks(
        ctx.state,
        ctx.identity,
        ctx.tenant_id(),
        database_id,
        scope.auth(),
        ctx.sessions.tx_id(ctx.peer_addr),
        &clean_sql,
    )
    .await
    .and_then(|()| {
        enforce_statement_enum_labels(ctx.state, ctx.tenant_id(), database_id, &clean_sql)
    }) {
        return resp(ddl_result_to_native(seq, Err(error)));
    }

    // Planning, authorization, implicit-edge extraction (pgwire parity: a
    // schemaless document carrying `_from`/`_to` is mirrored as a
    // `GraphOp::EdgePut` task so the classify/Calvin/single-shard logic below
    // routes it like an explicit edge) and lease admission run as ONE retried
    // unit, so a descriptor drain starting between the planner's catalog read
    // and the lease acquisition is absorbed rather than surfaced. Admission
    // still follows authorization inside the unit, so denied requests consume
    // no descriptor lease. The scope stays alive through all dispatch and
    // response shaping below.
    let admission = match plan_authorize_and_admit(PlanAdmissionRequest {
        state: ctx.state,
        query_ctx: ctx.query_ctx,
        scope: &scope,
        sql: &clean_sql,
        trace_id: TraceId::ZERO,
    })
    .await
    {
        Ok(admission) => admission,
        Err(error) => return resp(error_to_native(seq, &error)),
    };

    let mut tasks = admission.tasks;
    let output_schema = admission.output_schema;
    // Re-derived here rather than carried from `admission`: Calvin dispatch
    // and implicit-edge reconciliation are trusted internal mechanisms that
    // never reach the clone-checked dispatch boundary (they don't call
    // `dispatch_authorized_to_data_plane` / `Gateway::execute`), so a plain
    // batch authorize is correct for them. `run_dispatch_loop` below clone-checks
    // and authorizes each task itself, immediately before its own dispatch.
    let mut authorized_tasks =
        match crate::control::server::shared::authorization::authorize_task_set(
            ctx.identity,
            &tasks,
            &ctx.state.permissions,
            &ctx.state.roles,
            &ArcAuditEmitter(Arc::clone(&ctx.state.audit)),
        ) {
            Ok(authorized) => authorized,
            Err(error) => return resp(error_to_native(seq, &crate::Error::from(error))),
        };
    let mut lease_scope = Some(admission.lease_scope);
    // Covers the images every cross-shard materialized-sum balance in `tasks`
    // was settled from, so Calvin's OCC check aborts rather than committing a
    // total folded from an image that has since moved.
    let sum_target_reads = admission.sum_target_reads;

    if tasks.is_empty() {
        return resp(NativeResponse::status_row(seq, "OK"));
    }

    // A statement outside a transaction block whose writes fire a BEFORE,
    // INSTEAD OF or SYNC AFTER body, a MERGE into an edge-bearing collection,
    // or a join-expanding write into the source of a cross-shard
    // materialized sum, runs in an implicit transaction ahead of every autocommit
    // route: its writes stage on their vShards' leaders and commit together
    // at its end.
    let in_txn_block = ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock;
    if !in_txn_block && statement_needs_implicit_txn(ctx.state, &tasks) {
        if !all_writes_bufferable(&tasks) {
            return resp(error_to_native(seq, &unbufferable_joined_statement()));
        }
        let Some(lease_scope) = lease_scope.take() else {
            return resp(sqlstate_error(
                seq,
                nodedb_types::error::sqlstate::INTERNAL_ERROR,
                "internal error: query lease scope missing before SQL dispatch",
            ));
        };
        return run_implicit_statement(
            ctx,
            seq,
            PlannedStatement {
                tasks,
                output_schema: Some(&output_schema),
                database_id,
                plan_lease_scope: Arc::new(lease_scope),
                sum_target_reads,
            },
        )
        .await;
    }

    // Implicit-edge DELETE/UPDATE routing gate (native-protocol parity with
    // pgwire). See `edge_recon_gate` for the full invariant and guard
    // documentation. Returns early when the gate fires, consuming `tasks`.
    {
        use super::edge_recon_gate::{EdgeReconResult, try_edge_recon_dispatch};
        match try_edge_recon_dispatch(ctx, seq, tasks, authorized_tasks).await {
            EdgeReconResult::Outcome(outcome) => return outcome,
            EdgeReconResult::NotFired(returned_tasks, returned_authorized) => {
                tasks = returned_tasks;
                authorized_tasks = returned_authorized;
            }
        }
    }

    // Cross-shard write parity with pgwire: classify the planned task set and,
    // for a strict multi-shard write, route the whole batch through the Calvin
    // sequencer so it commits atomically. Single-shard (and best-effort) keep
    // the existing per-task gateway/SPSC dispatch loop below unchanged.
    // Autocommit single-statement dispatch: no session read-set to widen with.
    let sum_read_vshards = match crate::control::planner::calvin::read_vshards_of(&sum_target_reads)
    {
        Ok(vshards) => vshards,
        Err(error) => return resp(error_to_native(seq, &error)),
    };
    match classify_dispatch(&tasks, &sum_read_vshards) {
        DispatchClass::SingleShard { .. } => {}
        DispatchClass::MultiShard { .. } => {
            // Dispatching to Calvin here applies the statement durably at
            // statement time, escaping the transaction buffer. Inside a block,
            // fall through to the per-task staging gate when the gate can
            // buffer every write — COMMIT flushes the whole buffer through
            // Calvin. Anything else is refused, matching pgwire.
            if in_txn_block && !all_writes_bufferable(&tasks) {
                return resp(error_to_native(
                    seq,
                    &crate::Error::CrossShardInExplicitTransaction,
                ));
            }

            // Native has no per-session `cross_shard_txn` parameter wired, so it
            // reads the same `SessionStore` accessor pgwire uses; an unset value
            // defaults to `CrossShardTxnMode::Strict` (the documented default),
            // so native multi-shard writes route through Calvin by default.
            let cross_shard_mode = ctx.sessions.cross_shard_txn_mode(ctx.peer_addr);
            if !in_txn_block && cross_shard_mode == CrossShardTxnMode::Strict {
                return match dispatch_authorized_tasks_to_calvin(
                    ctx.state,
                    authorized_tasks,
                    ctx.tenant_id(),
                    cross_shard_mode,
                    TxnDispatchPosition::Autocommit,
                    &sum_target_reads,
                    None,
                )
                .await
                {
                    // Calvin committed. A RETURNING write surfaces its rows from
                    // the applied Response; a plain write reports the affected
                    // count its own mutation returned.
                    Ok(apply_resp) => {
                        let plans: Vec<_> = tasks.iter().map(|t| t.plan.clone()).collect();
                        resp(super::conversion::calvin_native_response(
                            seq,
                            apply_resp,
                            &plans,
                            ctx.state,
                            database_id,
                            ctx.tenant_id(),
                            ctx.auth_context(),
                        ))
                    }
                    Err(e) => resp(error_to_native(seq, &e)),
                };
            }
            // An in-block statement and `BestEffortNonAtomic` both fall
            // through to the per-task loop below.
        }
    }

    // A native lazy stream outlives this handler, so transfer its descriptor
    // leases to the session-owned `SqlStream` before returning it. The session
    // loop retains that owner through final emission or connection teardown.
    if allow_stream {
        match try_open_sql_stream(ctx, seq, &tasks, database_id, Some(&output_schema)).await {
            Ok(Some(mut stream)) => {
                let Some(scope) = lease_scope.take() else {
                    return resp(sqlstate_error(
                        seq,
                        nodedb_types::error::sqlstate::INTERNAL_ERROR,
                        "internal error: query lease scope missing before SQL stream dispatch",
                    ));
                };
                if let Err(error) = stream.attach_lease_scope(scope) {
                    return resp(error_to_native(seq, &error));
                }
                return SqlOutcome::Stream(Box::new(stream));
            }
            Ok(None) => {}
            Err(error) => return resp(error_to_native(seq, &error)),
        }
    }

    // Materialized statements retain their admitted scope in an Arc so any
    // writes buffered during this statement keep the same descriptor leases
    // after this local owner is dropped. Lazy streams above retain the raw
    // scope directly in their stream owner.
    let Some(lease_scope) = lease_scope.take() else {
        return resp(sqlstate_error(
            seq,
            nodedb_types::error::sqlstate::INTERNAL_ERROR,
            "internal error: query lease scope missing before materialized SQL dispatch",
        ));
    };
    // A statement admitted under a lease this node then loses ends with a
    // retryable error: a read mid-flight, a write only before dispatch.
    let lease_scope = Arc::new(lease_scope);
    if let Err(revoked) = lease_scope.check_not_revoked() {
        return resp(error_to_native(seq, &revoked));
    }
    let read_only = tasks.iter().all(|task| !plan_is_write(&task.plan));
    let guard_scope = Arc::clone(&lease_scope);
    let client_txn = DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    let run = run_dispatch_loop(
        ctx,
        seq,
        PlannedStatement {
            tasks,
            output_schema: Some(&output_schema),
            database_id,
            plan_lease_scope: lease_scope,
            sum_target_reads,
        },
        in_txn_block.then_some(&client_txn),
    );
    if read_only {
        match guard_scope.guard(run).await {
            Ok(outcome) => outcome,
            Err(revoked) => resp(error_to_native(seq, &revoked)),
        }
    } else {
        run.await
    }
}
