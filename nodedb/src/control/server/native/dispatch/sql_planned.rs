// SPDX-License-Identifier: BUSL-1.1

//! Planned SQL execution: DataFusion planning, admission, and dispatch of
//! the planned tasks to the Data Plane, Calvin, or a lazy stream.

use nodedb_types::TraceId;
use nodedb_types::protocol::NativeResponse;

use std::sync::Arc;

use crate::control::server::native::sqlstate_code::sqlstate_error;
use crate::control::server::shared::check_constraint::{
    enforce_statement_checks, enforce_statement_enum_labels,
};
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit_in_txn,
};
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::server::shared::statement_exec::{
    AtomicRoute, AtomicStatement, StatementExec, route_atomic_statement,
};
use crate::control::server::shared::write_admission::plan_is_write;

use super::sql::resp;
use super::sql_fold::{answer_to_native, statement_error_to_native};
use super::sql_loop::{PlannedStatement, run_dispatch_loop, session_sequences};
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
    // The derived writes read their source rows through the open
    // transaction's staging overlay, so they see this transaction's earlier
    // statements.
    let admission = match plan_authorize_and_admit_in_txn(
        PlanAdmissionRequest {
            state: ctx.state,
            query_ctx: ctx.query_ctx,
            scope: &scope,
            sql: &clean_sql,
            trace_id: TraceId::ZERO,
        },
        ctx.sessions.tx_id(ctx.peer_addr),
    )
    .await
    {
        Ok(admission) => admission,
        Err(error) => return resp(error_to_native(seq, &error)),
    };

    let output_schema = admission.output_schema;
    if admission.tasks.is_empty() {
        return resp(NativeResponse::status_row(seq, "OK"));
    }

    // The shared atomic routes, the ones every protocol takes: the implicit
    // transaction, the implicit-edge OLLP/Calvin gate, and a Calvin commit
    // for a write that spans vShards. The native session's
    // `cross_shard_txn` parameter picks the cross-shard mode. An unset value
    // is the documented default, `CrossShardTxnMode::Strict`.
    let in_txn_block = ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock;
    let session = DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    let sequences = session_sequences(ctx, database_id);
    let exec = StatementExec {
        state: ctx.state,
        identity: ctx.identity,
        scope: &ctx.scope,
        output_schema: Some(&output_schema),
        database_id,
        sequences: &sequences,
    };
    let routed = route_atomic_statement(
        &exec,
        AtomicStatement {
            tasks: admission.tasks,
            lease_scope: admission.lease_scope,
            // Covers the images every cross-shard materialized-sum balance in
            // the tasks was settled from, so Calvin's OCC check aborts rather
            // than committing a total folded from an image that has since
            // moved.
            sum_target_reads: admission.sum_target_reads,
        },
        &session,
    )
    .await;
    let AtomicStatement {
        tasks,
        lease_scope,
        sum_target_reads,
    } = match routed {
        Ok(AtomicRoute::Implicit(answer)) => return resp(answer_to_native(seq, answer)),
        // A RETURNING write surfaces its rows from the applied response. A
        // plain write reports the count its own mutation returned.
        Ok(AtomicRoute::Calvin(applied)) => {
            return resp(super::conversion::calvin_native_response(
                seq,
                applied.apply_result,
                &applied.plans,
                ctx.state,
                applied.database_id,
                ctx.tenant_id(),
                ctx.auth_context(),
            ));
        }
        Ok(AtomicRoute::PerTask(statement)) => statement,
        Err(error) => return resp(statement_error_to_native(seq, error)),
    };
    let mut lease_scope = Some(lease_scope);

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
        in_txn_block.then_some(&session),
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
