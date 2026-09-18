// SPDX-License-Identifier: BUSL-1.1

//! SQL dispatch: DataFusion planning + Data Plane execution.

use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;
use nodedb_types::{TraceId, strip_prefix_ascii_case_insensitive};

use std::sync::Arc;

use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, classify_dispatch,
    dispatch_authorized_tasks_to_calvin,
};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::write_admission::all_writes_bufferable;

use super::sql_admin::{handle_explain, handle_set_sql, handle_show_sql, is_session_show};
use super::sql_loop::run_dispatch_loop;
use super::streaming::{SqlOutcome, try_open_sql_stream};
use super::transaction::{handle_begin, handle_commit, handle_rollback};
use super::transaction_savepoint::{
    handle_release_savepoint, handle_rollback_to_savepoint, handle_savepoint,
};
use super::{DispatchCtx, error_to_native, handle_reset};
use crate::control::server::native::sqlstate_code::sqlstate_error;

/// Handle a SQL statement: transaction control, SET/SHOW, DDL, or DataFusion.
///
/// `sql_params`, when present, carries the caller's bound values for
/// `$1`, `$2`, … placeholders in `sql`. The handler renders each value
/// as a SQL literal via `value_to_sql_literal` and substitutes the
/// placeholders before any other dispatch — DDL routing, planner,
/// transaction buffer — so every downstream sees one canonical SQL
/// string with literal values in place of placeholders. `None` (the
/// common case) routes the SQL through unmodified.
pub(crate) async fn handle_sql(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    sql: &str,
    sql_params: Option<&[Value]>,
) -> NativeResponse {
    // Non-streaming entry: SET-via-sql, SHOW-via-sql, EXPLAIN, COPY FROM. These
    // never reach the streamable SELECT fast path, so `allow_stream = false`
    // guarantees a `Response` outcome.
    handle_sql_inner(ctx, seq, sql, sql_params, false)
        .await
        .into_response()
}

/// Streaming-capable entry for `OpCode::Sql | OpCode::Ddl`.
///
/// Identical to [`handle_sql`] except an eligible autocommit, single-task,
/// unordered multi-row SELECT yields [`SqlOutcome::Stream`] for the session
/// loop to emit as multiple frames instead of one materialized response.
pub(crate) async fn handle_sql_streaming(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    sql: &str,
    sql_params: Option<&[Value]>,
) -> SqlOutcome {
    handle_sql_inner(ctx, seq, sql, sql_params, true).await
}

async fn handle_sql_inner(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    sql: &str,
    sql_params: Option<&[Value]>,
    allow_stream: bool,
) -> SqlOutcome {
    // Inline bound parameters before any dispatch — keeps the
    // substitution invariant in one place so the DDL router, planner,
    // and transaction buffer all see the same SQL shape regardless of
    // whether the caller sent params or inlined values directly.
    let substituted = sql_params
        .filter(|params| !params.is_empty())
        .map(|params| inline_params(sql, params));
    let sql = substituted.as_deref().unwrap_or(sql);
    let sql_trimmed = sql.trim();
    let upper = sql_trimmed.to_uppercase();

    ctx.sessions.ensure_session(*ctx.peer_addr);

    if sql_trimmed.is_empty() || sql_trimmed == ";" {
        return resp(NativeResponse::ok(seq));
    }

    // Transaction control.
    if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
        return resp(handle_begin(ctx, seq));
    }
    if upper == "COMMIT" || upper == "END" || upper == "END TRANSACTION" {
        return resp(handle_commit(ctx, seq).await);
    }
    if upper == "ROLLBACK" || upper == "ABORT" {
        return resp(handle_rollback(ctx, seq).await);
    }
    if upper.starts_with("SAVEPOINT ") {
        return resp(handle_savepoint(ctx, seq, sql_trimmed).await);
    }
    if upper.starts_with("RELEASE SAVEPOINT ") || upper.starts_with("RELEASE ") {
        return resp(handle_release_savepoint(ctx, seq, sql_trimmed));
    }
    if upper.starts_with("ROLLBACK TO ") {
        return resp(handle_rollback_to_savepoint(ctx, seq, sql_trimmed).await);
    }

    if ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::Failed {
        return resp(sqlstate_error(
            seq,
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        ));
    }

    // SET / SHOW / RESET.
    if upper.starts_with("SET ") {
        return resp(handle_set_sql(ctx, seq, sql_trimmed));
    }
    if let Some(rest) = strip_prefix_ascii_case_insensitive(sql_trimmed, "RESET ") {
        // The SQL form and the opcode form share one contract: the same
        // allowlist, and the connection default restored rather than an empty
        // string stored over the parameter.
        return resp(handle_reset(ctx, seq, rest.trim()));
    }
    if upper == "DISCARD ALL" {
        // Recreate only disposable session state. The authenticated database
        // binding belongs to the connection and must survive the reset.
        let database_id = ctx.sessions.get_current_database(ctx.peer_addr);
        ctx.sessions.remove(ctx.peer_addr);
        ctx.sessions.ensure_session(*ctx.peer_addr);
        if let Some(database_id) = database_id {
            ctx.sessions
                .set_current_database(ctx.peer_addr, database_id);
        }
        return resp(NativeResponse::status_row(seq, "DISCARD ALL"));
    }

    // Every statement that can inspect or mutate database state must pass the
    // selected-database gate before EXPLAIN, DDL, planning, or stream creation.
    let database_id = ctx.database_id();
    let emitter = ArcAuditEmitter(Arc::clone(&ctx.state.audit));
    if let Err(error) = authorize_database(ctx.identity, database_id, &emitter) {
        return resp(error_to_native(seq, &crate::Error::from(error)));
    }

    // EXPLAIN.
    if upper.starts_with("EXPLAIN ") {
        return resp(handle_explain(ctx, seq, sql_trimmed).await);
    }

    // DDL: try DDL router first.
    let txn_ctx = crate::control::server::shared::session::DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    if let Some(result) = crate::control::server::shared::ddl::dispatch(
        ctx.state,
        ctx.identity,
        sql_trimmed,
        database_id,
        &txn_ctx,
    )
    .await
    {
        return resp(super::ddl_result_to_native(seq, result));
    }

    // SHOW falls through to the session-variable handler only after the
    // DDL/admin router declines it.
    if upper.starts_with("SHOW ") && is_session_show(&upper) {
        return resp(handle_show_sql(ctx, seq, sql_trimmed));
    }

    // Quota check.
    if let Err(e) = ctx.state.check_tenant_quota(ctx.tenant_id()) {
        return resp(error_to_native(seq, &e));
    }

    // DataFusion planning + dispatch. The streaming fast path (when
    // `allow_stream`) may return a `SqlStream`; otherwise this collapses to a
    // single materialized `NativeResponse`.
    let _request = ctx.state.tenant_request_guard(ctx.tenant_id());
    let outcome = execute_planned(ctx, seq, sql_trimmed, database_id, allow_stream).await;

    if let SqlOutcome::Response(ref r) = outcome
        && r.status == nodedb_types::protocol::ResponseStatus::Error
    {
        ctx.sessions.fail_transaction(ctx.peer_addr);
    }

    outcome
}

/// Wrap a materialized response as a non-streaming [`SqlOutcome`].
#[inline]
fn resp(r: NativeResponse) -> SqlOutcome {
    SqlOutcome::Response(Box::new(r))
}

/// Plan SQL via DataFusion and dispatch tasks to the Data Plane.
///
/// When `allow_stream` is set and the planned statement is an eligible
/// autocommit, single-task, unordered multi-row SELECT, returns
/// [`SqlOutcome::Stream`] for lazy frame emission. Every other case — writes,
/// in-block buffering, multi-task, set-ops, errors — collapses to a single
/// [`SqlOutcome::Response`].
async fn execute_planned(
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
    match classify_dispatch(
        &tasks,
        &crate::control::planner::calvin::read_vshards_of(&sum_target_reads),
    ) {
        DispatchClass::SingleShard { .. } => {}
        DispatchClass::MultiShard { .. } => {
            // Dispatching to Calvin here applies the statement durably at
            // statement time, escaping the transaction buffer. Inside a block,
            // fall through to the per-task staging gate when the gate can
            // buffer every write — COMMIT flushes the whole buffer through
            // Calvin. Anything else is refused, matching pgwire.
            let in_txn_block =
                ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock;
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
                        "XX000",
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
            "XX000",
            "internal error: query lease scope missing before materialized SQL dispatch",
        ));
    };
    run_dispatch_loop(
        ctx,
        seq,
        tasks,
        Some(&output_schema),
        database_id,
        Arc::new(lease_scope),
    )
    .await
}

// ─── Bound parameter substitution ────────────────────────────────────
//
// The native protocol carries bound parameters in `TextFields::sql_params`
// as a zerompk-MessagePack `Vec<Value>`. Inlining them into the SQL
// string before any dispatch is the simplest correct shape: it keeps
// the planner, DDL router, and transaction buffer unaware of the
// distinction, and matches what `nodedb_sql::parser::preprocess`
// expects (a single, fully-resolved SQL string).
//
// Errors here surface as `42P02` (`undefined_parameter`) so the client
// gets a typed SQLSTATE rather than a generic `XX000` opaque failure.

/// Substitute `$N` placeholders in `sql` with canonical SQL literals.
fn inline_params(sql: &str, params: &[Value]) -> String {
    let literals = params.iter().map(Value::to_sql_literal).collect::<Vec<_>>();
    crate::control::server::shared::sql::placeholder::rewrite_sql_placeholders(sql, &literals)
}

#[cfg(test)]
mod tests {
    use super::inline_params;
    use crate::bridge::envelope::PhysicalPlan;
    use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp};
    use nodedb_types::Value;

    #[test]
    fn native_params_use_canonical_literals_for_scalar_and_nested_values() {
        let values = [
            Value::String("x'; --".into()),
            Value::Array(vec![Value::Integer(1), Value::String("two".into())]),
        ];
        let sql = inline_params("SELECT $1, $2", &values);
        assert_eq!(
            sql,
            format!(
                "SELECT {}, {}",
                values[0].to_sql_literal(),
                values[1].to_sql_literal()
            )
        );
    }

    #[test]
    fn columnar_scan_is_sharded_source() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "metrics",
            ),
            projection: Vec::new(),
            limit: 10,
            filters: Vec::new(),
            rls_filters: Vec::new(),
            sort_keys: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
            computed_columns: Vec::new(),
        });
        assert!(plan.is_sharded_source());
    }

    #[test]
    fn document_scan_is_still_sharded_source() {
        let plan = PhysicalPlan::Document(DocumentOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            filters: Vec::new(),
            limit: 10,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        });
        assert!(plan.is_sharded_source());
    }
}
