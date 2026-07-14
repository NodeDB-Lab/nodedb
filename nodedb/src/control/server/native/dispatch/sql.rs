// SPDX-License-Identifier: BUSL-1.1

//! SQL dispatch: DataFusion planning + Data Plane execution.

use nodedb_types::TraceId;
use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

use std::sync::Arc;

use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, classify_dispatch,
    dispatch_tasks_to_calvin,
};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::shared::authorization::{authorize_database, authorize_task_set};
use crate::control::server::shared::session::TransactionState;

use super::sql_admin::{handle_explain, handle_set_sql, handle_show_sql, is_session_show};
use super::sql_loop::run_dispatch_loop;
use super::streaming::{SqlOutcome, try_open_sql_stream};
use super::transaction::{handle_begin, handle_commit, handle_rollback};
use super::transaction_savepoint::{
    handle_release_savepoint, handle_rollback_to_savepoint, handle_savepoint,
};
use super::{DispatchCtx, error_to_native};

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
    let substituted: Option<String> = match sql_params {
        Some(params) if !params.is_empty() => match inline_params(sql, params) {
            Ok(s) => Some(s),
            Err(msg) => return resp(NativeResponse::error(seq, "42P02", msg)),
        },
        _ => None,
    };
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
        return resp(NativeResponse::error(
            seq,
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        ));
    }

    // SET / SHOW / RESET.
    if upper.starts_with("SET ") {
        return resp(handle_set_sql(ctx, seq, sql_trimmed));
    }
    if upper.starts_with("RESET ") {
        let param = sql_trimmed[6..].trim().to_lowercase();
        ctx.sessions
            .set_parameter(ctx.peer_addr, param, String::new());
        return resp(NativeResponse::status_row(seq, "RESET"));
    }
    if upper == "DISCARD ALL" {
        ctx.sessions.remove(ctx.peer_addr);
        ctx.sessions.ensure_session(*ctx.peer_addr);
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
        addr: ctx.peer_addr,
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
    ctx.state.tenant_request_start(ctx.tenant_id());
    let outcome = execute_planned(ctx, seq, sql_trimmed, database_id, allow_stream).await;
    ctx.state.tenant_request_end(ctx.tenant_id());

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
    // Extract per-query ON DENY override (e.g., SELECT ... ON DENY ERROR 'CODE' MESSAGE '...').
    let mut auth_ctx = ctx.auth_context.clone();
    let clean_sql =
        crate::control::server::session_auth::extract_and_apply_on_deny(sql, &mut auth_ctx);

    let perm_cache = ctx.state.permission_cache.read().await;
    let sec = crate::control::planner::context::PlanSecurityContext {
        identity: ctx.identity,
        auth: &auth_ctx,
        rls_store: &ctx.state.rls,
        permissions: &ctx.state.permissions,
        roles: &ctx.state.roles,
        permission_cache: Some(&*perm_cache),
    };
    let (mut tasks, output_schema) = match ctx
        .query_ctx
        .plan_sql_with_rls(crate::control::planner::context::PlanSqlWithRlsParams {
            sql: &clean_sql,
            tenant_id: ctx.tenant_id(),
            database_id,
            sec: &sec,
        })
        .await
    {
        Ok(t) => t,
        Err(e) => return resp(error_to_native(seq, &e)),
    };

    if tasks.is_empty() {
        return resp(NativeResponse::status_row(seq, "OK"));
    }

    // Implicit graph-edge extraction (pgwire parity): a schemaless document
    // carrying `_from`/`_to` is mirrored as a `GraphOp::EdgePut` task so the
    // classify/Calvin/single-shard logic below routes it like an explicit edge.
    if let Err(e) = crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        ctx.state,
        &mut tasks,
        ctx.tenant_id(),
        database_id,
        TraceId::ZERO,
    )
    .await
    {
        return resp(error_to_native(seq, &e));
    }

    drop(perm_cache);
    let emitter = ArcAuditEmitter(Arc::clone(&ctx.state.audit));
    if let Err(error) = authorize_task_set(
        ctx.identity,
        &tasks,
        &ctx.state.permissions,
        &ctx.state.roles,
        &emitter,
    ) {
        return resp(error_to_native(seq, &crate::Error::from(error)));
    }

    // Implicit-edge DELETE/UPDATE routing gate (native-protocol parity with
    // pgwire). See `edge_recon_gate` for the full invariant and guard
    // documentation. Returns early when the gate fires, consuming `tasks`.
    {
        use super::edge_recon_gate::{EdgeReconResult, try_edge_recon_dispatch};
        match try_edge_recon_dispatch(ctx, seq, tasks).await {
            EdgeReconResult::Outcome(outcome) => return outcome,
            EdgeReconResult::NotFired(returned_tasks) => {
                tasks = returned_tasks;
            }
        }
    }

    // Cross-shard write parity with pgwire: classify the planned task set and,
    // for a strict multi-shard write, route the whole batch through the Calvin
    // sequencer so it commits atomically. Single-shard (and best-effort) keep
    // the existing per-task gateway/SPSC dispatch loop below unchanged.
    // Autocommit single-statement dispatch: no session read-set to widen with.
    match classify_dispatch(&tasks, &std::collections::BTreeSet::new()) {
        DispatchClass::SingleShard { .. } => {}
        DispatchClass::MultiShard { .. } => {
            // Reject a cross-shard write inside an explicit transaction block,
            // matching pgwire's `CrossShardInExplicitTransaction` semantics.
            // Native buffers in-block writes per task below; a multi-shard
            // write cannot be buffered atomically, so reject up front.
            if ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock {
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
            if cross_shard_mode == CrossShardTxnMode::Strict {
                return match dispatch_tasks_to_calvin(
                    ctx.state,
                    &tasks,
                    ctx.tenant_id(),
                    cross_shard_mode,
                    TxnDispatchPosition::Autocommit,
                    &[],
                )
                .await
                {
                    // Calvin committed. A RETURNING write surfaces its rows from
                    // the applied Response; a plain write reports one row-affected
                    // per task, mirroring pgwire's one-tag-per-task synthesis.
                    Ok(apply_resp) => {
                        let returning_plan = tasks
                            .iter()
                            .find(|t| {
                                matches!(
                                    crate::control::server::response_shape::types::describe_plan(
                                        &t.plan,
                                    ),
                                    crate::control::server::response_shape::types::PlanKind::ReturningRows
                                )
                            })
                            .map(|t| t.plan.clone());
                        resp(super::conversion::calvin_native_response(
                            seq,
                            apply_resp,
                            returning_plan.as_ref(),
                            tasks.len() as u64,
                            ctx.state,
                            database_id,
                            ctx.tenant_id(),
                        ))
                    }
                    Err(e) => resp(error_to_native(seq, &e)),
                };
            }
            // BestEffortNonAtomic falls through to the per-task loop below.
        }
    }

    // Streaming fast path: an eligible autocommit, single-task, unordered
    // multi-row SELECT streams its rows as multiple frames. The complete final
    // task set was authorized above before this stream can be opened.
    if allow_stream {
        match try_open_sql_stream(ctx, seq, &tasks, database_id, Some(&output_schema)).await {
            Ok(Some(stream)) => return SqlOutcome::Stream(stream),
            Ok(None) => {}
            Err(e) => return resp(error_to_native(seq, &e)),
        }
    }

    run_dispatch_loop(ctx, seq, tasks, Some(&output_schema), database_id).await
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

/// Substitute `$N` placeholders in `sql` with the rendered SQL literal
/// form of each value. Returns the new SQL or a typed error message
/// suitable for `42P02`.
fn inline_params(sql: &str, params: &[Value]) -> Result<String, String> {
    // Render every value first so a render failure aborts the whole
    // substitution rather than partially rewriting placeholders — a
    // partially-rewritten SQL would re-create the silent-wrong pattern
    // the trait contract guards against.
    let literals: Vec<String> = params
        .iter()
        .map(value_to_sql_literal)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(crate::control::server::shared::sql::placeholder::rewrite_sql_placeholders(sql, &literals))
}

/// Render a `nodedb_types::Value` as a SQL literal usable in
/// expression position.
///
/// Strings are single-quote escaped per `standard_conforming_strings=on`;
/// numeric / boolean / null values are formatted directly. Variants
/// that have no canonical SQL literal form (objects, arrays, vectors,
/// binary, datetime) return `Err` rather than producing a malformed
/// statement — the caller surfaces this as `42P02`.
fn value_to_sql_literal(v: &Value) -> Result<String, String> {
    match v {
        Value::Null => Ok("NULL".into()),
        Value::Bool(b) => Ok(if *b { "TRUE".into() } else { "FALSE".into() }),
        Value::Integer(i) => Ok(i.to_string()),
        Value::Float(f) => Ok(f.to_string()),
        Value::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
        other => Err(format!(
            "sql_params value has no SQL literal form: {other:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::PhysicalPlan;
    use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp};

    #[test]
    fn columnar_scan_is_sharded_source() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: "metrics".into(),
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
            collection: "docs".into(),
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
