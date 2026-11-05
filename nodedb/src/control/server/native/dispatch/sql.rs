// SPDX-License-Identifier: BUSL-1.1

//! SQL dispatch: DataFusion planning + Data Plane execution.

use nodedb_types::protocol::NativeResponse;
use nodedb_types::strip_prefix_ascii_case_insensitive;
use nodedb_types::value::Value;

use std::sync::Arc;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::native::sqlstate_code::sqlstate_error;
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::txn_control::{TxnControl, classify as classify_txn_control};

use super::sql_admin::{handle_explain, handle_set_sql, handle_show_sql, is_session_show};
use super::sql_planned::execute_planned;
use super::streaming::SqlOutcome;
use super::transaction::{handle_begin, handle_commit, handle_rollback};
use super::transaction_savepoint::{
    handle_release_savepoint, handle_rollback_to_savepoint, handle_savepoint,
};
use super::{DispatchCtx, error_to_native, handle_reset};

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

    // Each statement's DDL proposals carry its own text, with the parameters
    // inlined above. A credential statement installs its redacted text.
    let _audit = crate::control::server::shared::session::audit_context::AuditScope::statement(
        ctx.identity,
        sql_trimmed,
    );

    // Transaction control. Its session handlers own the aborted-block state,
    // so it runs before the failed-transaction guard. pgwire classifies with
    // the same function, so both protocols accept the same spellings.
    if let Some(control) = classify_txn_control(sql_trimmed) {
        return resp(handle_txn_control(ctx, seq, control).await);
    }

    // An aborted block refuses every other statement until it ends.
    if ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::Failed {
        return resp(aborted_block_error(seq));
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
    // `allow_stream`) can return a `SqlStream`; otherwise this collapses to a
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

/// Run one classified transaction-control statement.
async fn handle_txn_control(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    control: TxnControl,
) -> NativeResponse {
    match control {
        TxnControl::Begin(modes) => handle_begin(ctx, seq, &modes),
        TxnControl::Commit => handle_commit(ctx, seq).await,
        TxnControl::Rollback => handle_rollback(ctx, seq).await,
        TxnControl::Savepoint(name) => handle_savepoint(ctx, seq, &name).await,
        TxnControl::Release(name) => handle_release_savepoint(ctx, seq, &name),
        TxnControl::RollbackTo(name) => handle_rollback_to_savepoint(ctx, seq, &name).await,
    }
}

/// The SQLSTATE 25P02 frame an aborted transaction block answers to every
/// statement except transaction control.
pub(crate) fn aborted_block_error(seq: u64) -> NativeResponse {
    sqlstate_error(
        seq,
        "25P02",
        "current transaction is aborted, commands ignored until end of transaction block",
    )
}

/// Wrap a materialized response as a non-streaming [`SqlOutcome`].
#[inline]
pub(super) fn resp(r: NativeResponse) -> SqlOutcome {
    SqlOutcome::Response(Box::new(r))
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
// gets a typed SQLSTATE rather than an opaque internal error.

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
