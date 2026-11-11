// SPDX-License-Identifier: BUSL-1.1

//! The sequential request runner: the statements of one request run in
//! order inside one transaction.
//!
//! [`sequential_gate`] picks the requests it runs. `select_route` calls the
//! gate, so the transports cannot drift.
//!
//! [`run_sequential_request`] opens the request's transaction, then runs
//! each statement on it in order:
//!
//! 1. A statement `router::claims` takes runs through `ddl::dispatch` on the
//!    transaction's context. Its writes stage, and its DDL buffers. A
//!    statement the router declines falls through to step 2.
//! 2. Any other statement plans alone, after the statement before it ran.
//!    The catalog overlay shows it every earlier DDL of the transaction. It
//!    takes the shared atomic routes, then the per-task loop of a statement
//!    in a transaction block.
//!
//! The transaction commits after the last statement. The first error rolls
//! it back and names its statement.
//!
//! The answer holds one part per statement. A transport sums the parts into
//! its one reply, as it sums a request the one-set path ran. A statement the
//! DDL router ran folds its results into its part: a row set adds its rows,
//! a status that carries a count adds its verb and count, and any other
//! status adds no row and no count.

use std::sync::Arc;

use nodedb_sql::parser::preprocess::lex::first_sql_word;

use crate::control::planner::context::QueryContext;
use crate::control::planner::context::query::{SoloKind, solo_kind};
use crate::control::server::response_shape::types::{DmlOutcome, StatementTag, TaskTagRole};
use crate::control::server::session_auth::apply_per_query_on_deny;
use crate::control::server::shared::ddl::DdlResult;
use crate::control::server::shared::ddl::neutral::router;
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit_in_txn,
};
use crate::control::server::shared::request_route::{RequestPolicy, select_route};
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::audit_context::AuditScope;
use crate::control::server::shared::sql_split::Statement;
use crate::control::trigger::statement_txn::{in_block, with_statement_txn_lifted};
use crate::types::TraceId;

use super::answer::{RequestAnswer, StatementAnswer, StatementPart};
use super::atomic::{AtomicRoute, AtomicStatement, calvin_answer, route_atomic_statement};
use super::error::{RequestError, StatementError};
use super::task_loop::{PlannedStatement, StatementExec, run_statement_loop};

/// Whether a request of `statements` runs through the sequential request
/// runner.
///
/// True when the request holds two or more statements, at least one of
/// them DDL or a statement the DDL router runs, and none the runner cannot
/// run. The runner cannot run transaction control, `SET`, `RESET`, or a
/// `SHOW` the router does not take. A request the gate declines takes the
/// one-set planner and its refusals.
pub(crate) fn sequential_gate(statements: &[Statement]) -> bool {
    if statements.len() < 2 {
        return false;
    }
    let mut sequential = false;
    for statement in statements {
        match solo_kind(&statement.text) {
            None => {}
            Some(SoloKind::Ddl | SoloKind::Routed) => sequential = true,
            Some(SoloKind::SessionCommand) if is_routed_show(&statement.text) => {
                sequential = true;
            }
            Some(SoloKind::SessionCommand | SoloKind::TransactionControl) => return false,
        }
    }
    sequential
}

/// Whether `statement` is a `SHOW` the DDL router runs.
fn is_routed_show(statement: &str) -> bool {
    first_sql_word(statement).is_some_and(|head| head.eq_ignore_ascii_case("SHOW"))
        && router::claims(statement)
}

/// What the sequential request runner runs.
pub(crate) struct SequentialRequest<'a> {
    /// Who runs the request. Its `schemas` are not read: each statement
    /// shapes against the schemas it plans.
    pub(crate) exec: &'a StatementExec<'a>,
    /// The planning context, private to the request or its connection: the
    /// runner binds the transaction's snapshot on it.
    pub(crate) query_ctx: &'a QueryContext,
    /// The request text split at top-level semicolons, split once by the
    /// transport.
    pub(crate) statements: &'a [Statement],
    pub(crate) trace_id: TraceId,
}

/// Run every statement of `request` in order inside one transaction.
///
/// `client` is the session the request arrived on. A transport with no
/// session passes a detached one. Outside a transaction block the
/// statements run on a private implicit transaction: it commits after the
/// last statement and rolls back on the first error. Inside the client's
/// block they run on that block, and its COMMIT applies them. Every
/// statement reads the transaction's one snapshot, taken at the first
/// statement.
///
/// The answer holds one part per statement. An error carries the index of
/// the statement that raised it. An error of the commit carries none.
pub(crate) async fn run_sequential_request(
    request: &SequentialRequest<'_>,
    client: &DmlTxnCtx<'_>,
) -> Result<RequestAnswer, RequestError> {
    let exec = request.exec;
    let count = request.statements.len();
    let answer = with_statement_txn_lifted(
        exec.state,
        exec.identity,
        client,
        !in_block(client),
        RequestError::from,
        async |txn: &DmlTxnCtx<'_>| {
            let mut statements = Vec::with_capacity(count);
            let mut last_lsn = 0;
            for (index, statement) in request.statements.iter().enumerate() {
                let (part, lsn) = run_statement(request, statement, txn)
                    .await
                    .map_err(|error| RequestError::in_statement(*error.error, index, count))?;
                if lsn != 0 {
                    last_lsn = lsn;
                }
                statements.push(part);
            }
            Ok(RequestAnswer {
                statements,
                last_lsn,
            })
        },
    )
    .await;
    // The cut ends with the request. A connection's next plan call binds its own.
    request.query_ctx.set_txn_snapshot(None);
    answer
}

/// Run one statement on `txn`. Returns its part and the watermark of its
/// last dispatched task, `0` when it dispatched none.
async fn run_statement(
    request: &SequentialRequest<'_>,
    carried: &Statement,
    txn: &DmlTxnCtx<'_>,
) -> Result<(StatementPart, u64), RequestError> {
    let base = request.exec;
    let statement = carried.text.as_str();
    // Each statement's DDL proposals carry that statement's own text.
    let _audit = AuditScope::statement(base.identity, statement);
    // The first call takes the transaction's snapshot. Each later call
    // answers the same one, so every statement reads one cut.
    let snapshot = txn.txn_snapshot(base.state).await?;
    request.query_ctx.set_txn_snapshot(snapshot);

    if router::claims(statement)
        && let Some(result) = crate::control::server::shared::ddl::dispatch(
            base.state,
            base.identity,
            statement.trim(),
            base.database_id,
            txn,
        )
        .await
    {
        let results = result.map_err(crate::Error::from)?;
        return Ok((ddl_part(results), 0));
    }

    // The statement reads its own `ON DENY` clause and plans under it alone.
    let (sql, scope) = apply_per_query_on_deny(statement, base.scope.clone());
    // The statement plans alone, as a request of one statement.
    let alone = std::slice::from_ref(carried);
    // Planned after the statement before it ran: the catalog overlay shows
    // this transaction's DDL, and derived writes read its staged rows.
    let admission = plan_authorize_and_admit_in_txn(
        PlanAdmissionRequest {
            state: base.state,
            query_ctx: request.query_ctx,
            scope: &scope,
            sql: &sql,
            statements: alone,
            route: select_route(alone, RequestPolicy::NONE),
            trace_id: request.trace_id,
        },
        txn.sessions.tx_id(txn.session_id),
    )
    .await?;
    if admission.tasks.is_empty() {
        return Ok((empty_part(), 0));
    }
    // A statement admitted under a lease this node then loses ends with a
    // retryable error before it dispatches.
    admission.lease_scope.check_not_revoked()?;

    let exec = StatementExec {
        state: base.state,
        identity: base.identity,
        scope: &scope,
        schemas: &admission.output_schemas,
        database_id: base.database_id,
        sequences: base.sequences,
    };
    let tenant_id = exec.tenant_id();
    let lifted = |error: StatementError| error.into_request_error(tenant_id);
    // `txn` is in a block, so the route stages each write. A write the
    // block cannot buffer answers `CrossShardInExplicitTransaction`.
    let routed = route_atomic_statement(
        &exec,
        AtomicStatement {
            tasks: admission.tasks,
            statement_map: admission.statement_map,
            lease_scope: admission.lease_scope,
            sum_target_reads: admission.sum_target_reads,
        },
        txn,
    )
    .await
    .map_err(lifted)?;
    let answer = match routed {
        AtomicRoute::PerTask(statement) => run_statement_loop(
            &exec,
            PlannedStatement {
                tasks: statement.tasks,
                statement_map: statement.statement_map,
                plan_lease_scope: Arc::new(statement.lease_scope),
                sum_target_reads: statement.sum_target_reads,
            },
            Some(txn),
        )
        .await
        .and_then(RequestAnswer::summed),
        AtomicRoute::Implicit(answer) => answer.summed(),
        AtomicRoute::Calvin(applied) => calvin_answer(&exec, applied),
    }
    .map_err(lifted)?;
    Ok(into_part(answer))
}

/// A statement's summed answer as its part of the request, with its
/// watermark.
fn into_part(answer: StatementAnswer) -> (StatementPart, u64) {
    let StatementAnswer {
        rows,
        warnings,
        tag,
        last_lsn,
    } = answer;
    (
        StatementPart {
            rows,
            warnings,
            tag,
        },
        last_lsn,
    )
}

/// The part of a statement that planned no task.
fn empty_part() -> StatementPart {
    StatementPart {
        rows: Vec::new(),
        warnings: Vec::new(),
        tag: None,
    }
}

/// Verbs a DDL-router status can count under, as tag names.
const COUNTED_VERBS: &[&str] = &["INSERT", "UPSERT", "UPDATE", "DELETE", "MERGE"];

/// The part of a statement the DDL router ran.
///
/// A row set adds its rows. A status with a count and a verb of
/// [`COUNTED_VERBS`] adds that verb and count. Any other status adds no row
/// and no count. An empty result adds nothing.
fn ddl_part(results: Vec<DdlResult>) -> StatementPart {
    let mut part = empty_part();
    let mut tag = StatementTag::for_request(std::iter::empty());
    for result in results {
        match result {
            DdlResult::Rows(mut shaped) => {
                if let Some(notice) = shaped.notice.take() {
                    part.warnings.push(notice);
                }
                part.rows.push(shaped);
            }
            DdlResult::Status {
                command,
                rows_affected,
            } => {
                let verb = COUNTED_VERBS.iter().copied().find(|verb| *verb == command);
                match (verb, rows_affected) {
                    (Some(verb), Some(affected)) => {
                        // Two verbs fold as `MIXED`. A request tag never
                        // refuses a verb pair.
                        let outcome = DmlOutcome { verb, affected };
                        if tag.fold(TaskTagRole::Counts, outcome).is_err() {
                            tag.fold_opaque();
                        }
                    }
                    _ => tag.fold_opaque(),
                }
            }
            DdlResult::Empty => {}
        }
    }
    part.tag = tag.finish();
    part
}

#[cfg(test)]
#[path = "sequential_tests.rs"]
mod tests;
