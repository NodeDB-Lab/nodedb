// SPDX-License-Identifier: BUSL-1.1

//! The native SQL path's per-task loop: the shared statement loop
//! (`shared::statement_exec`), rendered as one native frame.
//!
//! A statement in a transaction (the client's block, or the implicit
//! transaction a statement whose writes fire a BEFORE, INSTEAD OF or SYNC
//! AFTER body runs in) routes each task through the shared `txn_route`, the
//! route pgwire takes too. The frame renders in `sql_fold.rs`.

use std::sync::Arc;

use crate::control::sequence::SessionSequenceAccess;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::server::shared::statement_exec::{self, StatementExec};
use crate::types::DatabaseId;
use nodedb_physical::physical_task::PhysicalTask;

use super::DispatchCtx;
use super::sql_fold::statement_result_to_native;
use super::streaming::SqlOutcome;

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

/// The session's sequence access for a statement in `database_id`: the
/// registry plus this connection's `currval` map.
pub(super) fn session_sequences<'a>(
    ctx: &DispatchCtx<'a>,
    database_id: DatabaseId,
) -> SessionSequenceAccess<'a> {
    SessionSequenceAccess::for_session(
        ctx.state,
        ctx.sessions.sequence_values(ctx.peer_addr),
        database_id,
        ctx.tenant_id(),
    )
}

/// Run the per-task loop for a planned, non-streamed task set, rendered as
/// one [`SqlOutcome::Response`]. `txn` is the statement's transaction, `None`
/// for an autocommit statement.
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
    let sequences = session_sequences(ctx, database_id);
    let exec = StatementExec {
        state: ctx.state,
        identity: ctx.identity,
        scope: &ctx.scope,
        output_schema,
        database_id,
        sequences: &sequences,
        client_session: Some(DmlTxnCtx {
            sessions: ctx.sessions,
            session_id: ctx.peer_addr.into(),
        }),
    };
    let result = statement_exec::run_statement_loop(
        &exec,
        statement_exec::PlannedStatement {
            tasks,
            plan_lease_scope,
            sum_target_reads,
        },
        txn,
    )
    .await;
    SqlOutcome::Response(Box::new(statement_result_to_native(seq, result)))
}

/// Run a statement in an implicit transaction: it commits when every task
/// succeeds, and rolls back when one fails.
pub(super) async fn run_implicit_statement(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    statement: PlannedStatement<'_>,
) -> SqlOutcome {
    let PlannedStatement {
        tasks,
        output_schema,
        database_id,
        plan_lease_scope,
        sum_target_reads,
    } = statement;
    let sequences = session_sequences(ctx, database_id);
    let exec = StatementExec {
        state: ctx.state,
        identity: ctx.identity,
        scope: &ctx.scope,
        output_schema,
        database_id,
        sequences: &sequences,
        client_session: Some(DmlTxnCtx {
            sessions: ctx.sessions,
            session_id: ctx.peer_addr.into(),
        }),
    };
    let client = DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    let result = statement_exec::run_implicit_statement(
        &exec,
        statement_exec::PlannedStatement {
            tasks,
            plan_lease_scope,
            sum_target_reads,
        },
        &client,
    )
    .await;
    SqlOutcome::Response(Box::new(statement_result_to_native(seq, result)))
}
