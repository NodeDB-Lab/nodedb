// SPDX-License-Identifier: BUSL-1.1

//! A direct write whose collection fires a BEFORE, INSTEAD OF or SYNC AFTER
//! body, in the statement's transaction.
//!
//! The write's tasks run through the SQL path's per-task loop, which routes
//! each one through the shared `txn_route`: the bodies join the transaction,
//! a shadowed clone takes its copy-on-write steps, and the writes stage.
//! Inside a transaction block that is the client's transaction. Outside one
//! it is an implicit transaction that commits when every task succeeds and
//! rolls back when one fails.

use std::sync::Arc;

use nodedb_types::protocol::NativeResponse;

use crate::control::lease::QueryLeaseScope;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::server::shared::statement_exec::StatementMap;
use crate::control::server::shared::txn_route::unbufferable_joined_statement;
use crate::control::server::shared::write_admission::all_writes_bufferable;
use nodedb_physical::physical_task::PhysicalTask;

use super::sql_loop::{PlannedStatement, run_dispatch_loop, run_implicit_statement};
use super::{DispatchCtx, error_to_native};

/// A direct write's tasks and what they carry into the transaction.
pub(super) struct DirectWrite {
    pub tasks: Vec<PhysicalTask>,
    /// The row images the write's cross-shard balances were settled from.
    pub sum_target_reads: Vec<ReadSetEntry>,
    /// The write's descriptor leases, kept on every task it buffers.
    pub lease: Arc<QueryLeaseScope>,
}

/// Run a direct write's tasks in the statement's transaction. The answer is
/// the statement fold the SQL path gives: the write's count and verb.
pub(super) async fn dispatch_direct_in_txn(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    write: DirectWrite,
) -> NativeResponse {
    let DirectWrite {
        tasks,
        sum_target_reads,
        lease,
    } = write;
    // A direct write is one statement.
    let statement = PlannedStatement {
        statement_map: StatementMap::single(tasks.len()),
        tasks,
        // A direct write announces no output columns.
        schemas: &[],
        database_id: ctx.database_id(),
        plan_lease_scope: lease,
        sum_target_reads,
    };
    if ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock {
        let client = DmlTxnCtx {
            sessions: ctx.sessions,
            session_id: ctx.peer_addr.into(),
        };
        return run_dispatch_loop(ctx, seq, statement, Some(&client))
            .await
            .into_response();
    }
    if !all_writes_bufferable(&statement.tasks) {
        return error_to_native(seq, &unbufferable_joined_statement());
    }
    run_implicit_statement(ctx, seq, statement)
        .await
        .into_response()
}
