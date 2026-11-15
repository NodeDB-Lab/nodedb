// SPDX-License-Identifier: BUSL-1.1

//! Which route one planned statement takes, decided once for every transport.
//!
//! A transport runs its own dispatch and renders its own answer. It asks here
//! which dispatch to run, so the same statement takes the same route over
//! pgwire, native, HTTP, WebSocket RPC and NDJSON.
//!
//! The decision comes in two parts, because edge reconciliation sits between
//! them and can answer the statement itself:
//!
//! 1. [`implicit_txn_route`]: whether the statement runs in an implicit
//!    transaction.
//! 2. [`autocommit_route`]: for a statement that does not, whether its tasks
//!    go to the Calvin sequencer or to the caller's per-task loop.
//!
//! [`statement_needs_implicit_txn`] weighs the position of each task inside
//! the task set it is given. A request of several statements plans each
//! statement alone and joins the tasks. A read of the rows an earlier
//! statement of the same request wrote leaves no trace in that set, so
//! neither part sees it.

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, classify_dispatch, read_vshards_of,
};
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::server::shared::txn_route::{
    statement_needs_implicit_txn, unbufferable_joined_statement,
};
use crate::control::server::shared::write_admission::all_writes_bufferable;
use crate::control::state::SharedState;
use crate::control::trigger::statement_txn::in_block;

/// Whether a statement runs in an implicit transaction.
pub(crate) enum ImplicitTxnRoute {
    /// The statement's writes stage on their vShards' leaders and commit
    /// together at its end, through Calvin when they span vShards.
    Implicit,
    /// No implicit transaction applies: the session is in a block, or no
    /// predicate flags the statement. The caller continues to the autocommit
    /// routes.
    Autocommit,
    /// The statement needs an implicit transaction and the staging gate cannot
    /// buffer every write, or the predicate itself returned an error.
    Refuse(crate::Error),
}

/// Where an autocommit statement's tasks go, once edge reconciliation has
/// declined the statement.
pub(crate) enum AutocommitRoute {
    /// The caller runs its per-task loop, authorizing each task immediately
    /// before its own dispatch.
    PerTask,
    /// A statement on one vShard outside a block that holds settlement reads,
    /// such as a zero-net settlement. It ships no task the per-task loop can
    /// check a read on, so it commits through the Calvin sequencer.
    SettledCalvin,
    /// The tasks span vShards outside a block in `Strict` mode. They commit
    /// through the Calvin sequencer, whose coordinator answers the statement.
    Calvin,
    /// The statement cannot take either route: a multi-vShard statement inside
    /// a block whose writes the staging gate cannot buffer, or a statement
    /// whose read vShards cannot be enumerated.
    Refuse(crate::Error),
}

/// Whether `tasks`, planned on `session` for one request, run in an implicit
/// transaction.
///
/// A statement already inside a block joins that block. Outside one,
/// [`statement_needs_implicit_txn`] decides, and the statement is refused when
/// the staging gate cannot buffer its writes.
pub(crate) fn implicit_txn_route(
    state: &SharedState,
    tasks: &[PhysicalTask],
    session: &DmlTxnCtx<'_>,
) -> ImplicitTxnRoute {
    if in_block(session) {
        return ImplicitTxnRoute::Autocommit;
    }
    match statement_needs_implicit_txn(state, tasks) {
        Ok(true) => {}
        Ok(false) => return ImplicitTxnRoute::Autocommit,
        Err(error) => return ImplicitTxnRoute::Refuse(error),
    }
    if !all_writes_bufferable(tasks) {
        return ImplicitTxnRoute::Refuse(unbufferable_joined_statement());
    }
    ImplicitTxnRoute::Implicit
}

/// Where an autocommit statement's tasks go.
///
/// The read vShards widen the class, so a write to one vShard that reads
/// another commits through Calvin with both as participants. Only the
/// materialized-sum settlement stamps reads on an autocommit statement: there
/// is no session read set outside a block.
pub(crate) fn autocommit_route(
    tasks: &[PhysicalTask],
    sum_target_reads: &[ReadSetEntry],
    session: &DmlTxnCtx<'_>,
) -> AutocommitRoute {
    let sum_read_vshards = match read_vshards_of(sum_target_reads) {
        Ok(vshards) => vshards,
        Err(error) => return AutocommitRoute::Refuse(error),
    };
    let in_txn_block = in_block(session);
    match classify_dispatch(tasks, &sum_read_vshards) {
        // Only Calvin checks the reads of a settlement that ships no task.
        DispatchClass::SingleShard { .. } if !in_txn_block && !sum_target_reads.is_empty() => {
            AutocommitRoute::SettledCalvin
        }
        // One vShard is one Raft group, so the per-task loop applies the
        // statement deterministically. Edge-bearing dependent predicates were
        // preempted before this point.
        DispatchClass::SingleShard { .. } => AutocommitRoute::PerTask,
        DispatchClass::MultiShard { .. } if in_txn_block => {
            // The per-task loop buffers each write and COMMIT flushes the
            // buffer through Calvin. A write the gate cannot buffer can take
            // neither route.
            if all_writes_bufferable(tasks) {
                AutocommitRoute::PerTask
            } else {
                AutocommitRoute::Refuse(crate::Error::CrossShardInExplicitTransaction)
            }
        }
        DispatchClass::MultiShard { .. } => {
            let mode = session.sessions.cross_shard_txn_mode(session.session_id);
            if mode == CrossShardTxnMode::Strict {
                AutocommitRoute::Calvin
            } else {
                // `BestEffortNonAtomic` dispatches each vShard independently.
                AutocommitRoute::PerTask
            }
        }
    }
}
