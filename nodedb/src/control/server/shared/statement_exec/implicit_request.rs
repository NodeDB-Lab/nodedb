// SPDX-License-Identifier: BUSL-1.1

//! Which requests of several statements run whole in an implicit transaction.
//!
//! pgwire runs a simple query of several statements in one implicit block.
//! HTTP, NDJSON, WebSocket RPC and native plan such a request as one task
//! set. A set whose writes all stage runs in one implicit transaction too, so
//! an error in a later statement undoes the writes before it. pgwire plans
//! each statement alone and never reaches this hook.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::txn_route::statement_needs_implicit_txn;
use crate::control::server::shared::write_admission::{all_writes_bufferable, plan_is_write};
use crate::control::state::SharedState;
use crate::control::trigger::statement_txn::in_block;

use super::answer::{INSERTED_KEY, RequestAnswer};
use super::route_decision::{ImplicitTxnRoute, implicit_txn_route};
use super::statement_map::StatementMap;

/// Whether `tasks`, planned on `session` for one request, run in an implicit
/// transaction: always for a request [`request_runs_whole_in_txn`] accepts,
/// else as [`implicit_txn_route`] decides.
///
/// The second value names the statements whose count answers under
/// `inserted`. See [`InsertedCounts`].
pub(super) fn request_txn_route(
    state: &SharedState,
    tasks: &[PhysicalTask],
    statement_map: &StatementMap,
    session: &DmlTxnCtx<'_>,
) -> (ImplicitTxnRoute, InsertedCounts) {
    if !request_runs_whole_in_txn(tasks, statement_map, session) {
        return (
            implicit_txn_route(state, tasks, session),
            InsertedCounts::default(),
        );
    }
    let flagged = statement_needs_implicit_txn(state, tasks).unwrap_or(true);
    let inserted = if flagged {
        InsertedCounts::default()
    } else {
        InsertedCounts::of(tasks, statement_map)
    };
    (ImplicitTxnRoute::Implicit, inserted)
}

/// The statements of a request whose count answers `{"inserted": n}`.
///
/// A batch insert or `INSERT .. SELECT` answers `inserted` on the per-task
/// route. A request only this hook moved into the transaction keeps that
/// key. A request a statement flagged for the transaction, such as a
/// cross-shard sum move, keeps the `affected` of that route.
#[derive(Debug, Default)]
pub(super) struct InsertedCounts(BTreeSet<usize>);

impl InsertedCounts {
    /// The statements of `tasks` that plan a batch insert.
    fn of(tasks: &[PhysicalTask], statement_map: &StatementMap) -> Self {
        Self(
            tasks
                .iter()
                .enumerate()
                .filter(|(_, task)| {
                    matches!(
                        &task.plan,
                        PhysicalPlan::Document(
                            DocumentOp::InsertSelect { .. } | DocumentOp::BatchInsert { .. }
                        )
                    )
                })
                .filter_map(|(index, _)| statement_map.statement_of(index))
                .map(|statement| statement as usize)
                .collect(),
        )
    }

    /// Mark the count key of each named statement of `answer`.
    pub(super) fn apply(&self, answer: &mut RequestAnswer) {
        for &statement in &self.0 {
            if let Some(part) = answer.statements.get_mut(statement) {
                part.count_key = INSERTED_KEY;
            }
        }
    }
}

/// Whether a request runs whole in an implicit transaction that no statement
/// of it asked for: it is outside a transaction block, holds two or more
/// statements and a write, and [`stages_whole`] accepts its tasks.
pub(super) fn request_runs_whole_in_txn(
    tasks: &[PhysicalTask],
    statement_map: &StatementMap,
    session: &DmlTxnCtx<'_>,
) -> bool {
    !in_block(session)
        && statement_map.statement_count() >= 2
        && tasks.iter().any(|task| plan_is_write(&task.plan))
        && stages_whole(tasks)
}

/// Whether every write of `tasks` stages, and none is a TRUNCATE or a CRDT
/// document delete.
///
/// A staged TRUNCATE has no row count, so it cannot answer
/// `{"truncated": n}` as the Data Plane does. A CRDT document delete needs
/// its edge-maintenance recon, which the implicit transaction does not run.
/// A request with either keeps its other routes and their gates.
fn stages_whole(tasks: &[PhysicalTask]) -> bool {
    use crate::control::planner::calvin::dependent_recon_crdt::is_crdt_doc_delete;
    use crate::control::planner::calvin::edge_truncate::is_truncate;
    all_writes_bufferable(tasks)
        && !tasks
            .iter()
            .any(|task| is_truncate(&task.plan) || is_crdt_doc_delete(&task.plan))
}
