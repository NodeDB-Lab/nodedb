// SPDX-License-Identifier: BUSL-1.1

//! How an autocommit statement of several writes commits them together.
//!
//! One statement is atomic, as in PostgreSQL: when one of its writes fails,
//! none of them is stored. A statement that plans several write tasks (a
//! multi-key `UPDATE` or `DELETE`, a CRDT multi-row `INSERT`, a row and its
//! implicit edge) must not apply them one task at a time. Each per-task
//! dispatch is its own commit, so an error in a later task leaves the
//! earlier ones stored.
//!
//! - A set Calvin sequences commits as one Calvin transaction.
//! - A set that writes a clone still reading from its source, or that Calvin
//!   does not sequence, runs in an implicit transaction: the clone takes its
//!   copy-on-write steps there, which a Calvin participant does not take.
//! - A multi-shard set under the opt-in `best_effort_non_atomic` mode keeps
//!   the per-task route the session asked for.
//!
//! A statement of one write keeps the direct path.

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::{CloneStatus, CollectionKey};

use crate::control::planner::calvin::single_vshard_route::holds_several_writes;
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, classify_dispatch, is_write_plan,
};
use crate::control::server::shared::plan_util::extract_collection;
use crate::control::server::shared::write_admission::all_writes_bufferable;
use crate::control::state::SharedState;

use super::fallible_returning::calvin_sequences;

/// The route that commits an autocommit statement's several writes as one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeveralWrites {
    /// The writes commit as one Calvin transaction.
    Calvin,
    /// The writes stage in an implicit transaction and commit at its end.
    ImplicitTxn,
}

/// The route that commits `tasks`, one autocommit statement, as one. `None`
/// when the statement holds at most one write, or when a multi-shard set
/// runs under `best_effort_non_atomic`, or when no atomic route can carry
/// the set.
pub(crate) fn several_writes_route(
    state: &SharedState,
    tasks: &[PhysicalTask],
    cross_shard_mode: CrossShardTxnMode,
) -> Option<SeveralWrites> {
    if !holds_several_writes(tasks) {
        return None;
    }
    if cross_shard_mode == CrossShardTxnMode::BestEffortNonAtomic
        && matches!(
            classify_dispatch(tasks, &Default::default()),
            DispatchClass::MultiShard { .. }
        )
    {
        return None;
    }
    if calvin_sequences(tasks) && !writes_unmaterialized_clone(state, tasks) {
        return Some(SeveralWrites::Calvin);
    }
    all_writes_bufferable(tasks).then_some(SeveralWrites::ImplicitTxn)
}

/// Whether a write of `tasks` targets a clone that still reads from its
/// source. A collection the catalog cannot read counts as one, which only
/// sends the statement to the implicit transaction.
fn writes_unmaterialized_clone(state: &SharedState, tasks: &[PhysicalTask]) -> bool {
    let catalog = state.credentials.catalog();
    tasks
        .iter()
        .filter(|task| is_write_plan(&task.plan))
        .any(|task| {
            let Some(qualified) = extract_collection(&task.plan) else {
                return false;
            };
            let Ok(key) = CollectionKey::from_relation_name(task.database_id, qualified) else {
                return true;
            };
            match catalog.get_collection(task.database_id, task.tenant_id.as_u64(), key.name()) {
                Ok(Some(stored)) => {
                    stored.cloned_from.is_some()
                        && !matches!(stored.clone_status, CloneStatus::Materialized)
                }
                Ok(None) => false,
                Err(_) => true,
            }
        })
}
