// SPDX-License-Identifier: BUSL-1.1

//! What a write resolves on its proposer before its record exists.
//!
//! A record is a Raft entry, a sequenced Calvin transaction, or a WAL record.
//! Every install applies it, so every value an install would compute
//! differently is computed here, once: a volatile DEFAULT binds into the
//! body, and a timeseries ingest resolves to its rows.

use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;

use super::resolver::WriteResolveContext;
use super::timeseries::{resolve_ingest_for_log, resolve_ingest_tasks_for_log};
use super::volatile_defaults::bind_volatile_defaults;

/// The plan a record carries in place of `plan`, or `None` when `plan` goes
/// on the record as it is.
pub(crate) async fn resolve_for_log(
    state: &SharedState,
    ctx: WriteResolveContext,
    vshard_id: crate::types::VShardId,
    plan: &PhysicalPlan,
) -> crate::Result<Option<PhysicalPlan>> {
    let bound = bind_volatile_defaults(state, ctx.tenant_id, ctx.database_id, plan)?;
    let current = bound.as_ref().unwrap_or(plan);
    match resolve_ingest_for_log(state, ctx, vshard_id, current).await? {
        Some(resolved) => Ok(Some(resolved)),
        None => Ok(bound),
    }
}

/// `tasks` as a sequenced transaction carries them, or `None` when every
/// task goes on it as it is. Every replica applies a sequenced transaction on
/// its own, so the submitting node resolves each task first.
pub(crate) async fn resolve_tasks_for_log(
    state: &SharedState,
    tasks: &[PhysicalTask],
) -> crate::Result<Option<Vec<PhysicalTask>>> {
    let mut bound: Option<Vec<PhysicalTask>> = None;
    for (index, task) in tasks.iter().enumerate() {
        let Some(plan) =
            bind_volatile_defaults(state, task.tenant_id, task.database_id, &task.plan)?
        else {
            continue;
        };
        let all = bound.get_or_insert_with(|| tasks.to_vec());
        if let Some(slot) = all.get_mut(index) {
            slot.plan = plan;
        }
    }
    let current = bound.as_deref().unwrap_or(tasks);
    match resolve_ingest_tasks_for_log(state, current).await? {
        Some(resolved) => Ok(Some(resolved)),
        None => Ok(bound),
    }
}
