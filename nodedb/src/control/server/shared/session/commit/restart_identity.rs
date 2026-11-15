// SPDX-License-Identifier: BUSL-1.1

//! `TRUNCATE ... RESTART IDENTITY` after a Calvin commit.
//!
//! Inside a transaction the truncate is staged, so its sequences restart only
//! once COMMIT has made the truncate durable. A `ROLLBACK` leaves every
//! sequence where it was. The reconnaissance route restarts them once its
//! transaction has committed (`planner::calvin::edge_truncate`). Autocommit
//! restarts them right after dispatch instead
//! (`pgwire::handler::routing::dispatch_loop`, `http::routes::query::materialized::shape`,
//! `http::routes::query::ndjson`, `http::routes::ws_rpc::execute_sql`).

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::gateway::core::QueryContext;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// Reset the sequences of every collection a buffered
/// `TRUNCATE ... RESTART IDENTITY` names, whichever engine stores it
/// (`PhysicalPlan::truncate_target`). Called after every abort branch of
/// the commit has returned.
///
/// The commit already holds the truncate, so an error here cannot fail it. The
/// report names it instead. A counter that stayed ahead repeats no value.
pub(crate) fn restart_truncated_identities(
    state: &SharedState,
    tenant_id: TenantId,
    buffered: &[PhysicalTask],
) {
    for task in buffered {
        restart_truncated_identity(state, task.database_id, tenant_id, &task.plan);
    }
}

/// Reset the sequences of the collection `plan` truncates with `RESTART
/// IDENTITY`. A plan that is not such a truncate restarts nothing. An error is
/// reported, not returned: the truncate is already durable.
pub(crate) fn restart_truncated_identity(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: &PhysicalPlan,
) {
    if let Some((collection, true)) = plan.truncate_target()
        && let Err(error) = state.sequence_registry.restart_sequences_for_collection(
            database_id.as_u64(),
            tenant_id.as_u64(),
            collection.as_str(),
        )
    {
        crate::diag::sequence_restart_failed(
            &error.to_string(),
            database_id.as_u64(),
            tenant_id.as_u64(),
            collection.as_str(),
        );
    }
}

/// The plan to hand to [`restart_after_ok`]: a clone of `plan` when it is a
/// `TRUNCATE ... RESTART IDENTITY`, `None` otherwise. A dispatch that consumes
/// the plan holds this copy.
pub(crate) fn restart_plan_of(plan: &PhysicalPlan) -> Option<PhysicalPlan> {
    matches!(plan.truncate_target(), Some((_, true))).then(|| plan.clone())
}

/// Restart the sequences of `plan` once its dispatch under `ctx` answered
/// `Ok`. A `None` plan or an `Err` answer restarts nothing.
pub(crate) fn restart_after_ok<T>(
    state: &SharedState,
    ctx: &QueryContext,
    plan: Option<&PhysicalPlan>,
    answer: &crate::Result<T>,
) {
    if let (Some(plan), Ok(_)) = (plan, answer) {
        restart_truncated_identity(state, ctx.database_id, ctx.tenant_id, plan);
    }
}
