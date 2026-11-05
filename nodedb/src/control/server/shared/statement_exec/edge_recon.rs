// SPDX-License-Identifier: BUSL-1.1

//! The implicit-edge OLLP/Calvin gate.
//!
//! A dependent predicate (`BulkDelete`/`BulkUpdate`) on an edge-bearing
//! schemaless collection routes through the OLLP/Calvin coordinator, so the
//! implicit edge tasks derive from a pre-exec reconnaissance scan and commit
//! atomically with the document write. Without this gate such a delete
//! classifies as `SingleShard`, dispatches directly to the Data Plane, and
//! leaves its mirrored CSR edges dangling.
//!
//! A recon plan on one vShard that holds settlement reads takes the same
//! coordinator, so the reads guard its write.

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::planner::calvin::{
    DispatchClass, classify_dispatch, dispatch_authorized_dependent_edge_recon, is_edge_recon_plan,
    plan_needs_implicit_edge_recon, read_vshards_of,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::authorization::AuthorizedTaskSet;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// A statement Calvin applied as one transaction, before any protocol
/// renders its answer.
pub(crate) struct CalvinApplied {
    /// The applied response Calvin deposited, when a task deposits one.
    pub(crate) apply_result: Option<Response>,
    /// Every task's plan, in dispatch order.
    pub(crate) plans: Vec<PhysicalPlan>,
    /// The database the answer shapes against.
    pub(crate) database_id: DatabaseId,
}

/// What the gate did with a statement.
pub(crate) enum EdgeRecon {
    /// The gate did not fire. The caller gets the tasks back.
    NotFired(Vec<PhysicalTask>, AuthorizedTaskSet),
    /// The coordinator applied the statement.
    Applied(CalvinApplied),
}

/// Route `tasks` through the implicit-edge OLLP/Calvin coordinator when one
/// of them is a `BulkDelete`/`BulkUpdate` on an edge-bearing collection and
/// the statement runs outside a transaction block.
///
/// Inside a block the gate does not fire: a multi-step OLLP inside the
/// client's transaction needs a two-phase commit across its boundary. A
/// catalog I/O error propagates. Misrouting on a real fault skips the edge
/// cleanup.
///
/// `reads` are the images materialized-sum settlement folded the tasks' balance
/// deltas from. The coordinator carries them into its transaction.
pub(crate) async fn try_edge_recon(
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    in_txn_block: bool,
    tasks: Vec<PhysicalTask>,
    authorized: AuthorizedTaskSet,
    reads: &[ReadSetEntry],
) -> crate::Result<EdgeRecon> {
    if in_txn_block {
        return Ok(EdgeRecon::NotFired(tasks, authorized));
    }
    let tenant_id: TenantId = identity.tenant_id;
    let Some((_collection, database_id)) =
        plan_needs_implicit_edge_recon(state, &tasks, tenant_id)?
    else {
        return Ok(EdgeRecon::NotFired(tasks, authorized));
    };
    run_recon(state, identity, tasks, authorized, database_id, reads).await
}

/// Route `tasks` through the OLLP/Calvin coordinator when they hold a recon
/// plan and settlement reads on one vShard, outside a transaction block.
///
/// A zero-net settlement ships no balance task, and the source rows it folded
/// are still owed its guard. The per-task route checks no read, so a source row
/// that moved after settlement leaves the balance unequal to the source sum.
pub(crate) async fn try_settled_recon(
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    in_txn_block: bool,
    tasks: Vec<PhysicalTask>,
    authorized: AuthorizedTaskSet,
    reads: &[ReadSetEntry],
) -> crate::Result<EdgeRecon> {
    if in_txn_block || reads.is_empty() {
        return Ok(EdgeRecon::NotFired(tasks, authorized));
    }
    let read_vshards = read_vshards_of(reads)?;
    if matches!(
        classify_dispatch(&tasks, &read_vshards),
        DispatchClass::MultiShard { .. }
    ) {
        return Ok(EdgeRecon::NotFired(tasks, authorized));
    }
    let Some(database_id) = tasks
        .iter()
        .find(|task| is_edge_recon_plan(&task.plan))
        .map(|task| task.database_id)
    else {
        return Ok(EdgeRecon::NotFired(tasks, authorized));
    };
    run_recon(state, identity, tasks, authorized, database_id, reads).await
}

/// Route `tasks` through the OLLP/Calvin coordinator when one of them is a recon
/// plan and the statement spans several vShards, outside a transaction block.
///
/// The caller classified the statement as multi-shard. A predicate write that
/// reaches Calvin must carry the rows it matched, which only the coordinator
/// reads. A shipped balance puts a single-collection write on two vShards, so
/// the edge-bearing gate does not see it.
///
/// `reads` are the images settlement folded that balance from.
pub(crate) async fn try_multi_shard_predicate_recon(
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    tasks: Vec<PhysicalTask>,
    authorized: AuthorizedTaskSet,
    reads: &[ReadSetEntry],
) -> crate::Result<EdgeRecon> {
    let Some(database_id) = tasks
        .iter()
        .find(|task| is_edge_recon_plan(&task.plan))
        .map(|task| task.database_id)
    else {
        return Ok(EdgeRecon::NotFired(tasks, authorized));
    };
    run_recon(state, identity, tasks, authorized, database_id, reads).await
}

async fn run_recon(
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    tasks: Vec<PhysicalTask>,
    authorized: AuthorizedTaskSet,
    database_id: DatabaseId,
    reads: &[ReadSetEntry],
) -> crate::Result<EdgeRecon> {
    // Captured before the coordinator consumes the tasks, so the answer can
    // shape a RETURNING dependent write's rows and read a plain write's
    // count from the applied response.
    let plans: Vec<PhysicalPlan> = tasks.iter().map(|task| task.plan.clone()).collect();
    let recon = dispatch_authorized_dependent_edge_recon(
        state,
        authorized,
        identity,
        identity.tenant_id,
        database_id,
        reads,
    )
    .await?;
    Ok(EdgeRecon::Applied(CalvinApplied {
        apply_result: recon.apply_result,
        plans,
        database_id,
    }))
}
