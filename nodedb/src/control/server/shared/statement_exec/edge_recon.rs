// SPDX-License-Identifier: BUSL-1.1

//! The implicit-edge OLLP/Calvin gate.
//!
//! A dependent predicate (`BulkDelete`/`BulkUpdate`) on an edge-bearing
//! schemaless collection routes through the OLLP/Calvin coordinator, so the
//! implicit edge tasks derive from a pre-exec reconnaissance scan and commit
//! atomically with the document write. Without this gate such a delete
//! classifies as `SingleShard`, dispatches directly to the Data Plane, and
//! leaves its mirrored CSR edges dangling.

use std::sync::Arc;

use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::planner::calvin::{
    dispatch_authorized_dependent_edge_recon, plan_needs_implicit_edge_recon,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::authorization::AuthorizedTaskSet;
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
pub(crate) async fn try_edge_recon(
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    in_txn_block: bool,
    tasks: Vec<PhysicalTask>,
    authorized: AuthorizedTaskSet,
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
    // Captured before the coordinator consumes the tasks, so the answer can
    // shape a RETURNING dependent write's rows and read a plain write's
    // count from the applied response.
    let plans: Vec<PhysicalPlan> = tasks.iter().map(|task| task.plan.clone()).collect();
    let recon = dispatch_authorized_dependent_edge_recon(
        state,
        authorized,
        identity,
        tenant_id,
        database_id,
    )
    .await?;
    Ok(EdgeRecon::Applied(CalvinApplied {
        apply_result: recon.apply_result,
        plans,
        database_id,
    }))
}
