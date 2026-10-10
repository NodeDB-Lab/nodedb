// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `ClusterArray` plan dispatch, shared by every transport.
//!
//! `ClusterArrayOp` plans are handled entirely on the Control Plane by the
//! `ArrayCoordinator`. They never reach the gateway, the SPSC bridge or the
//! trigger/DML machinery. Each transport's dispatch loop intercepts a
//! `PhysicalPlan::ClusterArray` task after authorization:
//!
//! - pgwire, native and the shared statement loop
//!   (`shared::statement_exec`) call [`execute_cluster_array`] and render the
//!   [`ClusterArrayShaped`] outcome in their own shape. The protocol
//!   adapters live in `pgwire::handler::routing::cluster_array` and
//!   `native::dispatch::cluster_array`.
//! - HTTP and WebSocket RPC call [`run_cluster_array`] and shape the raw
//!   payload the way they shape a gateway payload.

use std::sync::Arc;

use nodedb_physical::physical_plan::{ClusterArrayOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::cluster::ClusterArrayExecutor;
use crate::control::security::auth_context::AuthContext;
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{
    DmlOutcome, PlanKind, ShapedRows, describe_plan,
};
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::server::shared::sql::staging_predicates::require_affected_count;
use crate::control::state::SharedState;

/// What one `ClusterArrayOp` answers with, before any protocol renders it.
pub(crate) enum ClusterArrayShaped {
    /// A read's rows (`Slice` / `Agg`). Any carried client-facing notice
    /// lives on `ShapedRows::notice`.
    Rows(ShapedRows),
    /// A write's count (`Put` / `Delete`), with the verb the tag names it.
    Affected(DmlOutcome),
}

/// Whether `plan` is a `ClusterArray` plan. Every transport tests this after
/// authorization and runs a match through [`run_cluster_array`] or
/// [`execute_cluster_array`], never through the gateway.
pub(crate) fn is_cluster_array(plan: &PhysicalPlan) -> bool {
    matches!(plan, PhysicalPlan::ClusterArray(_))
}

/// Execute one authorized `ClusterArrayOp` via the `ArrayCoordinator` and
/// return its raw payload.
///
/// The payload has the shape the local `ArrayOp` counterpart answers with,
/// so a transport shapes it with `describe_plan` of the task's plan, exactly
/// as it shapes a gateway payload.
///
/// A `Put`/`Delete` publishes no change event here: each shard's committed
/// array write publishes as its replicas apply it.
pub(crate) async fn run_cluster_array(
    state: &Arc<SharedState>,
    authorized: AuthorizedTask,
) -> crate::Result<Vec<u8>> {
    run_trusted_cluster_array(state, authorized.into_physical_task()).await
}

/// [`run_cluster_array`] for a task whose authority comes from an already
/// admitted operation, such as a read inside a trigger or procedure body's
/// system transaction.
pub(crate) async fn run_trusted_cluster_array(
    state: &Arc<SharedState>,
    task: PhysicalTask,
) -> crate::Result<Vec<u8>> {
    // An in-transaction `Slice`/`Agg` carries the transaction id, and each
    // shard folds that transaction's staged cells into its result.
    let txn_id = task.txn_id;
    let PhysicalPlan::ClusterArray(cluster_op) = task.plan else {
        return Err(crate::Error::Internal {
            detail: "authorized task is not a ClusterArray operation".to_owned(),
        });
    };

    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| crate::Error::Internal {
            detail: "cluster transport not available for ClusterArray dispatch".to_owned(),
        })?;
    let routing = state
        .cluster_routing
        .as_ref()
        .ok_or_else(|| crate::Error::Internal {
            detail: "cluster routing not available for ClusterArray dispatch".to_owned(),
        })?;
    let executor = ClusterArrayExecutor::new(
        Arc::clone(transport),
        Arc::clone(routing),
        state.node_id,
        Arc::clone(state),
    );
    let op = match &cluster_op {
        ClusterArrayOp::Slice { .. } => "slice",
        ClusterArrayOp::Agg { .. } => "agg",
        ClusterArrayOp::Put { .. } => "put",
        ClusterArrayOp::Delete { .. } => "delete",
    };
    tracing::debug!(
        op,
        ?txn_id,
        array = %cluster_op.array_id().name,
        "cluster array dispatch"
    );
    executor.execute(&cluster_op, txn_id).await
}

/// Execute one authorized `ClusterArrayOp` through [`run_cluster_array`] and
/// shape its payload into protocol-neutral rows or a count-bearing outcome.
pub(crate) async fn execute_cluster_array(
    state: &Arc<SharedState>,
    auth: &AuthContext,
    authorized: AuthorizedTask,
    projection: Option<&OutputSchema>,
) -> crate::Result<ClusterArrayShaped> {
    let tenant_id = authorized.tenant_id();
    let cluster_plan_kind = describe_plan(authorized.plan());
    // The coordinator path builds no `PhysicalPlan` per shard, so the source
    // collection comes straight off the op's array name. A single source
    // means bare-key matching, which is what an array's cell rows carry.
    let PhysicalPlan::ClusterArray(cluster_op) = authorized.plan() else {
        return Err(crate::Error::Internal {
            detail: "authorized task is not a ClusterArray operation".to_owned(),
        });
    };
    let array_name = cluster_op.array_id().name.clone();
    let payload_bytes = run_cluster_array(state, authorized).await?;
    let redaction =
        QueryRedaction::for_collections(tenant_id, auth, vec![(String::new(), array_name)]);
    // A cluster array plan projects attribute names only, never a
    // Control-Plane computed column, so no session sequence access.
    match compose::shape_payload_no_plan(
        &payload_bytes,
        cluster_plan_kind,
        projection,
        Some(redaction.ctx(&state.redaction)),
        None,
    )? {
        ShapeOutcome::Rows(shaped) => Ok(ClusterArrayShaped::Rows(shaped)),
        ShapeOutcome::Passthrough => match cluster_plan_kind {
            PlanKind::DmlResult(verb) => {
                let affected = require_affected_count(&payload_bytes)?;
                Ok(ClusterArrayShaped::Affected(DmlOutcome { verb, affected }))
            }
            // `cluster_plan_kind` above is only ever `ArraySlice`, `MultiRow`
            // or `DmlResult(_)`; the remaining `PlanKind` variants can never
            // reach this arm. Kept exhaustive (no `_ =>`) so a future
            // `PlanKind` desync surfaces as a typed error rather than a panic.
            PlanKind::DmlResultByOp
            | PlanKind::Execution
            | PlanKind::ArraySlice
            | PlanKind::ReturningRows
            | PlanKind::SingleDocument
            | PlanKind::MultiRow => Err(crate::Error::Internal {
                detail: format!(
                    "ClusterArray dispatch produced an unreachable passthrough plan kind: \
                     {cluster_plan_kind:?}"
                ),
            }),
        },
    }
}
