// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `ClusterArray` plan dispatch, shared by pgwire and native.
//!
//! `ClusterArrayOp` plans are handled entirely on the Control Plane by the
//! `ArrayCoordinator` — they must never reach the SPSC bridge or the
//! trigger/DML machinery. Each protocol's dispatch loop intercepts a
//! `PhysicalPlan::ClusterArray` task right after its own in-transaction
//! routing gate, calls [`execute_cluster_array`], then renders the
//! [`ClusterArrayShaped`] outcome in its own wire format. pgwire's adapter
//! lives in `pgwire::handler::routing::cluster_array`; native's lives in
//! `native::dispatch::cluster_array`.

use std::sync::Arc;

use nodedb_physical::physical_plan::{ClusterArrayOp, PhysicalPlan};

use crate::control::cluster::ClusterArrayExecutor;
use crate::control::security::auth_context::AuthContext;
use crate::control::server::dispatch_utils::publish_cluster_array_change_events;
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{DmlOutcome, PlanKind, ShapedRows};
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

/// Execute one authorized `ClusterArrayOp` via the `ArrayCoordinator` and
/// shape its payload into protocol-neutral rows or a count-bearing outcome.
///
/// On a successful `Put`/`Delete` (writes; `Slice`/`Agg` are reads and
/// publish nothing), publishes a CDC change event keyed by the op's own
/// `wal_lsn` — this path never touches the SPSC bridge, so there is no
/// Data-Plane `Response::watermark_lsn` to read the LSN from the way the
/// normal dispatch funnel does (see `publish_cluster_array_change_events`'s
/// own doc comment).
pub(crate) async fn execute_cluster_array(
    state: &Arc<SharedState>,
    auth: &AuthContext,
    authorized: AuthorizedTask,
    projection: Option<&OutputSchema>,
) -> crate::Result<ClusterArrayShaped> {
    // Read before the task is consumed: an in-transaction `Slice`/`Agg`
    // carries the session's transaction id, and each shard folds that
    // transaction's staged cells into its result.
    let txn_id = authorized.txn_id();
    let task = authorized.into_physical_task();
    let tenant_id = task.tenant_id;
    let database_id = task.database_id;
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
    let payload_bytes = executor.execute(&cluster_op, txn_id).await?;

    // Publish CDC change event(s) for a successful write. `Slice`/`Agg` are
    // reads and publish nothing; `Put`/`Delete` carry their own
    // Control-Plane-allocated `wal_lsn` since there is no Data-Plane
    // `Response::watermark_lsn` on this coordinator-only path.
    let write_lsn = match &cluster_op {
        ClusterArrayOp::Put { wal_lsn, .. } | ClusterArrayOp::Delete { wal_lsn, .. } => {
            Some(*wal_lsn)
        }
        ClusterArrayOp::Slice { .. } | ClusterArrayOp::Agg { .. } => None,
    };
    if let Some(lsn) = write_lsn {
        publish_cluster_array_change_events(state, tenant_id, database_id, &cluster_op, lsn);
    }

    let cluster_plan_kind = match &cluster_op {
        ClusterArrayOp::Slice { .. } => PlanKind::ArraySlice,
        ClusterArrayOp::Agg { .. } => PlanKind::MultiRow,
        // The coordinator reports `{"inserted": n}` / `{"deleted": n}`, the
        // same count map the local array handlers emit.
        ClusterArrayOp::Put { .. } => PlanKind::DmlResult("INSERT"),
        ClusterArrayOp::Delete { .. } => PlanKind::DmlResult("DELETE"),
    };
    // This coordinator path never builds a `PhysicalPlan`, so the source
    // collection comes straight off the op's array name. A single source
    // means bare-key matching, which is what an array's cell rows carry.
    let array_name = match &cluster_op {
        ClusterArrayOp::Slice { array_id, .. }
        | ClusterArrayOp::Agg { array_id, .. }
        | ClusterArrayOp::Put { array_id, .. }
        | ClusterArrayOp::Delete { array_id, .. } => array_id.name.clone(),
    };
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
