// SPDX-License-Identifier: BUSL-1.1

//! ClusterArray plan dispatch for the pgwire handler.
//!
//! ClusterArray plans are handled entirely on the Control Plane by the
//! `ArrayCoordinator` — they must never reach the SPSC bridge or the
//! trigger/DML machinery. `dispatch_task_loop` intercepts them and delegates
//! to the helper here, which shapes the coordinator's payload into a single
//! pgwire `Response` (surfacing any client-facing notice via the session).

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use nodedb_physical::physical_plan::{ClusterArrayOp, PhysicalPlan};

use crate::control::server::dispatch_utils::publish_cluster_array_change_events;
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::session::SessionId;

use super::super::super::types::{error_to_sqlstate, shape_error_to_pg};
use super::super::core::NodeDbPgHandler;
use super::super::plan::{PlanKind, payload_to_response};
use super::super::shape_encode;

impl NodeDbPgHandler {
    /// Execute a single `ClusterArrayOp` via the `ArrayCoordinator` and shape
    /// its payload into one pgwire `Response`. Any carried notice is pushed to
    /// the supplied session.
    ///
    /// On a successful `Put`/`Delete` (writes; `Slice`/`Agg` are reads and
    /// publish nothing), publishes a CDC change event keyed by the op's own
    /// `wal_lsn` — this path never touches the SPSC bridge, so there is no
    /// Data-Plane `Response::watermark_lsn` to read the LSN from the way the
    /// normal dispatch funnel does (see `publish_cluster_array_change_events`'s
    /// own doc comment).
    pub(super) async fn dispatch_cluster_array_task(
        &self,
        authorized: crate::control::server::shared::authorization::AuthorizedTask,
        projection: Option<&OutputSchema>,
        result_formats: &[FieldFormat],
        session_id: SessionId,
        auth: &crate::control::security::auth_context::AuthContext,
    ) -> PgWireResult<Response> {
        use crate::control::cluster::ClusterArrayExecutor;
        use std::sync::Arc;

        let task = authorized.into_physical_task();
        let tenant_id = task.tenant_id;
        let database_id = task.database_id;
        let PhysicalPlan::ClusterArray(cluster_op) = task.plan else {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "authorized task is not a ClusterArray operation".to_owned(),
            ))));
        };

        let transport = self.state.cluster_transport.as_ref().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "cluster transport not available for ClusterArray dispatch".to_owned(),
            )))
        })?;
        let routing = self.state.cluster_routing.as_ref().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "cluster routing not available for ClusterArray dispatch".to_owned(),
            )))
        })?;
        let executor = ClusterArrayExecutor::new(
            Arc::clone(transport),
            Arc::clone(routing),
            self.state.node_id,
            Arc::clone(&self.state),
        );
        let payload_bytes = executor.execute(&cluster_op).await.map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;
        // Publish CDC change event(s) for a successful write. `Slice`/`Agg`
        // are reads and publish nothing; `Put`/`Delete` carry their own
        // Control-Plane-allocated `wal_lsn` since there is no Data-Plane
        // `Response::watermark_lsn` on this coordinator-only path.
        let write_lsn = match &cluster_op {
            ClusterArrayOp::Put { wal_lsn, .. } | ClusterArrayOp::Delete { wal_lsn, .. } => {
                Some(*wal_lsn)
            }
            ClusterArrayOp::Slice { .. } | ClusterArrayOp::Agg { .. } => None,
        };
        if let Some(lsn) = write_lsn {
            publish_cluster_array_change_events(
                &self.state,
                tenant_id,
                database_id,
                &cluster_op,
                lsn,
            );
        }

        let cluster_plan_kind = match &cluster_op {
            ClusterArrayOp::Slice { .. } => PlanKind::ArraySlice,
            ClusterArrayOp::Agg { .. }
            | ClusterArrayOp::Put { .. }
            | ClusterArrayOp::Delete { .. } => PlanKind::MultiRow,
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
            Some(redaction.ctx(&self.state.redaction)),
            None,
        )
        .map_err(|e| shape_error_to_pg(&e))?
        {
            ShapeOutcome::Rows(shaped) => {
                let (response, notice) =
                    shape_encode::shaped_query_response(shaped, result_formats);
                if let Some(n) = notice {
                    self.sessions.push_notice(session_id, n);
                }
                Ok(response)
            }
            ShapeOutcome::Passthrough => {
                let shaped = payload_to_response(&payload_bytes, cluster_plan_kind)?;
                if let Some(notice) = shaped.notice {
                    self.sessions.push_notice(session_id, notice);
                }
                Ok(shaped.response)
            }
        }
    }
}
