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

use nodedb_physical::physical_plan::ClusterArrayOp;

use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::schema::OutputSchema;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;
use super::super::plan::{PlanKind, payload_to_response};
use super::super::shape_encode;

impl NodeDbPgHandler {
    /// Execute a single `ClusterArrayOp` via the `ArrayCoordinator` and shape
    /// its payload into one pgwire `Response`. Any carried notice is pushed to
    /// the session for `addr`.
    pub(super) async fn dispatch_cluster_array_task(
        &self,
        cluster_op: &ClusterArrayOp,
        projection: Option<&OutputSchema>,
        result_formats: &[FieldFormat],
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Response> {
        use crate::control::cluster::ClusterArrayExecutor;
        use std::sync::Arc;

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
        let payload_bytes = executor.execute(cluster_op).await.map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;
        let cluster_plan_kind = match cluster_op {
            ClusterArrayOp::Slice { .. } => PlanKind::ArraySlice,
            _ => PlanKind::MultiRow,
        };
        match compose::shape_payload_no_plan(&payload_bytes, cluster_plan_kind, projection) {
            ShapeOutcome::Rows(shaped) => {
                let (response, notice) =
                    shape_encode::shaped_query_response(shaped, result_formats);
                if let Some(n) = notice {
                    self.sessions.push_notice(addr, n);
                }
                Ok(response)
            }
            ShapeOutcome::Passthrough => {
                let shaped = payload_to_response(&payload_bytes, cluster_plan_kind)?;
                if let Some(notice) = shaped.notice {
                    self.sessions.push_notice(addr, notice);
                }
                Ok(shaped.response)
            }
        }
    }
}
