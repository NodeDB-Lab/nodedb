// SPDX-License-Identifier: BUSL-1.1

//! ClusterArray plan dispatch for the pgwire handler.
//!
//! `dispatch_task_loop` intercepts a `PhysicalPlan::ClusterArray` task and
//! delegates to the shared, protocol-neutral core
//! (`shared::cluster_array_dispatch::execute_cluster_array`), then encodes
//! the outcome as one pgwire `Response` (surfacing any client-facing notice
//! via the session) or a `DmlOutcome` the caller folds into the statement tag.

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::DmlOutcome;
use crate::control::server::shared::cluster_array_dispatch::{
    ClusterArrayShaped, execute_cluster_array,
};
use crate::control::server::shared::session::SessionId;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;
use super::super::shape_encode;

/// What one `ClusterArrayOp` answers with.
pub(super) enum ClusterArrayResult {
    /// A read's rows (`Slice` / `Agg`), encoded. Caller pushes the response.
    Rows(Response),
    /// A write's count (`Put` / `Delete`). Caller folds it into the
    /// statement tag.
    Dml(DmlOutcome),
}

impl NodeDbPgHandler {
    /// Execute a single `ClusterArrayOp` via the shared core and encode its
    /// outcome into one pgwire `Response` or one count-bearing outcome. Any
    /// carried notice is pushed to the supplied session.
    pub(super) async fn dispatch_cluster_array_task(
        &self,
        authorized: crate::control::server::shared::authorization::AuthorizedTask,
        projection: Option<&OutputSchema>,
        result_formats: &[FieldFormat],
        session_id: SessionId,
        auth: &crate::control::security::auth_context::AuthContext,
    ) -> PgWireResult<ClusterArrayResult> {
        match execute_cluster_array(&self.state, auth, authorized, projection)
            .await
            .map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })? {
            ClusterArrayShaped::Rows(shaped) => {
                let (response, notice) =
                    shape_encode::shaped_query_response(shaped, result_formats);
                if let Some(n) = notice {
                    self.sessions.push_notice(session_id, n);
                }
                Ok(ClusterArrayResult::Rows(response))
            }
            ClusterArrayShaped::Affected(outcome) => Ok(ClusterArrayResult::Dml(outcome)),
        }
    }
}
