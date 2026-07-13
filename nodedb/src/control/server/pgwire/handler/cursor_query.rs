// SPDX-License-Identifier: BUSL-1.1

//! `DECLARE CURSOR` materialisation: plan a SELECT, dispatch it to the
//! Data Plane, and collect JSON-encoded rows for cursor storage.

use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::TraceId;

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Execute a SELECT query and return results as JSON strings for cursor storage.
    pub(super) async fn execute_query_for_cursor(
        &self,
        addr: &std::net::SocketAddr,
        sql: &str,
        identity: &AuthenticatedIdentity,
    ) -> PgWireResult<Vec<String>> {
        let tenant_id = identity.tenant_id;
        let query_ctx =
            crate::control::planner::context::QueryContext::for_state_with_lease(&self.state);

        if let Some(mode) = self.sessions.get_parameter(addr, "rounding_mode") {
            query_ctx.set_rounding_mode(&mode);
        }

        let database_id = self
            .sessions
            .get_current_database(addr)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);

        let auth_ctx = crate::control::server::session_auth::build_auth_context(identity);
        let perm_cache = self.state.permission_cache.read().await;
        let sec = crate::control::planner::context::PlanSecurityContext {
            identity,
            auth: &auth_ctx,
            rls_store: &self.state.rls,
            permissions: &self.state.permissions,
            roles: &self.state.roles,
            permission_cache: Some(&*perm_cache),
        };
        let (tasks, _output_schema) = query_ctx
            .plan_sql_with_rls(crate::control::planner::context::PlanSqlWithRlsParams {
                sql,
                tenant_id,
                database_id,
                sec: &sec,
            })
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42000".to_owned(),
                    e.to_string(),
                )))
            })?;

        self.authorize_tasks(identity, &tasks)?;

        let mut rows = Vec::new();
        for task in tasks {
            let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
                &self.state,
                task.tenant_id,
                task.database_id,
                task.vshard_id,
                task.plan,
                TraceId::ZERO,
            )
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    e.to_string(),
                )))
            })?;

            if !resp.payload.is_empty() {
                let json =
                    crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
                rows.push(json);
            }
        }
        Ok(rows)
    }
}
