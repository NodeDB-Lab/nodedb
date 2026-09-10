// SPDX-License-Identifier: BUSL-1.1

//! `DECLARE CURSOR` materialisation: plan a SELECT, dispatch it to the
//! Data Plane, and collect JSON-encoded rows for cursor storage.

use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::retry::retry_on_schema_change;
use crate::control::server::shared::session::SessionId;
use crate::types::TraceId;

use super::core::NodeDbPgHandler;
use super::routing::setup_error::StatementSetupError;

impl NodeDbPgHandler {
    /// Execute a SELECT query and return results as JSON strings for cursor storage.
    pub(super) async fn execute_query_for_cursor(
        &self,
        session_id: SessionId,
        sql: &str,
        identity: &AuthenticatedIdentity,
    ) -> PgWireResult<Vec<String>> {
        let tenant_id = identity.tenant_id;
        let query_ctx =
            crate::control::planner::context::QueryContext::for_state_with_lease(&self.state);

        if let Some(mode) = self.sessions.get_parameter(session_id, "rounding_mode") {
            query_ctx.set_rounding_mode(&mode);
        }

        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);

        // Cursor plans must resolve RLS variables in the selected database
        // context — `for_database` builds the identity-derived `AuthContext`
        // and stamps `database_id` through the single lockstep path, plus
        // (new here) runs scope-grant enrichment so a cursor's
        // `$auth.scope_status(...)` resolves like every other query path.
        let scope = crate::control::security::request_scope::RequestAuthScope::for_database(
            identity,
            self.state.auth_stores(),
            database_id,
        );

        // Borrowed so each retry attempt re-reads permission state without
        // moving the planning context out of the retried closure.
        let auth_ctx = scope.auth();
        let query_ctx = &query_ctx;

        // Planning records the descriptor versions and the lease that pins them
        // is acquired afterwards, so both run as ONE retried unit: a descriptor
        // drain starting in between is absorbed instead of failing the DECLARE.
        // Admission still follows the explicit authorization boundary, so a
        // rejected cursor declaration consumes no descriptor lease. The scope
        // remains live while every cursor-materialization task is dispatched.
        let (tasks, _lease_scope) = retry_on_schema_change(move || async move {
            let perm_cache = self.state.permission_cache.read().await;
            let sec = crate::control::planner::context::PlanSecurityContext {
                identity,
                auth: auth_ctx,
                rls_store: &self.state.rls,
                redaction_store: &self.state.redaction,
                permissions: &self.state.permissions,
                roles: &self.state.roles,
                permission_cache: Some(&*perm_cache),
            };
            let (tasks, _output_schema, versions, _) = query_ctx
                .plan_sql_with_rls_and_versions(sql, tenant_id, database_id, &sec, None)
                .await
                .map_err(StatementSetupError::from)?;
            drop(perm_cache);

            // Deliberate gate: proves the planned set is authorizable before the
            // descriptor lease is acquired. Dispatch below re-derives the
            // capability per task through the clone-check gate.
            let _preauthorized_tasks = self
                .authorize_tasks(identity, &tasks)
                .map_err(StatementSetupError::from)?;

            let lease_scope = self
                .state
                .acquire_plan_lease_scope(&versions)
                .map_err(StatementSetupError::from)?;
            Ok::<_, StatementSetupError>((tasks, lease_scope))
        })
        .await
        .map_err(PgWireError::from)?;

        let mut rows = Vec::new();
        for task in tasks {
            let tenant_id = task.tenant_id;
            let emitter = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
                &self.state.audit,
            ));
            let resp =
                crate::control::server::shared::clone_write::intercept_authorize_and_dispatch(
                    crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                        state: &self.state,
                        task,
                        identity,
                        tenant_id,
                        permissions: &self.state.permissions,
                        roles: &self.state.roles,
                        emitter: &emitter,
                    },
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
