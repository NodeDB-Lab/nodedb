// SPDX-License-Identifier: BUSL-1.1

//! EXPLAIN command handler: plan the inner SQL and return the plan description.

use std::sync::Arc;

use nodedb_types::strip_prefix_ascii_case_insensitive;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;

use super::super::types::{error_to_sqlstate, text_field};
use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Handle EXPLAIN: plan the inner SQL and return the plan description.
    pub(super) async fn handle_explain(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let inner_sql =
            if let Some(inner) = strip_prefix_ascii_case_insensitive(sql, "EXPLAIN ANALYZE ") {
                inner.trim()
            } else if let Some(inner) = strip_prefix_ascii_case_insensitive(sql, "EXPLAIN ") {
                inner.trim()
            } else {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "syntax error in EXPLAIN".to_owned(),
                ))));
            };

        match nodedb_sql::ddl_ast::parse(inner_sql) {
            Some(Ok(stmt)) => {
                let schema = Arc::new(vec![text_field("QUERY PLAN")]);
                let plan_text = format!("DDL: {stmt:?}");
                let mut encoder = DataRowEncoder::new(schema.clone());
                encoder.encode_field(&plan_text)?;
                let row = encoder.take_row();
                return Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    futures::stream::iter(vec![Ok(row)]),
                ))]);
            }
            Some(Err(error)) => {
                let sqlstate = match error {
                    nodedb_sql::SqlError::UnsupportedConstraint { .. }
                    | nodedb_sql::SqlError::ConflictingEngineClause { .. } => "0A000",
                    _ => "42601",
                };
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    sqlstate.to_owned(),
                    error.to_string(),
                ))));
            }
            None => {}
        }

        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let tenant_id = identity.tenant_id;
        // EXPLAIN must resolve RLS variables in the selected database
        // context — `for_database` stamps `database_id` through the single
        // lockstep path and runs scope-grant enrichment.
        let scope = crate::control::security::request_scope::RequestAuthScope::for_database(
            identity,
            self.state.auth_stores(),
            database_id,
        );
        let perm_cache =
            crate::control::security::auth_fence::permission_view(&self.state, tenant_id)
                .await
                .map_err(|e| crate::control::server::pgwire::types::error_map::error_to_pg(&e))?;
        let sec = crate::control::planner::context::PlanSecurityContext {
            identity,
            auth: scope.auth(),
            rls_store: &self.state.rls,
            redaction_store: &self.state.redaction,
            permissions: &self.state.permissions,
            roles: &self.state.roles,
            permission_cache: Some(&*perm_cache),
        };
        let (tasks, _output_schema) = self
            .query_ctx
            .plan_sql_with_rls_metadata(crate::control::planner::context::PlanSqlWithRlsParams {
                sql: inner_sql,
                tenant_id,
                database_id,
                sec: &sec,
            })
            .await
            .map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

        let _authorized_tasks = self.authorize_tasks(identity, &tasks)?;

        let schema = Arc::new(vec![text_field("QUERY PLAN")]);
        let mut rows = Vec::new();
        let mut encoder = DataRowEncoder::new(schema.clone());

        // Prepend Calvin preamble row when tasks span multiple vShards.
        {
            use crate::control::planner::calvin::calvin_explain_preamble;
            let mode = self.sessions.cross_shard_txn_mode(session_id);
            if let Some(preamble) = calvin_explain_preamble(&tasks, mode, None) {
                encoder.encode_field(&preamble)?;
                rows.push(Ok(encoder.take_row()));
            }
        }

        if tasks.is_empty() {
            encoder.encode_field(&"Empty plan (no tasks)")?;
            rows.push(Ok(encoder.take_row()));
        } else {
            for (i, task) in tasks.iter().enumerate() {
                let plan_desc = format!(
                    "Task {}: {:?} tenant={} vshard={}",
                    i + 1,
                    task.plan,
                    task.tenant_id.as_u64(),
                    task.vshard_id.as_u32(),
                );
                for line in plan_desc.lines() {
                    encoder.encode_field(&line)?;
                    rows.push(Ok(encoder.take_row()));
                }
            }
        }

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            futures::stream::iter(rows),
        ))])
    }
}
