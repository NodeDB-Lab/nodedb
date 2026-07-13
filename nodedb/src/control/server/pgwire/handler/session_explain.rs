// SPDX-License-Identifier: BUSL-1.1

//! EXPLAIN command handler: plan the inner SQL and return the plan description.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;

use super::super::types::{error_to_sqlstate, text_field};
use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Handle EXPLAIN: plan the inner SQL and return the plan description.
    pub(super) async fn handle_explain(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let upper = sql.to_uppercase();
        let is_analyze = upper.starts_with("EXPLAIN ANALYZE ");

        let inner_sql = if is_analyze {
            sql[16..].trim()
        } else if upper.starts_with("EXPLAIN ") {
            sql[8..].trim()
        } else {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "syntax error in EXPLAIN".to_owned(),
            ))));
        };

        let database_id = self
            .sessions
            .get_current_database(addr)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let txn_ctx = crate::control::server::shared::session::DmlTxnCtx {
            sessions: &self.sessions,
            addr,
        };
        if crate::control::server::shared::ddl::dispatch(
            &self.state,
            identity,
            inner_sql,
            database_id,
            &txn_ctx,
        )
        .await
        .is_some()
        {
            let schema = Arc::new(vec![text_field("QUERY PLAN")]);
            let plan_text = format!(
                "DDL: {}",
                inner_sql
                    .split_whitespace()
                    .take(3)
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder.encode_field(&plan_text)?;
            let row = encoder.take_row();
            return Ok(vec![Response::Query(QueryResponse::new(
                schema,
                futures::stream::iter(vec![Ok(row)]),
            ))]);
        }

        let tenant_id = identity.tenant_id;
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
        let (tasks, _output_schema) = self
            .query_ctx
            .plan_sql_with_rls(crate::control::planner::context::PlanSqlWithRlsParams {
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

        self.authorize_tasks(identity, &tasks)?;

        let schema = Arc::new(vec![text_field("QUERY PLAN")]);
        let mut rows = Vec::new();
        let mut encoder = DataRowEncoder::new(schema.clone());

        // Prepend Calvin preamble row when tasks span multiple vShards.
        {
            use crate::control::planner::calvin::calvin_explain_preamble;
            let mode = self.sessions.cross_shard_txn_mode(addr);
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
