//! Session parameter commands: SET, SHOW, SHOW ALL, EXPLAIN.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;

use super::super::types::{error_to_sqlstate, text_field};
use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Handle SET commands: parse, validate, store in session.
    pub(super) fn handle_set(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        use super::super::session::parse_set_command;
        use pgwire::api::results::Tag;

        // Handle SET TRANSACTION READ ONLY / READ WRITE.
        let upper = sql.to_uppercase();
        if upper.starts_with("SET TRANSACTION") || upper.starts_with("SET SESSION CHARACTERISTICS")
        {
            if upper.contains("READ ONLY") {
                self.sessions.set_parameter(
                    addr,
                    "transaction_access_mode".into(),
                    "read_only".into(),
                );
                return Ok(vec![Response::Execution(Tag::new("SET"))]);
            }
            if upper.contains("READ WRITE") {
                self.sessions.set_parameter(
                    addr,
                    "transaction_access_mode".into(),
                    "read_write".into(),
                );
                return Ok(vec![Response::Execution(Tag::new("SET"))]);
            }
            // Other SET TRANSACTION options (ISOLATION LEVEL, etc.) — acknowledge.
            return Ok(vec![Response::Execution(Tag::new("SET"))]);
        }

        let (key, value) = match parse_set_command(sql) {
            Some(kv) => kv,
            None => {
                return Ok(vec![Response::Execution(Tag::new("SET"))]);
            }
        };

        if key == "nodedb.consistency" {
            match value.as_str() {
                "strong" | "eventual" => {}
                s if s.starts_with("bounded_staleness") => {}
                _ => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22023".to_owned(),
                        format!(
                            "invalid value for nodedb.consistency: '{value}'. Valid: strong, bounded_staleness(<ms>), eventual"
                        ),
                    ))));
                }
            }
        }

        if key == super::super::session::read_consistency::PARAM_KEY
            && super::super::session::read_consistency::parse_value(&value).is_none()
        {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!(
                    "invalid value for {}: '{value}'. Valid: strong, bounded_staleness:<secs>, eventual",
                    super::super::session::read_consistency::PARAM_KEY
                ),
            ))));
        }

        if key == "nodedb.tenant_id" && value.parse::<u32>().is_err() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!("invalid value for nodedb.tenant_id: '{value}'. Must be an integer."),
            ))));
        }

        // Eager validation for `nodedb.auth_session`: drive the resolve path
        // now so rate-limit / audit / fingerprint checks fire on each SET
        // rather than being deferred to the next query. A probing client
        // that hammers SET LOCAL with bogus handles and never runs a query
        // must still be throttled and observed.
        if key == "nodedb.auth_session" {
            use crate::control::security::session_handle::{ClientFingerprint, ResolveOutcome};
            let caller_fp = ClientFingerprint::from_peer(identity.tenant_id, addr);
            let conn_key = addr.to_string();
            match self
                .state
                .session_handles
                .resolve(&value, &conn_key, &caller_fp)
            {
                ResolveOutcome::RateLimited => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "53300".to_owned(),
                        "session handle resolve rate limit exceeded on this \
                         connection — closing"
                            .to_owned(),
                    ))));
                }
                ResolveOutcome::Resolved(_) | ResolveOutcome::Miss => {
                    // Store the raw value either way — Miss might be a
                    // handle that was valid previously and expired; the
                    // next query's resolve will fall back to base identity.
                }
            }
        }

        self.sessions.set_parameter(addr, key, value);
        Ok(vec![Response::Execution(Tag::new("SET"))])
    }

    /// Handle SHOW commands: return session parameter values.
    pub(super) fn handle_show(
        &self,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        use super::super::session::parse_show_command;

        let param = match parse_show_command(sql) {
            Some(p) => p,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "syntax error: SHOW <parameter> or SHOW ALL".to_owned(),
                ))));
            }
        };

        if param == "all" {
            return self.handle_show_all(addr);
        }

        let value = match param.as_str() {
            "server_version" => Some("NodeDB 0.1.0".to_owned()),
            "server_encoding" => Some("UTF8".into()),
            _ => self.sessions.get_parameter(addr, &param),
        };

        let value = value.unwrap_or_else(|| "".into());

        let schema = Arc::new(vec![text_field(&param)]);
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder.encode_field(&value)?;
        let row = encoder.take_row();
        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            futures::stream::iter(vec![Ok(row)]),
        ))])
    }

    /// SHOW ALL — return all session parameters.
    pub(super) fn handle_show_all(
        &self,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        let schema = Arc::new(vec![text_field("name"), text_field("setting")]);

        let params = self.sessions.all_parameters(addr);
        let mut rows = Vec::with_capacity(params.len());
        let mut encoder = DataRowEncoder::new(schema.clone());

        for (key, value) in &params {
            encoder.encode_field(key)?;
            encoder.encode_field(value)?;
            rows.push(Ok(encoder.take_row()));
        }

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            futures::stream::iter(rows),
        ))])
    }

    /// Handle EXPLAIN: plan the inner SQL and return the plan description.
    pub(super) async fn handle_explain(
        &self,
        identity: &AuthenticatedIdentity,
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

        if super::super::ddl::dispatch(&self.state, identity, inner_sql)
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
        let tasks = self
            .query_ctx
            .plan_sql_with_rls(inner_sql, tenant_id, &sec)
            .await
            .map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

        let schema = Arc::new(vec![text_field("QUERY PLAN")]);
        let mut rows = Vec::new();
        let mut encoder = DataRowEncoder::new(schema.clone());

        if tasks.is_empty() {
            encoder.encode_field(&"Empty plan (no tasks)")?;
            rows.push(Ok(encoder.take_row()));
        } else {
            for (i, task) in tasks.iter().enumerate() {
                let plan_desc = format!(
                    "Task {}: {:?} tenant={} vshard={}",
                    i + 1,
                    task.plan,
                    task.tenant_id.as_u32(),
                    task.vshard_id.as_u16(),
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
