//! NodeDB pgwire handler: struct definition, identity resolution,
//! permission checks, SQL execution entry point, and pgwire trait impls.
//!
//! Supports per-connection session state: transaction blocks (BEGIN/COMMIT/ROLLBACK),
//! session parameters (SET/SHOW), EXPLAIN, and DISCARD ALL.

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::SinkExt;
use futures::sink::Sink;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::bridge::envelope::PhysicalPlan;
use crate::config::auth::AuthMode;
use crate::control::planner::context::QueryContext;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{
    AuthMethod, AuthenticatedIdentity, Role, required_permission, role_grants_permission,
};
use crate::control::state::SharedState;
use crate::types::{RequestId, TenantId};

use super::super::session::{SessionStore, TransactionState, parse_set_command, parse_show_command};
use super::super::types::{error_to_sqlstate, notice_warning, text_field};
use super::plan::extract_collection;

/// PostgreSQL wire protocol handler for NodeDB.
///
/// Implements `SimpleQueryHandler` + `ExtendedQueryHandler`.
/// Receives SQL strings from clients, resolves the authenticated identity,
/// checks permissions, plans via DataFusion, dispatches to the Data Plane
/// via SPSC, and returns results.
///
/// Lives on the Control Plane (Send + Sync).
pub struct NodeDbPgHandler {
    pub(crate) state: Arc<SharedState>,
    pub(super) query_ctx: QueryContext,
    next_request_id: AtomicU64,
    query_parser: Arc<NoopQueryParser>,
    auth_mode: AuthMode,
    /// Per-connection session state (transaction blocks, parameters).
    pub(crate) sessions: SessionStore,
}

impl NodeDbPgHandler {
    pub fn new(state: Arc<SharedState>, auth_mode: AuthMode) -> Self {
        Self {
            state,
            query_ctx: QueryContext::new(),
            next_request_id: AtomicU64::new(1_000_000),
            query_parser: Arc::new(NoopQueryParser::new()),
            auth_mode,
            sessions: SessionStore::new(),
        }
    }

    pub(super) fn next_request_id(&self) -> RequestId {
        RequestId::new(self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Resolve the authenticated identity from pgwire client metadata.
    fn resolve_identity<C: ClientInfo>(&self, client: &C) -> PgWireResult<AuthenticatedIdentity> {
        let username = client
            .metadata()
            .get("user")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        match self.auth_mode {
            AuthMode::Trust => {
                if let Some(identity) = self
                    .state
                    .credentials
                    .to_identity(&username, AuthMethod::Trust)
                {
                    Ok(identity)
                } else {
                    Ok(AuthenticatedIdentity {
                        user_id: 0,
                        username,
                        tenant_id: TenantId::new(1),
                        auth_method: AuthMethod::Trust,
                        roles: vec![Role::Superuser],
                        is_superuser: true,
                    })
                }
            }
            AuthMode::Password | AuthMode::Md5Password | AuthMode::Certificate => self
                .state
                .credentials
                .to_identity(&username, AuthMethod::ScramSha256)
                .ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28000".to_owned(),
                        format!("authenticated user '{username}' not found in credential store"),
                    )))
                }),
        }
    }

    /// Check if the identity has permission for the given plan.
    ///
    /// Enforcement layers:
    /// 1. Superuser → always allowed
    /// 2. System catalog (`_system.*`) → superuser only
    /// 3. Collection-level grants (PermissionStore::check with ownership + roles + grants)
    /// 4. Built-in role fallback (role_grants_permission)
    pub(super) fn check_permission(
        &self,
        identity: &AuthenticatedIdentity,
        plan: &PhysicalPlan,
    ) -> PgWireResult<()> {
        if identity.is_superuser {
            return Ok(());
        }

        let required = required_permission(plan);
        let collection = extract_collection(plan);

        // Block non-superuser access to system catalog collections.
        if let Some(coll) = collection {
            if coll.starts_with("_system") {
                self.state.audit_record(
                    AuditEvent::AuthzDenied,
                    Some(identity.tenant_id),
                    &identity.username,
                    &format!("system catalog access denied: {coll}"),
                );
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42501".to_owned(),
                    "permission denied: system catalog access requires superuser".to_owned(),
                ))));
            }
        }

        // Check collection-level permissions (ownership + explicit grants + role grants).
        if let Some(coll) = collection {
            if self
                .state
                .permissions
                .check(identity, required, coll, &self.state.roles)
            {
                return Ok(());
            }
        }

        // Fall back to role-based check.
        let has_permission = identity
            .roles
            .iter()
            .any(|role| role_grants_permission(role, required));

        if has_permission {
            Ok(())
        } else {
            self.state.audit_record(
                AuditEvent::AuthzDenied,
                Some(identity.tenant_id),
                &identity.username,
                &format!("permission {:?} denied", required),
            );

            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42501".to_owned(),
                format!(
                    "permission denied: user '{}' lacks {:?} permission{}",
                    identity.username,
                    required,
                    collection.map(|c| format!(" on '{c}'")).unwrap_or_default()
                ),
            ))))
        }
    }

    /// Handle SET commands: parse, validate, store in session.
    fn handle_set(
        &self,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let (key, value) = match parse_set_command(sql) {
            Some(kv) => kv,
            None => {
                // Malformed SET — accept silently for compatibility.
                return Ok(vec![Response::Execution(Tag::new("SET"))]);
            }
        };

        // Validate NodeDB-specific parameters.
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

        if key == "nodedb.tenant_id" && value.parse::<u32>().is_err() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!("invalid value for nodedb.tenant_id: '{value}'. Must be an integer."),
            ))));
        }

        self.sessions.set_parameter(addr, key, value);
        Ok(vec![Response::Execution(Tag::new("SET"))])
    }

    /// Handle SHOW commands: return session parameter values.
    fn handle_show(
        &self,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
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

        // Special cases for server-level parameters.
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
    fn handle_show_all(
        &self,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        let schema = Arc::new(vec![
            text_field("name"),
            text_field("setting"),
        ]);

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
    async fn handle_explain(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let upper = sql.to_uppercase();
        let is_analyze = upper.starts_with("EXPLAIN ANALYZE ");

        // Strip EXPLAIN [ANALYZE] prefix.
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

        // Check if it's a DDL command.
        if super::super::ddl::dispatch(&self.state, identity, inner_sql).is_some() {
            let schema = Arc::new(vec![text_field("QUERY PLAN")]);
            let plan_text = format!("DDL: {}", inner_sql.split_whitespace().take(3).collect::<Vec<_>>().join(" "));
            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder.encode_field(&plan_text)?;
            let row = encoder.take_row();
            return Ok(vec![Response::Query(QueryResponse::new(
                schema,
                futures::stream::iter(vec![Ok(row)]),
            ))]);
        }

        // Plan via DataFusion to get the physical plan description.
        let tenant_id = identity.tenant_id;
        let tasks = self.query_ctx.plan_sql(inner_sql, tenant_id).await.map_err(|e| {
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
                // Split long lines for readability.
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

    /// Execute a SQL query: session state → identity → DDL check → quota → plan → perms → dispatch.
    async fn execute_sql(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sql_trimmed = sql.trim();
        let upper = sql_trimmed.to_uppercase();

        // Ensure session exists.
        self.sessions.ensure_session(*addr);

        // Empty query.
        if sql_trimmed.is_empty() || sql_trimmed == ";" {
            return Ok(vec![Response::EmptyQuery]);
        }

        // Transaction commands (always allowed, even in Failed state for ROLLBACK).
        if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
            self.sessions.begin(addr).map_err(|msg| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "25P02".to_owned(),
                    msg.to_owned(),
                )))
            })?;
            return Ok(vec![Response::Execution(Tag::new("BEGIN"))]);
        }

        if upper == "COMMIT" || upper == "END" || upper == "END TRANSACTION" {
            self.sessions.commit(addr).map_err(|msg| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "25000".to_owned(),
                    msg.to_owned(),
                )))
            })?;
            return Ok(vec![Response::Execution(Tag::new("COMMIT"))]);
        }

        if upper == "ROLLBACK" || upper == "ABORT" {
            let _ = self.sessions.rollback(addr);
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        }

        if upper.starts_with("SAVEPOINT ") {
            // Accept SAVEPOINT for compatibility but no-op (per-statement commit model).
            return Ok(vec![Response::Execution(Tag::new("SAVEPOINT"))]);
        }

        if upper.starts_with("RELEASE SAVEPOINT ") || upper.starts_with("RELEASE ") {
            return Ok(vec![Response::Execution(Tag::new("RELEASE"))]);
        }

        if upper.starts_with("ROLLBACK TO ") {
            // In per-statement commit model, ROLLBACK TO just clears failed state.
            // We can't actually undo prior statements, but we restore the session.
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        }

        // In failed transaction state, reject everything except ROLLBACK (handled above).
        if self.sessions.transaction_state(addr) == TransactionState::Failed {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P02".to_owned(),
                "current transaction is aborted, commands ignored until end of transaction block"
                    .to_owned(),
            ))));
        }

        // SET commands — parse and store in session.
        if upper.starts_with("SET ") {
            return self.handle_set(addr, sql_trimmed);
        }

        // SHOW commands — return session parameter values.
        if upper.starts_with("SHOW ")
            && !upper.starts_with("SHOW USERS")
            && !upper.starts_with("SHOW TENANTS")
            && !upper.starts_with("SHOW SESSION")
            && !upper.starts_with("SHOW CLUSTER")
            && !upper.starts_with("SHOW RAFT")
            && !upper.starts_with("SHOW MIGRATIONS")
            && !upper.starts_with("SHOW PEER")
            && !upper.starts_with("SHOW NODES")
            && !upper.starts_with("SHOW NODE ")
            && !upper.starts_with("SHOW COLLECTIONS")
            && !upper.starts_with("SHOW AUDIT")
            && !upper.starts_with("SHOW PERMISSIONS")
            && !upper.starts_with("SHOW GRANTS")
        {
            return self.handle_show(addr, sql_trimmed);
        }

        // RESET command — restore parameter to default.
        if upper.starts_with("RESET ") {
            let param = sql_trimmed[6..].trim().to_lowercase();
            self.sessions.set_parameter(addr, param, String::new());
            return Ok(vec![Response::Execution(Tag::new("RESET"))]);
        }

        // DISCARD ALL — reset all session state.
        if upper == "DISCARD ALL" {
            self.sessions.remove(addr);
            self.sessions.ensure_session(*addr);
            return Ok(vec![Response::Execution(Tag::new("DISCARD ALL"))]);
        }

        // EXPLAIN / EXPLAIN ANALYZE — plan but don't execute.
        if upper.starts_with("EXPLAIN ") {
            return self.handle_explain(identity, sql_trimmed).await;
        }

        // DDL dispatch (CREATE, DROP, ALTER, GRANT, REVOKE, BACKUP, RESTORE, etc.)
        if let Some(result) = super::super::ddl::dispatch(&self.state, identity, sql_trimmed) {
            return result;
        }

        // Regular SQL query — plan via DataFusion → dispatch to Data Plane.
        let tenant_id = identity.tenant_id;

        self.state.check_tenant_quota(tenant_id).map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        self.state.tenant_request_start(tenant_id);
        let result = self.execute_planned_sql(identity, sql_trimmed, tenant_id).await;
        self.state.tenant_request_end(tenant_id);

        // If the query failed and we're in a transaction block, mark as failed.
        if result.is_err() {
            self.sessions.fail_transaction(addr);
        }

        result
    }
}

// ── SimpleQueryHandler ──────────────────────────────────────────────

#[async_trait]
impl SimpleQueryHandler for NodeDbPgHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let addr = client.socket_addr();
        self.sessions.ensure_session(addr);

        let identity = self.resolve_identity(client)?;

        // Send notice if BEGIN is called (advisory transactions).
        let upper = query.trim().to_uppercase();
        if (upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION")
            && self.sessions.transaction_state(&addr) == TransactionState::InBlock
        {
            let notice = notice_warning("there is already a transaction in progress");
            let _ = client
                .send(PgWireBackendMessage::NoticeResponse(notice))
                .await;
        }

        if (upper == "COMMIT" || upper == "END")
            && self.sessions.transaction_state(&addr) == TransactionState::Idle
        {
            let notice = notice_warning("there is no transaction in progress");
            let _ = client
                .send(PgWireBackendMessage::NoticeResponse(notice))
                .await;
        }

        self.execute_sql(&identity, &addr, query).await
    }
}

// ── ExtendedQueryHandler ────────────────────────────────────────────

#[async_trait]
impl ExtendedQueryHandler for NodeDbPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let addr = client.socket_addr();
        let identity = self.resolve_identity(client)?;
        let query = &portal.statement.statement;
        let mut results = self.execute_sql(&identity, &addr, query).await?;
        Ok(results.pop().unwrap_or(Response::EmptyQuery))
    }
}

// Trust mode: NoopStartupHandler (no authentication).
impl NoopStartupHandler for NodeDbPgHandler {}
