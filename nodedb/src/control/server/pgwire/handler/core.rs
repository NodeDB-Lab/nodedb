//! NodeDB pgwire handler: struct definition, identity resolution,
//! permission checks, and pgwire trait impls (SimpleQueryHandler,
//! ExtendedQueryHandler, NoopStartupHandler).

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::SinkExt;
use futures::sink::Sink;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::Response;
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

use super::super::session::{SessionStore, TransactionState};
use super::super::types::notice_warning;
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
        let query_ctx = QueryContext::with_catalog(
            Arc::clone(&state.credentials),
            1, // default tenant for name resolution
        );
        Self {
            state,
            query_ctx,
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
        if let Some(coll) = collection
            && coll.starts_with("_system")
        {
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

        // Check collection-level permissions (ownership + explicit grants + role grants).
        if let Some(coll) = collection
            && self
                .state
                .permissions
                .check(identity, required, coll, &self.state.roles)
        {
            return Ok(());
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
