// SPDX-License-Identifier: BUSL-1.1

//! NodeDB pgwire handler: struct definition, identity resolution,
//! permission checks, and pgwire trait impls (SimpleQueryHandler,
//! ExtendedQueryHandler).

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::SinkExt;
use futures::sink::Sink;

use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::StoredStatement;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::config::auth::AuthMode;
use crate::control::planner::context::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::RequestId;

use super::super::types::notice_warning;
use super::in_flight::InFlightGuard;
use super::prepared::{NodeDbQueryParser, ParsedStatement};
use crate::control::server::shared::session::SessionStore;

mod simple_query;

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
    query_parser: Arc<NodeDbQueryParser>,
    pub(super) auth_mode: AuthMode,
    /// Per-connection session state (transaction blocks, parameters).
    pub(crate) sessions: Arc<SessionStore>,
    /// Per-connection in-flight COPY IN restore accumulators.
    pub(crate) restore_state: Arc<crate::control::backup::RestoreState>,
}

impl NodeDbPgHandler {
    pub fn new(state: Arc<SharedState>, auth_mode: AuthMode) -> Self {
        // Every top-level user query goes through this handler's
        // shared `query_ctx`, which acquires descriptor leases so
        // in-flight plans are protected from concurrent DDL.
        // Sub-planners (check constraints, type guards, ANALYZE,
        // procedural DML) build their own no-lease `QueryContext`
        // via `for_state`.
        let query_ctx = QueryContext::for_state_with_lease(&state);
        let sessions = Arc::new(SessionStore::new());
        let query_parser = Arc::new(NodeDbQueryParser::new(
            Arc::clone(&state),
            auth_mode.clone(),
            Arc::clone(&sessions),
        ));
        Self {
            state,
            query_ctx,
            query_parser,
            auth_mode,
            sessions,
            restore_state: Arc::new(crate::control::backup::RestoreState::new()),
        }
    }

    pub(super) fn next_request_id(&self) -> RequestId {
        self.state.next_request_id()
    }

    /// Resolve the authenticated identity from pgwire client metadata, then
    /// overlay the session's superuser tenant override (if any) onto the
    /// resolved `tenant_id`.
    ///
    /// The override is installed via `SET TENANT = '<name>' | <id> | DEFAULT`
    /// or `SET nodedb.tenant_id = <id>`; the SET handler enforces that only
    /// superuser sessions may install one and that no active transaction is
    /// in flight. Honoring it here — at the single chokepoint every query
    /// path passes through immediately after authentication — keeps every
    /// downstream `identity.tenant_id` read correct without threading the
    /// session into 13 unrelated dispatchers.
    pub(crate) fn resolve_identity<C: ClientInfo>(
        &self,
        client: &C,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<AuthenticatedIdentity> {
        super::auth::resolve_session_identity(
            &self.state,
            self.auth_mode.clone(),
            &self.sessions,
            client,
            addr,
        )
    }
}

// ── ExtendedQueryHandler ────────────────────────────────────────────

#[async_trait]
impl ExtendedQueryHandler for NodeDbPgHandler {
    type Statement = ParsedStatement;
    type QueryParser = NodeDbQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let addr = client.socket_addr();
        // Keep long-running prepared statements ineligible for idle teardown.
        let _in_flight = InFlightGuard::new(&self.sessions, addr);

        let result = self.execute_prepared(client, portal, max_rows).await;
        // Mirror the simple-query path: surface any queued NOTICE messages
        // (e.g. `truncated_before_horizon`) before returning.
        for message in self.sessions.drain_notices(&addr) {
            let notice = notice_warning(&message);
            let _ = client
                .send(PgWireBackendMessage::NoticeResponse(notice))
                .await;
        }
        result
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.describe_statement_impl(client, target).await
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.describe_portal_impl(client, target).await
    }
}
