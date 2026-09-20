// SPDX-License-Identifier: BUSL-1.1

//! Native protocol connection state and constructors.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;

use crate::config::auth::AuthMode;
use crate::control::planner::context::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::admission::{AdmissionRegistry, ConnectionPermit};
use crate::control::server::conn_stream::ConnStream;
use crate::control::server::shared::session::SessionStore;
use crate::control::state::SharedState;

use super::super::codec::FrameFormat;
use super::run::NativeTxnCleanup;

/// Exact cleanup resources for one accepted native connection.
///
/// The listener constructs these before spawning so forced draining retains
/// rollback ownership even if the connection task is cancelled mid-handshake.
pub(crate) struct NativeConnectionResources {
    sessions: Arc<SessionStore>,
    cleanup: Arc<NativeTxnCleanup>,
}

impl NativeConnectionResources {
    pub(crate) fn new(peer_addr: SocketAddr, state: Arc<SharedState>) -> Self {
        let sessions = Arc::new(SessionStore::new());
        let cleanup = Arc::new(NativeTxnCleanup::new(
            Arc::clone(&sessions),
            peer_addr.into(),
            state,
        ));
        Self { sessions, cleanup }
    }

    pub(crate) fn cleanup(&self) -> Arc<NativeTxnCleanup> {
        Arc::clone(&self.cleanup)
    }
}

/// A client session on the native binary protocol.
///
/// Auto-detects JSON vs MessagePack on the first frame. Supports all
/// operations: auth, SQL, DDL, transactions, direct Data Plane ops.
///
/// Admission is two-phase:
/// 1. A global connection permit is acquired at TCP accept and handed in via
///    `global_permit`.
/// 2. After successful authentication, per-database and per-tenant permits
///    are combined with the global permit into a `ConnectionPermit` held for
///    the connection's lifetime.
pub struct NativeSession {
    pub(super) stream: ConnStream,
    /// What the connection negotiated, captured at accept because the TLS
    /// session is unreachable once the stream is borrowed for framing. The
    /// auth frame hands it to the TLS-policy guard together with the resolved
    /// identity.
    pub(super) transport: crate::control::security::tls_policy::TransportSecurity,
    pub(super) peer_addr: SocketAddr,
    pub(super) state: Arc<SharedState>,
    pub(super) auth_mode: AuthMode,
    pub(super) identity: Option<AuthenticatedIdentity>,
    pub(super) auth_context: Option<crate::control::security::auth_context::AuthContext>,
    /// Opaque proof of claim verification from an OIDC bearer auth frame.
    /// `None` for every other auth method. Retained for the connection's
    /// lifetime so each request can rebuild its `RequestAuthScope` with the
    /// same claim-derived `$auth.*` enrichment the auth frame established.
    pub(super) verified_jwt: Option<crate::control::security::jwks::registry::VerifiedJwtClaims>,
    pub(super) format: Option<FrameFormat>,
    pub(super) query_ctx: QueryContext,
    /// Connection-private mutable state retained by exact cleanup ownership.
    pub(super) sessions: Arc<SessionStore>,
    /// Detached rollback ownership for an authenticated transaction.
    pub(super) cleanup: Arc<NativeTxnCleanup>,
    pub(super) connected_at: Instant,
    /// Protocol version negotiated during the handshake.
    pub proto_ver: u16,
    pub(super) admission_registry: Arc<AdmissionRegistry>,
    pub(super) global_permit: Option<OwnedSemaphorePermit>,
    pub(super) connection_permit: Option<ConnectionPermit>,
}

impl NativeSession {
    fn with_stream(
        stream: ConnStream,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        admission_registry: Arc<AdmissionRegistry>,
        global_permit: OwnedSemaphorePermit,
        resources: NativeConnectionResources,
    ) -> Self {
        // A top-level connection plans with descriptor leases, the array
        // catalog and the WAL handle, same as a pgwire connection.
        let query_ctx = QueryContext::for_state_with_lease(&state);
        let NativeConnectionResources { sessions, cleanup } = resources;
        let transport = stream.transport_security();
        Self {
            stream,
            transport,
            peer_addr,
            state,
            auth_mode,
            identity: None,
            auth_context: None,
            verified_jwt: None,
            format: None,
            query_ctx,
            sessions,
            cleanup,
            connected_at: Instant::now(),
            proto_ver: 0,
            admission_registry,
            global_permit: Some(global_permit),
            connection_permit: None,
        }
    }

    /// Create a session from a plain TCP stream.
    pub(crate) fn new(
        stream: TcpStream,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        admission_registry: Arc<AdmissionRegistry>,
        global_permit: OwnedSemaphorePermit,
        resources: NativeConnectionResources,
    ) -> Self {
        Self::with_stream(
            ConnStream::plain(stream),
            peer_addr,
            state,
            auth_mode,
            admission_registry,
            global_permit,
            resources,
        )
    }

    /// Create a session from a TLS-wrapped stream.
    pub(crate) fn new_tls(
        stream: tokio_rustls::server::TlsStream<TcpStream>,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        admission_registry: Arc<AdmissionRegistry>,
        global_permit: OwnedSemaphorePermit,
        resources: NativeConnectionResources,
    ) -> Self {
        Self::with_stream(
            ConnStream::tls(stream),
            peer_addr,
            state,
            auth_mode,
            admission_registry,
            global_permit,
            resources,
        )
    }
}
