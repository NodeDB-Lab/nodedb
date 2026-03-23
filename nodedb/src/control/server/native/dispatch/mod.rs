//! Per-opcode dispatch handlers for the native protocol.
//!
//! Each handler builds a `PhysicalPlan` (for Data Plane ops) or calls
//! `SharedState` methods directly (for DDL/session ops), reusing the
//! same infrastructure as the pgwire and HTTP endpoints.

mod direct_ops;
mod pgwire_bridge;
mod session_ops;
mod sql;
mod transaction;

use crate::control::planner::context::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::session::SessionStore;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};

// Re-export public handler functions.
pub(crate) use direct_ops::handle_direct_op;
pub(crate) use session_ops::{handle_reset, handle_set, handle_show};
pub(crate) use sql::handle_sql;
pub(crate) use transaction::{handle_begin, handle_commit, handle_rollback};

/// Dispatch context: holds references needed by all handlers.
pub(crate) struct DispatchCtx<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub query_ctx: &'a QueryContext,
    pub sessions: &'a SessionStore,
    pub peer_addr: &'a std::net::SocketAddr,
}

impl DispatchCtx<'_> {
    pub(super) fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id
    }

    pub(super) fn vshard_for_key(&self, key: &str) -> VShardId {
        VShardId::from_key(key.as_bytes())
    }
}

// ─── Auth ──────────────────────────────────────────────────────────

pub(crate) fn handle_auth(
    state: &SharedState,
    auth_mode: &crate::config::auth::AuthMode,
    auth: &nodedb_types::protocol::AuthMethod,
    peer_addr: &str,
) -> crate::Result<AuthenticatedIdentity> {
    use nodedb_types::protocol::AuthMethod as ProtoAuth;

    let body = match auth {
        ProtoAuth::Trust { username } => {
            serde_json::json!({ "method": "trust", "username": username })
        }
        ProtoAuth::Password { username, password } => {
            serde_json::json!({ "method": "password", "username": username, "password": password })
        }
        ProtoAuth::ApiKey { token } => {
            serde_json::json!({ "method": "api_key", "token": token })
        }
    };

    super::super::session_auth::authenticate(state, auth_mode, &body, peer_addr)
}

// ─── Ping ──────────────────────────────────────────────────────────

pub(crate) fn handle_ping(seq: u64) -> nodedb_types::protocol::NativeResponse {
    nodedb_types::protocol::NativeResponse::status_row(seq, "PONG")
}

// ─── Conversion Helpers (shared across sub-modules) ────────────────

pub(super) fn error_to_native(
    seq: u64,
    e: &crate::Error,
) -> nodedb_types::protocol::NativeResponse {
    let (code, message) = match e {
        crate::Error::BadRequest { detail } => ("42601", detail.clone()),
        crate::Error::RejectedAuthz { resource, .. } => ("42501", resource.clone()),
        crate::Error::DeadlineExceeded { .. } => ("57014", "query cancelled due to timeout".into()),
        crate::Error::CollectionNotFound { collection, .. } => {
            ("42P01", format!("collection '{collection}' not found"))
        }
        other => ("XX000", format!("{other}")),
    };
    nodedb_types::protocol::NativeResponse::error(seq, code, message)
}
