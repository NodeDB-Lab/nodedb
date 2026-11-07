// SPDX-License-Identifier: BUSL-1.1

//! RESP per-connection session state.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::TenantId;

/// Per-connection state for a RESP session.
///
/// Tracks the selected KV collection and authenticated tenant.
/// Each TCP connection gets its own session.
pub struct RespSession {
    /// Currently selected KV collection (via SELECT command).
    /// Defaults to "default" — the implicit KV collection.
    pub collection: String,

    /// Tenant ID for this connection.
    /// Defaults to tenant 1 (single-tenant mode).
    /// In multi-tenant mode, set after AUTH.
    pub tenant_id: TenantId,

    /// Identity established by RESP AUTH. Data operations fail closed until set.
    pub identity: Option<AuthenticatedIdentity>,

    /// Remote peer address of the TCP connection this session was accepted
    /// on, formatted as `SocketAddr::to_string()` (e.g. `"127.0.0.1:54321"`)
    /// to match the shape native/pgwire/HTTP pass. Set once at connection
    /// accept in `listener::handle_connection`; used for IP-blacklist checks
    /// in `check_request_admission`.
    pub peer_addr: String,

    /// What the connection negotiated, captured once at accept in
    /// `listener::handle_connection` because the TLS session is unreachable
    /// once the stream is borrowed for reads. AUTH hands it to the TLS-policy
    /// guard together with the resolved identity.
    pub transport: crate::control::security::tls_policy::TransportSecurity,

    /// The registry row and kill signal of the identity AUTH established. A
    /// later AUTH replaces both together.
    pub binding: Option<crate::control::security::sessions::SessionBinding>,
}

impl RespSession {
    /// Check the kill and bring the identity up to date for the next command.
    pub(super) fn refresh(
        &mut self,
        state: &crate::control::state::SharedState,
    ) -> crate::Result<()> {
        if let (Some(binding), Some(identity)) = (self.binding.as_mut(), self.identity.as_mut()) {
            binding.refresh(state, identity)?;
            binding.touch(0, 0);
            self.tenant_id = identity.tenant_id;
        }
        Ok(())
    }
}

impl Default for RespSession {
    fn default() -> Self {
        Self {
            collection: "default".into(),
            tenant_id: TenantId::new(1),
            identity: None,
            peer_addr: String::new(),
            // Fail-safe default for a session that was never bound to a
            // socket: the listener overwrites it at accept.
            transport: crate::control::security::tls_policy::TransportSecurity::Cleartext,
            binding: None,
        }
    }
}
