// SPDX-License-Identifier: BUSL-1.1

//! Trust-mode username resolution for the pgwire startup path.
//!
//! Split from the handler core so the connection struct + trait impls stay
//! within the file-size budget. The logic runs on the trust startup path
//! (see the pgwire factory) before AuthenticationOk is announced.

use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use crate::control::server::session_auth::identity::{stored_user_identity, trust_identity};

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Trust-mode username resolution. A known username receives its stored
    /// identity with the Trust auth method. With an empty credential store,
    /// the startup username instead receives an ephemeral tenant-1 superuser
    /// identity that exists only in this connection's session entry.
    ///
    /// Runs after startup parameters are saved to client metadata and before
    /// AuthenticationOk is announced, so an unknown user never reaches
    /// ReadyForQuery. Only reads `client.metadata()` / `client.socket_addr()`,
    /// so `C: ClientInfo` is sufficient.
    pub(crate) fn resolve_trust_user<C>(&self, client: &C) -> PgWireResult<AuthenticatedIdentity>
    where
        C: ClientInfo,
    {
        let username = client
            .metadata()
            .get("user")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        if let Some(identity) = stored_user_identity(&self.state, &username, AuthMethod::Trust) {
            return Ok(identity);
        }

        if self.state.credentials.is_empty() {
            return Ok(trust_identity(&self.state, &username));
        }

        let source = client.socket_addr().to_string();
        self.state.audit_record(
            AuditEvent::AuthFailure,
            None,
            &source,
            &format!("trust auth: user '{username}' does not exist"),
        );
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "FATAL".to_owned(),
            "28000".to_owned(),
            format!("trust auth: user '{username}' does not exist"),
        ))))
    }
}
