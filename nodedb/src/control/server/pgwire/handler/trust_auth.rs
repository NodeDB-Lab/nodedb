// SPDX-License-Identifier: BUSL-1.1

//! Trust-mode username resolution for the pgwire startup path.
//!
//! Split from the handler core so the connection struct + trait impls stay
//! within the file-size budget. The logic runs on the trust startup path
//! (see the pgwire factory) before AuthenticationOk is announced.

use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::escalation::{
    AuthViolation, ViolationSubject, record_auth_violation,
};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use crate::control::security::session_identity::stored_user_identity;

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Trust-mode username resolution. The username must resolve to a durable
    /// stored identity; trust mode skips credential verification, not identity
    /// materialization or authorization.
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

        let source = client.socket_addr().to_string();
        record_auth_violation(
            &self.state,
            AuthViolation {
                subject: ViolationSubject::Username(username.as_str()),
                tenant_id: None,
                source: &source,
                detail: &format!("trust auth: user '{username}' does not exist"),
            },
        );
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "FATAL".to_owned(),
            "28000".to_owned(),
            format!("trust auth: user '{username}' does not exist"),
        ))))
    }
}
