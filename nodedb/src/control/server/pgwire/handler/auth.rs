// SPDX-License-Identifier: BUSL-1.1

//! Pgwire adapters for transport-neutral SQL authorization.

use std::sync::Arc;

use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::config::auth::AuthMode;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use crate::control::security::session_identity::stored_user_identity;
use crate::control::security::tls_policy::TransportSecurity;
use crate::control::server::shared::authorization::{
    AuthorizationError, authorize_database, authorize_task_set,
};
use crate::control::server::shared::session::{SessionId, SessionStore};
use crate::control::state::SharedState;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::types::error_to_sqlstate;
use super::core::NodeDbPgHandler;

/// Resolve the identity shared by pgwire Parse and Execute paths.
///
/// Applying a superuser's session tenant override here ensures catalog
/// resolution during Parse observes the same identity as later execution.
pub(super) fn resolve_session_identity<C: ClientInfo>(
    state: &SharedState,
    auth_mode: AuthMode,
    sessions: &SessionStore,
    client: &C,
    session_id: &SessionId,
) -> PgWireResult<AuthenticatedIdentity> {
    let username = client
        .metadata()
        .get("user")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());

    // A killed session runs no new statement. The `FATAL` closes the connection.
    let binding = sessions.binding(*session_id);
    if let Some(binding) = &binding {
        binding.check().map_err(|error| {
            let (_, code, message) = error_to_sqlstate(&error);
            fatal(code, message)
        })?;
        binding.touch(0, 0);
    }

    let authenticated_identity = match auth_mode {
        AuthMode::Trust => {
            let startup_identity = sessions.identity(*session_id).ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_owned(),
                    "28000".to_owned(),
                    "trust auth: connection identity is missing".to_owned(),
                )))
            })?;

            // Re-resolve the durable identity on every simple, Parse, and
            // Execute path so role/grant changes take effect immediately. The
            // user ID must still match the identity bound at startup: a DROP
            // followed by same-name recreation must not inherit this socket.
            stored_user_identity(state, &startup_identity.username, AuthMethod::Trust)
                .filter(|current_identity| current_identity.user_id == startup_identity.user_id)
                .ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28000".to_owned(),
                        format!(
                            "trust auth: user '{}' does not exist",
                            startup_identity.username
                        ),
                    )))
                })?
        }
        AuthMode::Password | AuthMode::Certificate => {
            // The user id must match the one bound at startup: a DROP followed
            // by same-name recreation must not inherit this socket.
            let bound_user_id = binding.as_ref().map(|binding| binding.user_id());
            bound_password_identity(state, &username, bound_user_id).ok_or_else(|| {
                fatal(
                    "28000",
                    format!("authenticated user '{username}' not found in credential store"),
                )
            })?
        }
    };

    // Transport enforcement runs on the identity as authenticated, before the
    // session tenant override is applied and before the identity is bound to
    // the session store: a connection the TLS policy refuses must not leave a
    // usable identity behind for teardown or DISCARD ALL to reconstruct.
    check_client_transport(state, client, &authenticated_identity)?;

    let mut identity = authenticated_identity.clone();
    if let Some(effective) = sessions.get_effective_tenant_id(*session_id) {
        if identity.is_superuser {
            identity.tenant_id = effective;
        } else {
            sessions.set_effective_tenant_id(*session_id, None);
        }
    }

    // Preserve the identity in force for connection teardown so an abandoned
    // transaction can reclaim its Data-Plane staging overlays. DISCARD ALL
    // reconstructs Trust's base authenticated identity after it has released
    // any overlays.
    sessions.set_identity(*session_id, identity.clone());

    Ok(identity)
}

/// The stored identity of `username`, only while it is the user this connection
/// bound at startup. `None` for no binding, a dropped user, or a recreated name.
fn bound_password_identity(
    state: &SharedState,
    username: &str,
    bound_user_id: Option<u64>,
) -> Option<AuthenticatedIdentity> {
    stored_user_identity(state, username, AuthMethod::ScramSha256)
        .filter(|current_identity| Some(current_identity.user_id) == bound_user_id)
}

fn fatal(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_owned(),
        code.to_owned(),
        message,
    )))
}

/// Enforce the TLS policy for a pgwire connection.
///
/// The negotiated transport was captured in
/// [`connection::run`](crate::control::server::pgwire::connection) right after
/// pgwire's own SSL negotiation and stashed in the connection's typed
/// session-extension store, which is the only per-connection carrier pgwire
/// exposes that a client cannot write to. A connection with no stashed value
/// never went through that path, so it fails closed rather than being treated
/// as cleartext.
fn check_client_transport<C: ClientInfo>(
    state: &SharedState,
    client: &C,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<()> {
    if !state.tls_policy.is_enforcing() {
        return Ok(());
    }

    let transport = client
        .session_extensions()
        .get::<TransportSecurity>()
        .map(|stashed| *stashed);
    let source = client.socket_addr().to_string();

    let refusal = match transport {
        Some(transport) => crate::control::server::session_auth::check_transport_security(
            state, identity, transport, &source,
        )
        .err(),
        None => Some(crate::Error::RejectedAuthz {
            tenant_id: identity.tenant_id,
            resource: "connection transport could not be identified".into(),
        }),
    };

    match refusal {
        None => Ok(()),
        Some(error) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "FATAL".to_owned(),
            "28000".to_owned(),
            error.to_string(),
        )))),
    }
}

impl NodeDbPgHandler {
    /// Authorize the pgwire session database immediately after identity resolution.
    pub(super) fn authorize_session_database(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
    ) -> PgWireResult<()> {
        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let emitter = ArcAuditEmitter(Arc::clone(&self.state.audit));
        authorize_database(identity, database_id, &emitter).map_err(pgwire_authorization_error)
    }

    /// Authorize the final task set before pgwire execution can take any route.
    pub(super) fn authorize_tasks(
        &self,
        identity: &AuthenticatedIdentity,
        tasks: &[PhysicalTask],
    ) -> PgWireResult<crate::control::server::shared::authorization::AuthorizedTaskSet> {
        let emitter = ArcAuditEmitter(Arc::clone(&self.state.audit));
        authorize_task_set(
            identity,
            tasks,
            &self.state.permissions,
            &self.state.roles,
            &emitter,
        )
        .map_err(pgwire_authorization_error)
    }
}

pub(super) fn pgwire_authorization_error(error: AuthorizationError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "42501".to_owned(),
        crate::Error::from(error).to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::Role;
    use crate::types::TenantId;
    use crate::wal::WalManager;

    /// The kill normally closes such a socket first. This holds with no kill,
    /// so a statement that passes the kill check still cannot act as the new user.
    #[test]
    fn a_recreated_name_does_not_match_the_bound_user_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("t.wal")).expect("wal"));
        let (dispatcher, _) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        let create = || {
            state
                .credentials
                .create_user("reborn", "pw", TenantId::new(1), vec![Role::ReadOnly])
                .expect("create user")
        };
        let first_id = create();
        assert!(bound_password_identity(&state, "reborn", Some(first_id)).is_some());

        state.credentials.drop_user("reborn").expect("drop user");
        assert!(bound_password_identity(&state, "reborn", Some(first_id)).is_none());

        let second_id = create();
        assert_ne!(first_id, second_id);
        assert!(bound_password_identity(&state, "reborn", Some(first_id)).is_none());
        assert!(bound_password_identity(&state, "reborn", Some(second_id)).is_some());
        assert!(bound_password_identity(&state, "reborn", None).is_none());
    }
}
