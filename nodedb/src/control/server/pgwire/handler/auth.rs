// SPDX-License-Identifier: BUSL-1.1

//! Pgwire adapters for transport-neutral SQL authorization.

use std::sync::Arc;

use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::config::auth::AuthMode;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use crate::control::server::session_auth::identity::stored_user_identity;
use crate::control::server::shared::authorization::{
    AuthorizationError, authorize_database, authorize_task_set,
};
use crate::control::server::shared::session::SessionStore;
use crate::control::state::SharedState;
use nodedb_physical::physical_task::PhysicalTask;

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
    addr: &std::net::SocketAddr,
) -> PgWireResult<AuthenticatedIdentity> {
    let username = client
        .metadata()
        .get("user")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());

    let authenticated_identity = match auth_mode {
        AuthMode::Trust => {
            let startup_identity = sessions.identity(addr).ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_owned(),
                    "28000".to_owned(),
                    "trust auth: connection identity is missing".to_owned(),
                )))
            })?;

            // Empty-store Trust identities have no persisted user record and
            // must stay bound to this connection. Persisted users, however,
            // retain the prior per-request lookup so role changes and grants
            // are applied before every simple, Parse, and Execute path. The
            // user ID must still match the identity bound at startup: a DROP
            // followed by same-name recreation must not inherit this socket.
            if startup_identity.user_id == 0 {
                startup_identity
            } else {
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
        }
        AuthMode::Password | AuthMode::Certificate => {
            stored_user_identity(state, &username, AuthMethod::ScramSha256).ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_owned(),
                    "28000".to_owned(),
                    format!("authenticated user '{username}' not found in credential store"),
                )))
            })?
        }
    };

    let mut identity = authenticated_identity.clone();
    if let Some(effective) = sessions.get_effective_tenant_id(addr) {
        if identity.is_superuser {
            identity.tenant_id = effective;
        } else {
            sessions.set_effective_tenant_id(addr, None);
        }
    }

    // Preserve the identity in force for connection teardown so an abandoned
    // transaction can reclaim its Data-Plane staging overlays. DISCARD ALL
    // reconstructs Trust's base authenticated identity after it has released
    // any overlays.
    sessions.set_identity(addr, identity.clone());

    Ok(identity)
}

impl NodeDbPgHandler {
    /// Authorize the pgwire session database immediately after identity resolution.
    pub(super) fn authorize_session_database(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<()> {
        let database_id = self
            .sessions
            .get_current_database(addr)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let emitter = ArcAuditEmitter(Arc::clone(&self.state.audit));
        authorize_database(identity, database_id, &emitter).map_err(pgwire_authorization_error)
    }

    /// Authorize the final task set before pgwire execution can take any route.
    pub(super) fn authorize_tasks(
        &self,
        identity: &AuthenticatedIdentity,
        tasks: &[PhysicalTask],
    ) -> PgWireResult<()> {
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
