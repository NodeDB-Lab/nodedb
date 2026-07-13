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

    let mut identity = match auth_mode {
        AuthMode::Trust => {
            stored_user_identity(state, &username, AuthMethod::Trust).ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_owned(),
                    "28000".to_owned(),
                    format!("trust auth: user '{username}' does not exist"),
                )))
            })?
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

    if let Some(effective) = sessions.get_effective_tenant_id(addr) {
        if identity.is_superuser {
            identity.tenant_id = effective;
        } else {
            sessions.set_effective_tenant_id(addr, None);
        }
    }

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
