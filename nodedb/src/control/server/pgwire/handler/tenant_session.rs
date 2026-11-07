// SPDX-License-Identifier: BUSL-1.1

//! Tenant-context SET and RESET commands for pgwire sessions.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{SessionId, TransactionState};

use super::super::types::{error_to_pg_in_context, sqlstate_error};
use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Apply (or clear) a session-level tenant override after policy checks.
    fn apply_tenant_override(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        new_tenant: Option<crate::types::TenantId>,
        source: &str,
    ) -> PgWireResult<Vec<Response>> {
        use crate::control::security::audit::AuditEvent;

        if !identity.is_superuser {
            return Err(sqlstate_error(
                "42501",
                "only superuser may change session tenant; a regular user's \
                 tenant is identity-bound at CREATE USER time",
            ));
        }
        if self.sessions.transaction_state(session_id) != TransactionState::Idle {
            return Err(sqlstate_error(
                "25001",
                "cannot change session tenant inside an active transaction \
                 (COMMIT or ROLLBACK first)",
            ));
        }

        let prior = self.sessions.get_effective_tenant_id(session_id);
        let detail = match new_tenant {
            Some(tenant) => format!(
                "{source}: tenant switched from {} to {}",
                prior.unwrap_or(identity.tenant_id),
                tenant
            ),
            None => format!(
                "{source}: tenant reset to identity-bound {}",
                identity.tenant_id
            ),
        };

        // The override changes whose data the session reads and writes, so its record comes first.
        self.state
            .audit_record_strict(
                AuditEvent::ActionIntent,
                Some(identity.tenant_id),
                &identity.username,
                &detail,
            )
            .map_err(|e| error_to_pg_in_context("audit record", &e))?;
        self.sessions
            .set_effective_tenant_id(session_id, new_tenant);

        self.state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &detail,
        );

        Ok(vec![Response::Execution(Tag::new("SET"))])
    }

    /// Handle `SET TENANT = '<name>' | <id> | DEFAULT`.
    pub(super) fn handle_set_tenant_name_or_id(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        value: &str,
    ) -> PgWireResult<Vec<Response>> {
        if value.eq_ignore_ascii_case("default") {
            return self.apply_tenant_override(identity, session_id, None, "SET TENANT = DEFAULT");
        }
        let resolved = if let Ok(id) = value.parse::<u64>() {
            crate::types::TenantId::new(id)
        } else {
            let catalog = self.state.credentials.catalog();
            let stored = catalog
                .find_tenant_by_name(value)
                .map_err(|error| error_to_pg_in_context("catalog read", &error))?
                .ok_or_else(|| sqlstate_error("42704", &format!("tenant '{value}' not found")))?;
            crate::types::TenantId::new(stored.tenant_id)
        };
        self.apply_tenant_override(identity, session_id, Some(resolved), "SET TENANT")
    }

    /// Handle `SET nodedb.tenant_id = <id> | DEFAULT`.
    pub(super) fn handle_set_tenant_by_id(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        value: &str,
    ) -> PgWireResult<Vec<Response>> {
        if value.eq_ignore_ascii_case("default") {
            return self.apply_tenant_override(
                identity,
                session_id,
                None,
                "SET nodedb.tenant_id = DEFAULT",
            );
        }
        let id: u64 = value.parse().map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "22023".to_owned(),
                format!("invalid value for nodedb.tenant_id: '{value}'. Must be an integer."),
            )))
        })?;
        self.apply_tenant_override(
            identity,
            session_id,
            Some(crate::types::TenantId::new(id)),
            "SET nodedb.tenant_id",
        )
    }

    /// Reset the session's tenant override back to the identity-bound tenant.
    pub(crate) fn handle_reset_tenant(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
    ) -> PgWireResult<Vec<Response>> {
        if !identity.is_superuser {
            return Err(sqlstate_error("42501", "only superuser may RESET TENANT"));
        }
        if self.sessions.transaction_state(session_id) != TransactionState::Idle {
            return Err(sqlstate_error(
                "25001",
                "cannot RESET TENANT inside an active transaction",
            ));
        }
        self.sessions.set_effective_tenant_id(session_id, None);
        Ok(vec![Response::Execution(Tag::new("RESET"))])
    }
}
