// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `ALTER USER` DDL handler — typed dispatch for every
//! `AlterUserOp`.
//!
//! The self-vs-admin permission gates, per-op `prepare_*` credential
//! mutations, ISO-8601 expiry parsing, session-invalidation reasons,
//! default-database resolution, catalog propose, and `audit_record` run here.
//! The result is [`DdlResult`] / [`DdlError`].

use nodedb_sql::ddl_ast::AlterUserOp;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{
    parse_role, require_superuser_for_role, require_tenant_admin, status,
};
use super::super::role_checks::visible_user_in_or_missing;
use super::iso8601::parse_iso8601_to_unix;

/// ALTER USER <name> ... — typed dispatch for all AlterUserOp forms.
pub async fn alter_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    username: &str,
    op: &AlterUserOp,
) -> Result<Vec<DdlResult>, DdlError> {
    if username.is_empty() {
        return Err(DdlError::new(
            "42601",
            "syntax: ALTER USER <name> SET PASSWORD '<password>' | SET ROLE <role> | MUST CHANGE PASSWORD | PASSWORD NEVER EXPIRES | PASSWORD EXPIRES ...",
        ));
    }

    // Users can change their own password; admin required for anything else.
    let is_self = username == identity.username;
    let can_alter = is_self || identity.is_superuser || identity.has_role(&Role::TenantAdmin);

    match op {
        AlterUserOp::SetPassword { password } => {
            if !can_alter {
                return Err(DdlError::new(
                    "42501",
                    "permission denied: can only alter your own user, or be superuser/tenant_admin",
                ));
            }
            if password.is_empty() {
                return Err(DdlError::new(
                    "42601",
                    "password must be a non-empty single-quoted string",
                ));
            }
            let base = visible_user_in_or_missing(state, identity, username)?;
            let stored = state
                .credentials
                .prepare_user_update_from(base, Some(password.as_str()), None)
                .map_err(|e| DdlError::from_error(&e))?;
            // Password change — no role/access change; no invalidation.
            propose_and_install(state, stored).await?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("changed password for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::SetRole { role } => {
            if is_self && !identity.is_superuser {
                return Err(DdlError::new("42501", "cannot change your own role"));
            }
            require_tenant_admin(identity, "change roles")?;
            if role.is_empty() {
                return Err(DdlError::new("42601", "expected role name after SET ROLE"));
            }
            let parsed_role: Role = parse_role(role);
            require_superuser_for_role(identity, &parsed_role)?;
            let base = visible_user_in_or_missing(state, identity, username)?;
            let tenant_id = crate::types::TenantId::new(base.tenant_id);
            let new_roles = vec![parsed_role.clone()];
            super::super::role_checks::check_user_roles(state, &new_roles, tenant_id)?;
            let stored = state
                .credentials
                .prepare_user_update_from(base, None, Some(new_roles.clone()))
                .map_err(|e| DdlError::from_error(&e))?;
            if propose_and_install(state, stored).await? {
                super::super::role_checks::confirm_user_roles(
                    state, username, &new_roles, tenant_id,
                )?;
            }

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set role '{parsed_role}' for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::MustChangePassword => {
            require_tenant_admin(identity, "set must_change_password")?;
            let base = visible_user_in_or_missing(state, identity, username)?;
            let stored = state
                .credentials
                .prepare_set_must_change_password_from(base, true);
            propose_and_install(state, stored).await?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set must_change_password for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::PasswordNeverExpires => {
            require_tenant_admin(identity, "set password expiry")?;
            let base = visible_user_in_or_missing(state, identity, username)?;
            let stored = state
                .credentials
                .prepare_set_password_expires_at_from(base, 0);
            propose_and_install(state, stored).await?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set PASSWORD NEVER EXPIRES for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::PasswordExpiresAt { iso8601 } => {
            require_tenant_admin(identity, "set password expiry")?;
            let expires_at = parse_iso8601_to_unix(iso8601).map_err(|e| {
                DdlError::new(
                    "22007",
                    format!("invalid ISO-8601 datetime '{iso8601}': {e}"),
                )
            })?;
            let base = visible_user_in_or_missing(state, identity, username)?;
            let stored = state
                .credentials
                .prepare_set_password_expires_at_from(base, expires_at);
            propose_and_install(state, stored).await?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set PASSWORD EXPIRES '{iso8601}' for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::PasswordExpiresInDays { days } => {
            require_tenant_admin(identity, "set password expiry")?;
            if *days == 0 {
                return Err(DdlError::new(
                    "22003",
                    "PASSWORD EXPIRES IN requires a positive day count",
                ));
            }
            let expires_at = crate::control::security::time::now_secs() + (*days as u64) * 86400;
            let base = visible_user_in_or_missing(state, identity, username)?;
            let stored = state
                .credentials
                .prepare_set_password_expires_at_from(base, expires_at);
            propose_and_install(state, stored).await?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set PASSWORD EXPIRES IN {days} DAYS for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }

        AlterUserOp::SetDefaultDatabase { db_name } => {
            // Users can set their own default database; admin can set for others.
            if !can_alter {
                return Err(DdlError::new(
                    "42501",
                    "permission denied: can only alter your own user, or be superuser/tenant_admin",
                ));
            }
            if db_name.is_empty() {
                return Err(DdlError::new(
                    "42601",
                    "syntax: ALTER USER <name> SET DEFAULT DATABASE <db_name>",
                ));
            }
            // Resolve the database name to an ID via the system catalog.
            let catalog = state.credentials.catalog();
            let db_id = catalog
                .get_database_id_by_name(db_name)
                .map_err(|e| DdlError::from_error_in_context("catalog lookup", &e))?
                .ok_or_else(|| {
                    DdlError::new("42704", format!("database '{db_name}' does not exist"))
                })?;
            let base = visible_user_in_or_missing(state, identity, username)?;
            let stored = state
                .credentials
                .prepare_set_default_database_from(base, db_id.as_u64());
            propose_and_install(state, stored).await?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set default database '{db_name}' for user '{username}'"),
            );
            Ok(status("ALTER USER"))
        }
    }
}

/// Propose a `StoredUser`. Returns whether the entry applied on this node,
/// through the metadata group or directly without one. The apply installs
/// the user and notifies its sessions through `post_apply::user::put`.
async fn propose_and_install(
    state: &SharedState,
    stored: crate::control::security::catalog::StoredUser,
) -> Result<bool, DdlError> {
    let entry = crate::control::catalog_entry::CatalogEntry::PutUser(Box::new(stored));
    let outcome = crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    Ok(outcome.is_durable())
}
