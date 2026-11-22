// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `role` DDL — CREATE / DROP ROLE, ALTER ROLE (GRANT /
//! REVOKE / SET INHERIT), and the shared `set_role_parent` inheritance mutator.
//!
//! The tenant-admin gate, IF [NOT] EXISTS short-circuits, `prepare_role`,
//! parent-existence + inheritance-cycle validation, catalog propose, the
//! post-apply role check, and `audit_record` run here. The result is the
//! protocol-neutral [`DdlResult`] / [`DdlError`].

use nodedb_sql::ddl_ast::AlterRoleOp;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};
use super::auth_support::{require_tenant_admin, status, strip_if_exists, strip_if_not_exists};
use super::grant;

/// CREATE ROLE [IF NOT EXISTS] <name> [INHERIT <parent>]
pub async fn create_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create roles")?;

    let (if_not_exists, parts) = strip_if_not_exists(parts, 2);

    if parts.len() < 3 {
        return Err(DdlError::new(
            "42601",
            "syntax: CREATE ROLE [IF NOT EXISTS] <name> [INHERIT <parent>]",
        ));
    }

    let name = parts[2];

    // The roles this statement sees: committed ones, and inside a
    // transaction those it created earlier, so a parent created in the same
    // transaction resolves. COMMIT checks the whole batch again.
    let visible = super::role_checks::visible_roles(state);

    // `IF NOT EXISTS`: re-creating an existing role is a no-op success.
    if if_not_exists && visible.contains_key(name) {
        return Ok(status("CREATE ROLE"));
    }

    let parent = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("INHERIT") {
        Some(parts[4])
    } else {
        None
    };

    // Build the `StoredRole` on the proposer: the same validation as
    // `create_role`, against the visible roles, without touching state.
    let stored = crate::control::security::role::prepare_role_against(
        name,
        identity.tenant_id,
        parent,
        &visible,
    )
    .map_err(|e| DdlError::new("42710", e.to_string()))?;

    let entry = crate::control::catalog_entry::CatalogEntry::PutRole(Box::new(stored));
    let outcome = crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    if outcome.is_durable() {
        super::role_checks::confirm_role(state, name, parent)?;
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "created role '{name}'{}",
            parent.map_or(String::new(), |p| format!(" inheriting from '{p}'"))
        ),
    );

    Ok(status("CREATE ROLE"))
}

/// DROP ROLE [IF EXISTS] <name>
pub async fn drop_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop roles")?;

    let (if_exists, parts) = strip_if_exists(parts, 2);

    if parts.len() < 3 {
        return Err(DdlError::new(
            "42601",
            "syntax: DROP ROLE [IF EXISTS] <name>",
        ));
    }

    let name = parts[2];
    let exists_before = super::role_checks::role_in_caller_tenant(state, identity, name);
    if !exists_before {
        // `IF EXISTS`: dropping a missing role is a no-op success.
        if if_exists {
            return Ok(status("DROP ROLE"));
        }
        return Err(DdlError::new(
            "42704",
            format!("role '{name}' does not exist"),
        ));
    }

    // As PostgreSQL does, a role that users hold or roles inherit from is
    // not dropped: dropping it will leave them naming a role that grants
    // nothing.
    super::role_checks::check_role_droppable(state, name)?;

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteRole {
        name: name.to_string(),
    };
    let outcome = crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    // The synchronous post-apply removed the role from this node's cache
    // before the propose returned. A role still present means the apply
    // skipped the entry: a user or role came to depend on it before the drop
    // committed. A buffered drop applies at COMMIT.
    if outcome.is_durable() && state.roles.get_role(name).is_some() {
        super::role_checks::check_role_droppable(state, name)?;
        return Err(DdlError::new(
            "40001",
            format!("transient: the drop of role '{name}' was superseded, retry"),
        ));
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("dropped role '{name}'"),
    );
    Ok(status("DROP ROLE"))
}

/// Typed dispatch for `ALTER ROLE` — covers GRANT, REVOKE, and SET INHERIT forms.
///
/// Reuses the protocol-neutral `grant_permission` / `revoke_permission` for the
/// permission forms so all permission mutations go through the same
/// catalog-propose path and emit `AuditEvent::PrivilegeChange`.
pub async fn alter_role_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    role_name: &str,
    sub_op: &AlterRoleOp,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "alter roles")?;

    // The role must exist before we mutate it: committed, or created earlier
    // in this transaction.
    if !super::role_checks::role_in_caller_tenant(state, identity, role_name) {
        return Err(DdlError::new(
            "42704",
            format!("role '{role_name}' not found"),
        ));
    }

    match sub_op {
        AlterRoleOp::Grant {
            permission,
            target_type,
            target_name,
        } => {
            grant::permission::grant_permission(
                state,
                identity,
                database_id,
                std::slice::from_ref(permission),
                target_type,
                target_name,
                role_name,
            )
            .await
        }

        AlterRoleOp::Revoke {
            permission,
            target_type,
            target_name,
        } => {
            grant::permission::revoke_permission(
                state,
                identity,
                database_id,
                std::slice::from_ref(permission),
                target_type,
                target_name,
                role_name,
            )
            .await
        }

        AlterRoleOp::SetInherit { parent } => {
            set_role_parent(state, role_name, Some(parent)).await?;

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("altered role '{role_name}': set inherit '{parent}'"),
            );

            Ok(status("ALTER ROLE"))
        }
    }
}

/// Set (`parent = Some`) or clear (`parent = None`) a custom role's
/// inheritance parent.
///
/// Shared by `ALTER ROLE <name> SET INHERIT <parent>` and the role-to-role
/// form of `GRANT <role> TO <role>` / `REVOKE <role> FROM <role>` so every
/// inheritance mutation goes through one catalog-propose path. The caller
/// is responsible for the `require_tenant_admin` privilege check and for
/// emitting the audit record.
pub async fn set_role_parent(
    state: &SharedState,
    role_name: &str,
    parent: Option<&str>,
) -> Result<(), DdlError> {
    // Roles created earlier in this transaction count, as they do for
    // `CREATE ROLE`.
    let visible = super::role_checks::visible_roles(state);
    let old_role = visible
        .get(role_name)
        .cloned()
        .ok_or_else(|| DdlError::new("42704", format!("role '{role_name}' not found")))?;

    if let Some(parent) = parent {
        let parent_is_builtin =
            crate::control::security::role_assignment::is_builtin_role_name(parent);
        if !parent_is_builtin && !visible.contains_key(parent) {
            return Err(DdlError::new(
                "42704",
                format!("parent role '{parent}' does not exist"),
            ));
        }
        // Reject self-inheritance and multi-hop cycles, and enforce the
        // inheritance-depth cap — the same invariant `CREATE ROLE` checks.
        crate::control::security::role::check_inheritance_cycle_against(
            role_name, parent, &visible,
        )
        .map_err(|e| DdlError::new("42P16", e.to_string()))?;
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let stored = crate::control::security::catalog::StoredRole {
        name: role_name.to_string(),
        tenant_id: old_role.tenant_id.as_u64(),
        parent: parent.unwrap_or("").to_string(),
        created_at: now,
    };

    let entry = crate::control::catalog_entry::CatalogEntry::PutRole(Box::new(stored));
    let outcome = crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    if outcome.is_durable() {
        super::role_checks::confirm_role(state, role_name, parent)?;
    }
    Ok(())
}
