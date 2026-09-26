// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `GRANT/REVOKE ROLE x TO/FROM user` handlers.
//!
//! Ported from the pgwire `ddl::grant::role` handlers. All non-return logic
//! (tenant-admin gate, superuser-grant guard, self-superuser-revoke guard,
//! grantee resolution, role-list mutation, `prepare_user_update`, catalog
//! propose + single-node fallback, `install_replicated_user`, the role-to-role
//! `set_role_parent` delegation, and `audit_record`) is preserved verbatim;
//! only the result construction changed from pgwire `Response` / `PgWireError`
//! to the protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Reuses the existing `CatalogEntry::PutUser` variant. The mutated role list
//! is built locally from the user's current record, then
//! `CredentialStore::prepare_user_update` clones the `StoredUser` with the new
//! roles and the proposer ships the whole record through raft. Followers'
//! appliers reinstall the updated user via `install_replicated_user` — no
//! separate `Add/RemoveRole` variant needed.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{parse_role, require_tenant_admin, status};

/// The roles and tenant of user `username` as this statement sees it: a user
/// created earlier in the transaction is visible, one dropped in it is not.
fn current_roles(
    state: &SharedState,
    username: &str,
) -> Result<(Vec<Role>, crate::types::TenantId), DdlError> {
    let user = super::super::role_checks::visible_user_or_missing(state, username)?;
    let roles = user.roles.iter().map(|name| parse_role(name)).collect();
    Ok((roles, crate::types::TenantId::new(user.tenant_id)))
}

fn propose_user_with_roles(
    state: &SharedState,
    username: &str,
    tenant_id: crate::types::TenantId,
    new_roles: Vec<Role>,
    invalidation: crate::control::security::buses::SessionInvalidationReason,
) -> Result<(), DdlError> {
    let base = super::super::role_checks::visible_user_or_missing(state, username)?;
    let stored = state
        .credentials
        .prepare_user_update_from(base, None, Some(new_roles.clone()))
        .map_err(|e| DdlError::new("42704", e.to_string()))?;
    let entry = CatalogEntry::PutUser(Box::new(stored.clone()));
    let outcome = propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError::new("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        {
            let catalog = state.credentials.catalog();
            catalog
                .put_user(&stored)
                .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        }
        state
            .credentials
            .install_replicated_user(&stored, Some(invalidation));
    } else if outcome.is_replicated() {
        super::super::role_checks::confirm_user_roles(state, username, &new_roles, tenant_id)?;
    }
    Ok(())
}

/// `GRANT <role>[, ...] TO <grantee>`.
///
/// The grantee is resolved to a user or a custom role. For a user, every
/// listed role is added to the user's role set. For a role, the single
/// listed role becomes the grantee's inheritance parent (role-to-role
/// membership) — the role hierarchy permits one parent, so granting more
/// than one role to a role is rejected.
pub fn grant_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    roles: &[String],
    grantee: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "grant roles")?;
    if roles.is_empty() {
        return Err(DdlError::new("42601", "GRANT: missing role name"));
    }

    if super::super::role_checks::visible_user(state, grantee).is_some() {
        grant_roles_to_user(state, identity, roles, grantee)
    } else if super::super::role_checks::visible_roles(state).contains_key(grantee) {
        grant_role_to_role(state, identity, roles, grantee)
    } else {
        Err(DdlError::new(
            "42704",
            format!("grantee '{grantee}' is not a known user or role"),
        ))
    }
}

fn grant_roles_to_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    role_names: &[String],
    username: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (mut roles, tenant_id) = current_roles(state, username)?;
    let granted: Vec<Role> = role_names.iter().map(|name| parse_role(name)).collect();
    // A role that is neither built in nor defined in the user's tenant would
    // grant nothing: refuse it by name.
    super::super::role_checks::check_user_roles(state, &granted, tenant_id)?;
    for role in granted {
        if matches!(role, Role::Superuser) && !identity.is_superuser {
            return Err(DdlError::new(
                "42501",
                "only superuser can grant superuser role",
            ));
        }
        if !roles.contains(&role) {
            roles.push(role);
        }
    }
    propose_user_with_roles(
        state,
        username,
        tenant_id,
        roles,
        crate::control::security::buses::SessionInvalidationReason::RoleGranted,
    )?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "granted role(s) {} to user '{username}'",
            role_names.join(", ")
        ),
    );

    Ok(status("GRANT"))
}

fn grant_role_to_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    role_names: &[String],
    child: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    if role_names.len() != 1 {
        return Err(DdlError::new(
            "0A000",
            "a role can inherit from only one parent role; grant one role at a time",
        ));
    }
    let parent = &role_names[0];
    super::super::role::set_role_parent(state, child, Some(parent))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("granted role '{parent}' to role '{child}'"),
    );

    Ok(status("GRANT"))
}

/// `REVOKE <role>[, ...] FROM <grantee>`.
pub fn revoke_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    roles: &[String],
    grantee: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "revoke roles")?;
    if roles.is_empty() {
        return Err(DdlError::new("42601", "REVOKE: missing role name"));
    }

    // Reject self-superuser revocation before resolving the grantee — an
    // admin must not be able to strip their own superuser role even if the
    // grantee name does not resolve to a stored user record.
    if grantee == identity.username
        && roles
            .iter()
            .any(|r| matches!(parse_role(r), Role::Superuser))
    {
        return Err(DdlError::new(
            "42501",
            "cannot revoke your own superuser role",
        ));
    }

    if super::super::role_checks::visible_user(state, grantee).is_some() {
        revoke_roles_from_user(state, identity, roles, grantee)
    } else if super::super::role_checks::visible_roles(state).contains_key(grantee) {
        revoke_role_from_role(state, identity, roles, grantee)
    } else {
        Err(DdlError::new(
            "42704",
            format!("grantee '{grantee}' is not a known user or role"),
        ))
    }
}

fn revoke_roles_from_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    role_names: &[String],
    username: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (mut roles, tenant_id) = current_roles(state, username)?;
    let revoked: Vec<Role> = role_names.iter().map(|n| parse_role(n)).collect();
    for role in &revoked {
        if !roles.contains(role) {
            return Err(DdlError::new(
                "42704",
                format!("user '{username}' does not have role '{role}'"),
            ));
        }
    }
    roles.retain(|r| !revoked.contains(r));
    propose_user_with_roles(
        state,
        username,
        tenant_id,
        roles,
        crate::control::security::buses::SessionInvalidationReason::RoleRevoked,
    )?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "revoked role(s) {} from user '{username}'",
            role_names.join(", ")
        ),
    );

    Ok(status("REVOKE"))
}

fn revoke_role_from_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    role_names: &[String],
    child: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    if role_names.len() != 1 {
        return Err(DdlError::new(
            "0A000",
            "a role inherits from at most one parent role; revoke one role at a time",
        ));
    }
    let parent = &role_names[0];
    let current_parent = state.roles.get_role(child).and_then(|r| r.parent);
    if current_parent.as_deref() != Some(parent.as_str()) {
        return Err(DdlError::new(
            "42704",
            format!("role '{child}' does not inherit from '{parent}'"),
        ));
    }
    super::super::role::set_role_parent(state, child, None)?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked role '{parent}' from role '{child}'"),
    );

    Ok(status("REVOKE"))
}
