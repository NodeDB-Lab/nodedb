//! `GRANT/REVOKE ROLE x TO/FROM user` handlers.
//!
//! Reuses the existing `CatalogEntry::PutUser` variant. The
//! mutated role list is built locally from the user's current
//! record, then `CredentialStore::prepare_user_update` clones the
//! `StoredUser` with the new roles and the proposer ships the
//! whole record through raft. Followers' appliers reinstall the
//! updated user via `install_replicated_user` — no separate
//! `Add/RemoveRole` variant needed.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::super::types::{parse_role, require_admin, sqlstate_error};

fn current_roles(state: &SharedState, username: &str) -> PgWireResult<Vec<Role>> {
    state
        .credentials
        .get_user(username)
        .map(|r| r.roles)
        .ok_or_else(|| sqlstate_error("42704", &format!("user '{username}' not found")))
}

fn propose_user_with_roles(
    state: &SharedState,
    username: &str,
    new_roles: Vec<Role>,
) -> PgWireResult<()> {
    let stored = state
        .credentials
        .prepare_user_update(username, None, Some(new_roles))
        .map_err(|e| sqlstate_error("42704", &e.to_string()))?;
    let entry = CatalogEntry::PutUser(Box::new(stored.clone()));
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .put_user(&stored)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        state.credentials.install_replicated_user(&stored);
    }
    Ok(())
}

pub fn grant_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "grant roles")?;

    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: GRANT ROLE <role> TO <user>",
        ));
    }

    let role = parse_role(parts[2]);

    if matches!(role, Role::Superuser) && !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "only superuser can grant superuser role",
        ));
    }

    if !parts[3].eq_ignore_ascii_case("TO") {
        return Err(sqlstate_error("42601", "expected TO after role name"));
    }
    let username = parts[4];

    let mut roles = current_roles(state, username)?;
    if !roles.contains(&role) {
        roles.push(role.clone());
    }
    propose_user_with_roles(state, username, roles)?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("granted role '{role}' to user '{username}'"),
    );

    Ok(vec![Response::Execution(Tag::new("GRANT"))])
}

pub fn revoke_role(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "revoke roles")?;

    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REVOKE ROLE <role> FROM <user>",
        ));
    }

    let role = parse_role(parts[2]);

    if !parts[3].eq_ignore_ascii_case("FROM") {
        return Err(sqlstate_error("42601", "expected FROM after role name"));
    }
    let username = parts[4];

    if username == identity.username && matches!(role, Role::Superuser) {
        return Err(sqlstate_error(
            "42501",
            "cannot revoke your own superuser role",
        ));
    }

    let mut roles = current_roles(state, username)?;
    let before = roles.len();
    roles.retain(|r| r != &role);
    if roles.len() == before {
        return Err(sqlstate_error(
            "42704",
            &format!("user '{username}' does not have role '{role}'"),
        ));
    }
    propose_user_with_roles(state, username, roles)?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("revoked role '{role}' from user '{username}'"),
    );

    Ok(vec![Response::Execution(Tag::new("REVOKE"))])
}
