// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP USER` DDL handler.
//!
//! Ownership of every object the user owns (all owner-bearing kinds, not
//! just collections) is reassigned to the tenant admin, and every grant
//! made to the user is revoked, BEFORE the user row is removed — so no
//! dangling `owner → user` or `permission.grantee → user` reference can
//! survive the drop and brick the next boot's catalog integrity check.
//! The reassignment is fail-closed: if it errors, the user is not
//! dropped. See [`super::reassign_owned`].

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status, strip_if_exists};
use super::reassign_owned::reassign_owned_and_sweep_grants;

/// DROP USER [IF EXISTS] <name>
pub fn drop_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    drop_user_inner(state, identity, parts, false)
}

/// Remove the lifecycle administrator while its tenant is being dropped.
pub(in crate::control::server::shared::ddl::neutral) fn drop_tenant_admin(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    drop_user_inner(state, identity, parts, true)
}

fn drop_user_inner(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    tenant_teardown: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop users")?;

    let (if_exists, parts) = strip_if_exists(parts, 2);

    if parts.len() < 3 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: DROP USER [IF EXISTS] <name>".to_string(),
        });
    }

    let username = parts[2];

    if username == identity.username {
        return Err(DdlError {
            sqlstate: "42501".to_string(),
            message: "cannot drop your own user".to_string(),
        });
    }

    // Look up user's tenant before dropping (for ownership reassignment).
    let user_tenant = state
        .credentials
        .get_user(username)
        .map(|u| u.tenant_id)
        .unwrap_or(identity.tenant_id);

    // Pre-check existence so a DROP USER on a missing user is a
    // clean error that doesn't touch raft.
    let exists_before = state.credentials.get_user(username).is_some();
    if !exists_before {
        // `IF EXISTS`: dropping a missing user is a no-op success.
        if if_exists {
            return Ok(status("DROP USER"));
        }
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("user '{username}' does not exist"),
        });
    }

    let authoritative_admin = state
        .credentials
        .catalog()
        .authoritative_tenant_admin(user_tenant.as_u64())
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("load tenant administrator: {e}"),
        })?;
    if !tenant_teardown && authoritative_admin.as_deref() == Some(username) {
        return Err(DdlError {
            sqlstate: "55006".to_string(),
            message: format!(
                "cannot drop user '{username}': it is the authoritative tenant administrator"
            ),
        });
    }

    // Reassign every object owned by the user (all owner-bearing
    // kinds) to the tenant admin, and revoke every grant made to the
    // user, BEFORE removing the user row. Fail-closed: any error here
    // aborts the drop, because a partially-reassigned-then-deleted user
    // is exactly the dangling-reference bug this guards against.
    let admin_name = reassign_owned_and_sweep_grants(state, username, user_tenant)?;

    // `DropUser` fully removes the identity record on every node —
    // in-memory cache and redb catalog — so the username is freed
    // for reuse. A soft-delete tombstone would block a later
    // `CREATE USER` of the same name.
    let entry = crate::control::catalog_entry::CatalogEntry::DropUser {
        username: username.to_string(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("metadata propose: {e}"),
        })?;
    let dropped = if log_index == 0 {
        // Single-node fallback.
        state
            .credentials
            .drop_user(username)
            .map_err(|e| DdlError {
                sqlstate: "XX000".to_string(),
                message: e.to_string(),
            })?
    } else {
        // Cluster mode: the raft entry committed, so the
        // drop WILL be applied on every node. The
        // `post_apply` hook that updates the local in-memory
        // cache runs in a spawned tokio task and may not be
        // visible by the time this function returns — trust the
        // log index rather than re-reading the cache.
        true
    };

    if dropped {
        let detail = match admin_name {
            Some(admin_name) => {
                format!("dropped user '{username}' (ownership reassigned to '{admin_name}')")
            }
            None => format!("dropped user '{username}' (no owned objects required reassignment)"),
        };
        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &detail,
        );
        Ok(status("DROP USER"))
    } else {
        Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("user '{username}' does not exist"),
        })
    }
}
