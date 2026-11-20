// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE USER` DDL handler.
//!
//! The tenant-admin gate, IF NOT EXISTS short-circuit, tenant-selector
//! resolution, `prepare_user`, catalog propose, the post-apply role check,
//! and `audit_record` run here. The result is [`DdlResult`] / [`DdlError`].

use nodedb_sql::ddl_ast::TenantSelector;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{
    parse_role, require_superuser_for_role, require_tenant_admin, status,
};

/// Resolve a [`TenantSelector`] to a numeric [`TenantId`]. Numeric ids pass
/// through; names are resolved against the redb catalog.
fn resolve_tenant_selector(
    state: &SharedState,
    selector: &TenantSelector,
) -> Result<TenantId, DdlError> {
    match selector {
        TenantSelector::Id(id) => Ok(TenantId::new(*id)),
        TenantSelector::Name(name) => {
            let catalog = state.credentials.catalog();
            let stored = catalog
                .find_tenant_by_name(name)
                .map_err(|e| DdlError::from_error_in_context("catalog read", &e))?
                .ok_or_else(|| DdlError::new("42704", format!("tenant '{name}' not found")))?;
            Ok(TenantId::new(stored.tenant_id))
        }
    }
}

/// CREATE USER [IF NOT EXISTS] <name> WITH PASSWORD '<password>' [ROLE <role>]
/// [TENANT <id> | TENANT '<name>']
pub async fn create_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    username: &str,
    password: &str,
    role_name: Option<&str>,
    tenant: Option<&TenantSelector>,
    if_not_exists: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create users")?;

    if username.is_empty() {
        return Err(DdlError::new(
            "42601",
            "syntax: CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]",
        ));
    }

    // The name is taken when the statement sees the user: created earlier
    // in its transaction counts, dropped earlier in it frees the name.
    // `IF NOT EXISTS`: re-creating an existing user is a no-op success.
    if super::super::role_checks::visible_user(state, username).is_some() {
        if if_not_exists {
            return Ok(status("CREATE USER"));
        }
        return Err(DdlError::new(
            "42710",
            format!("user '{username}' already exists"),
        ));
    }

    if password.is_empty() {
        return Err(DdlError::new(
            "42601",
            "password must be a single-quoted string",
        ));
    }

    let role = role_name.map(parse_role).unwrap_or(Role::ReadWrite);
    require_superuser_for_role(identity, &role)?;
    let tenant_id = if let Some(selector) = tenant {
        if !identity.is_superuser {
            return Err(DdlError::new("42501", "only superuser can assign tenants"));
        }
        resolve_tenant_selector(state, selector)?
    } else {
        identity.tenant_id
    };

    // A role that is neither built in nor defined in the tenant will leave
    // the user with no permissions: refuse it by name.
    super::super::role_checks::check_user_roles(state, std::slice::from_ref(&role), tenant_id)?;

    // Build the full `StoredUser` locally (hash + salt + user_id).
    // Followers cannot reproduce the random salt, so this step
    // MUST happen on the proposer node. The computed record is
    // then replicated verbatim.
    let stored = state
        .credentials
        .prepare_new_user(username, password, tenant_id, vec![role.clone()])
        .map_err(|e| DdlError::new("42710", e.to_string()))?;

    let entry = crate::control::catalog_entry::CatalogEntry::PutUser(Box::new(stored));
    let outcome = crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    if outcome.is_durable() {
        // `propose_catalog_entry_async` returns once the entry applied on THIS
        // node, and the synchronous post_apply
        // (`install_replicated_user`) runs before the applied-index watermark
        // bumps. So a committed entry is visible now. A missing user means
        // the applier skipped the entry because its role was dropped first,
        // or the entry was truncated by a leader change (`propose` returns
        // the assigned index before the quorum ack). The first is refused by
        // name; the second is retryable, and `exec_ddl_on_any_leader`
        // re-proposes against whoever leads now.
        super::super::role_checks::confirm_user_roles(
            state,
            username,
            std::slice::from_ref(&role),
            tenant_id,
        )?;
    }
    // A `Buffered` outcome falls through: the open transaction owns the entry
    // until COMMIT, so neither the cache install nor the truncation check
    // applies — the user does not exist yet and must not.

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(tenant_id),
        &identity.username,
        &format!("created user '{username}' in tenant {tenant_id}"),
    );

    Ok(status("CREATE USER"))
}
