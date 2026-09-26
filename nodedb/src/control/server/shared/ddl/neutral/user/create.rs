// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE USER` DDL handler.
//!
//! Ported from the pgwire `ddl::user::create` handler. All non-return logic
//! (tenant-admin gate, IF NOT EXISTS short-circuit, tenant-selector
//! resolution, `prepare_user`, catalog propose + single-node `LocalOnly`
//! fallback, cluster-mode `get_user` truncation retry, `install_replicated_user`,
//! and `audit_record`) is preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to [`DdlResult`] / [`DdlError`].

use nodedb_sql::ddl_ast::TenantSelector;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{parse_role, require_tenant_admin, status};

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
                .map_err(|e| DdlError::new("XX000", format!("catalog read: {e}")))?
                .ok_or_else(|| DdlError::new("42704", format!("tenant '{name}' not found")))?;
            Ok(TenantId::new(stored.tenant_id))
        }
    }
}

/// CREATE USER [IF NOT EXISTS] <name> WITH PASSWORD '<password>' [ROLE <role>]
/// [TENANT <id> | TENANT '<name>']
pub fn create_user(
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
    let tenant_id = if let Some(selector) = tenant {
        if !identity.is_superuser {
            return Err(DdlError::new("42501", "only superuser can assign tenants"));
        }
        resolve_tenant_selector(state, selector)?
    } else {
        identity.tenant_id
    };

    // A role that is neither built in nor defined in the tenant would leave
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

    let entry = crate::control::catalog_entry::CatalogEntry::PutUser(Box::new(stored.clone()));
    let outcome = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError::new("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        // Single-node / no-cluster fallback: install into the
        // in-memory cache so subsequent reads see the user.
        // Persist to redb when a catalog is wired up — the
        // catalog write is best-effort durability, not a gate
        // on the cache update. Test fixtures (and any future
        // fully-in-memory deployment) can run without a redb
        // catalog and still get correct read-after-write.
        {
            let catalog = state.credentials.catalog();
            catalog
                .put_user(&stored)
                .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        }
        // CREATE USER: no open sessions exist for a brand-new user.
        state.credentials.install_replicated_user(&stored, None);
    } else if outcome.is_replicated() {
        // Cluster mode: `propose_catalog_entry` waits for the entry to apply
        // on THIS node, and the synchronous post_apply
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
