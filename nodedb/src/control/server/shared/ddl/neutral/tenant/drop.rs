// SPDX-License-Identifier: BUSL-1.1

//! `DROP TENANT [IF EXISTS] <id|name>` handler.
//!
//! Accepts either a numeric tenant id or a tenant name (single-quoted
//! optional), parallel to the `CREATE TENANT <name>` and
//! `SHOW TENANT <name|id>` paths.
//!
//! Ported verbatim from the pgwire `ddl::tenant::drop` handler. The
//! `reconcile_tenant_users` cleanup now calls `neutral::user::drop_user`
//! directly (it was already protocol-neutral) instead of round-tripping
//! through `ddl_encode::ddl_results_to_pgwire`.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::strip_if_exists;
use super::support::{ddl_err, resolve_tenant_ref, status, tenant_exists};

pub fn drop_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can drop tenants",
        ));
    }

    let (if_exists, parts) = strip_if_exists(parts, 2);

    if parts.len() < 3 {
        return Err(ddl_err(
            "42601",
            "syntax: DROP TENANT [IF EXISTS] <id|name>",
        ));
    }

    // Accept either a numeric id or a tenant name; mirror the existing
    // CREATE TENANT name-resolution path. A name that matches no tenant yields
    // `None` here; an unknown numeric id resolves to a candidate that the
    // existence gate below rejects, so both forms behave identically.
    let tenant_id = match resolve_tenant_ref(state, parts[2])? {
        Some(tid) => tid,
        None => {
            // Name token did not resolve to any tenant.
            if if_exists {
                return Ok(status("DROP TENANT"));
            }
            return Err(ddl_err(
                "42704",
                format!("tenant '{}' does not exist", parts[2]),
            ));
        }
    };
    let tid = tenant_id.as_u64();

    if tid == 0 {
        return Err(ddl_err("42501", "cannot drop system tenant (0)"));
    }

    // Existence gate, uniform across numeric ids and resolved names: an unknown
    // tenant is a no-op under `IF EXISTS`, otherwise `42704` — never a silent
    // delete proposal for a tenant that does not exist.
    if !tenant_exists(state, tenant_id)? {
        if if_exists {
            return Ok(status("DROP TENANT"));
        }
        return Err(ddl_err(
            "42704",
            format!("tenant '{}' does not exist", parts[2]),
        ));
    }

    // Reconcile the tenant's users before removing the tenant row.
    //
    // `SHOW TENANTS` derives its row set from the union of catalog
    // tenants and every user's `tenant_id`, so any user left pointing
    // at this tenant resurrects it as a ghost row (retained id, empty
    // name) after the catalog row is gone. To keep `DROP TENANT`
    // consistent with `DROP USER` (hard-delete, disappears from
    // `SHOW`), the tenant's users must be reconciled here:
    //
    //   * the lifecycle-owned `<name>_admin` auto-provisioned by
    //     `CREATE TENANT` is dropped as part of the tenant lifecycle;
    //   * any other user is real and operator-owned — refuse the drop
    //     (`42501`) and name them, so nobody is silently hard-deleted.
    reconcile_tenant_users(state, identity, tenant_id)?;

    let entry = CatalogEntry::DeleteTenant { tenant_id: tid };
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| ddl_err("XX000", format!("metadata propose: {e}")))?;
    if log_index == 0 {
        {
            let catalog = state.credentials.catalog();
            catalog
                .delete_tenant(tid)
                .map_err(|e| ddl_err("XX000", format!("catalog write: {e}")))?;
        }
        let mut tenants = match state.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        tenants.remove_quota(tenant_id);
    }

    state.audit_record(
        AuditEvent::TenantDeleted,
        Some(tenant_id),
        &identity.username,
        &format!("dropped tenant {tenant_id}"),
    );

    Ok(status("DROP TENANT"))
}

/// Reconcile the users that belong to `tenant_id` before its catalog
/// row is removed, so the tenant cannot survive as a ghost in
/// `SHOW TENANTS` (which unions catalog tenants with every user's
/// `tenant_id`).
///
/// The lifecycle-owned administrator persisted by `CREATE TENANT` is
/// hard-dropped through the canonical `DROP USER`
/// path. Any other user is operator-owned: the drop is refused with
/// `42501` and the remaining users are named, so no real account is
/// ever silently hard-deleted by a tenant drop.
fn reconcile_tenant_users(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
) -> Result<(), DdlError> {
    let Some(admin_username) = state
        .credentials
        .catalog()
        .authoritative_tenant_admin(tenant_id.as_u64())
        .map_err(|error| ddl_err("XX000", format!("load tenant administrator: {error}")))?
    else {
        return Ok(());
    };

    // The same active-user set `SHOW TENANTS` unions over — reconciling
    // exactly this set is what clears the ghost.
    let members: Vec<String> = state
        .credentials
        .list_user_details()
        .into_iter()
        .filter(|u| u.tenant_id == tenant_id)
        .map(|u| u.username)
        .collect();

    let mut lifecycle_admin = None;
    let mut others = Vec::new();
    for username in members {
        if username == admin_username {
            lifecycle_admin = Some(username);
        } else {
            others.push(username);
        }
    }

    if !others.is_empty() {
        others.sort();
        return Err(ddl_err(
            "42501",
            format!(
                "cannot drop tenant: {} user(s) still belong to it; drop or \
                 reassign them first: {}",
                others.len(),
                others.join(", ")
            ),
        ));
    }

    // Only the lifecycle-owned admin remains (if any): hard-delete it
    // through the canonical `DROP USER` handler so ownership
    // reassignment, session invalidation, and catalog + redb removal
    // all run — the same guarantees `DROP USER` gives directly.
    if let Some(admin) = lifecycle_admin {
        let parts = ["DROP", "USER", admin.as_str()];
        super::super::user::drop_tenant_admin(state, identity, &parts)?;
    }

    Ok(())
}
